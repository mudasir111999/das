# backend/agent_setup.py

from dotenv import load_dotenv
load_dotenv()

# Core
from typing import Optional
import os, json, glob, re
from datetime import datetime
# ⬇️ CHANGE: import the lazy getter instead of eager loader
from .m import get_mcp_client

# Agent & tools
from smolagents import CodeAgent, LiteLLMModel, Tool
# tools/motherduck_query_tool.py
import duckdb
import pandas as pd


# === File tools (writer/reader/list) ===
DEFAULT_POSTGRES_DSN = "postgresql://appuser:StrongPass123!@192.168.217.128:5432/postgres"
DEFAULT_MYSQL_DSN    = "mysql+pymysql://appuser:StrongPass123!@192.168.217.128:3306/appdb"
DEFAULT_MSSQL_DSN    = "mssql+pyodbc://sa:S3cur3!Pass@192.168.217.128:1433/appdb?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"

DEFAULT_CSV_BASE_DIR = "./data"

POSTGRES_DSN = os.getenv("POSTGRES_DSN", DEFAULT_POSTGRES_DSN)
MYSQL_DSN    = os.getenv("MYSQL_DSN", DEFAULT_MYSQL_DSN)
MSSQL_DSN    = os.getenv("MSSQL_DSN", DEFAULT_MSSQL_DSN)
CSV_BASE_DIR = os.getenv("CSV_BASE_DIR", DEFAULT_CSV_BASE_DIR)

# Disallow any write-like statements for safety
DISALLOWED_SQL = re.compile(
    r"\b(INSERT|UPDATE|DELETE|MERGE|CREATE|ALTER|DROP|TRUNCATE|REPLACE|GRANT|REVOKE)\b",
    re.IGNORECASE
)

def _assert_readonly(sql: str):
    if DISALLOWED_SQL.search(sql or ""):
        raise ValueError("Write-like statements are not allowed (read-only policy).")

# === Tools ===
class FileWriterTool(Tool):
    name = "file_writer"
    description = ("A tool that writes content to a file.")
    inputs = {
        "file_path": {"type": "string", "description": "Path to the file to be written."},
        "content": {"type": "string", "description": "The content to write into the file."},
        "append": {"type": "boolean", "description": "Append if True, overwrite if False."},
    }
    output_type = "string"

    def forward(self, file_path: str, content: str, append: bool):
        try:
            os.makedirs(os.path.dirname(file_path) or ".", exist_ok=True)
            mode = "a" if append else "w"
            with open(file_path, mode, encoding="utf-8") as f:
                if append:
                    f.write("\n" + content)
                else:
                    f.write(content)
            action = "Appended to" if append else "Overwritten in"
            return f"File successfully {action} {file_path}"
        except Exception as e:
            return f"Error writing to file: {str(e)}"

def _connect_motherduck():
    md_token = os.getenv("MOTHERDUCK_TOKEN")
    md_url = os.getenv("MOTHERDUCK_URL") or (f"md:?motherduck_token={md_token}" if md_token else None)
    if not md_url:
        raise RuntimeError("Missing connection URL/token for MotherDuck. Set MOTHERDUCK_TOKEN or MOTHERDUCK_URL.")
    return duckdb.connect(md_url)

class MotherDuckQueryTool(Tool):
    name = "motherduck_query"
    description = "Run read-only SQL against MotherDuck via DuckDB’s md: driver. Always pass sql as a keyword argument."
    output_type = "string"
    inputs = {
        "sql": {"type": "string", "description": "SQL to execute", "nullable": True},
        "max_rows": {"type": "integer", "description": "Optional row limit", "nullable": True},
    }

    def __init__(self, conn=None):
        super().__init__()
        self.conn = conn

    def _ensure_conn(self):
        if self.conn is None:
            self.conn = _connect_motherduck()

    def forward(self, sql: Optional[str] = None, max_rows: Optional[int] = None) -> str:
        if sql is None:
            raise ValueError("`sql` is required.")
        self._ensure_conn()
        df: pd.DataFrame = self.conn.execute(sql).fetch_df()
        if max_rows is not None:
            df = df.head(max_rows)
        records = df.where(pd.notnull(df), None).to_dict(orient="records")
        return json.dumps(records, default=str)


class FileReaderTool(Tool):
    name = "file_reader"
    description = "Reads content from a file."
    inputs = {"file_path": {"type": "string", "description": "Path to file"}}
    output_type = "string"

    def forward(self, file_path: str):
        try:
            with open(file_path, encoding="utf-8") as f:
                return f.read()
        except FileNotFoundError:
            return f"Error: File '{file_path}' not found."
        except Exception as e:
            return f"Error reading file: {str(e)}"

class ListFilesTool(Tool):
    name = "list_files"
    description = "Lists files in a directory; optionally recursive."
    inputs = {
        "directory_path": {"type": "string", "description": "Directory to list."},
        "recursive": {"type": "boolean", "description": "Recurse into subdirs.", "default": False, "nullable": True},
    }
    output_type = "string"

    def forward(self, directory_path: str, recursive: bool = False):
        try:
            results = []
            if recursive:
                for root, _, files in os.walk(directory_path):
                    for name in files:
                        results.append(os.path.join(root, name))
            else:
                if not os.path.isdir(directory_path):
                    return json.dumps([f"Error: '{directory_path}' is not a directory or does not exist."])
                for entry in os.listdir(directory_path):
                    full_path = os.path.join(directory_path, entry)
                    if os.path.isfile(full_path):
                        results.append(full_path)
            return json.dumps(results)
        except Exception as e:
            return json.dumps([f"Error listing files: {str(e)}"])


# =========================
# DB + CSV Query Utilities
# =========================
def _to_json_records(df):
    # Return a compact JSON array of objects
    try:
        import pandas as pd  # noqa
        return df.to_json(orient="records", date_format="iso")
    except Exception as e:
        return json.dumps({"error": f"Failed to serialize dataframe: {e}"})

def _limited_wrapped_query(sql: str, limit: int | None):
    sql = sql.strip().rstrip(";")
    if limit is not None and limit > 0:
        return f"SELECT * FROM ({sql}) AS _q LIMIT {int(limit)}"
    return sql

# ---- SQLAlchemy-based DB Tools (Postgres, MySQL, MSSQL) ----
class _SQLAlchemyQueryToolBase(Tool):
    # subclasses set: name, description, _dsn
    inputs = {
        "query": {"type": "string", "description": "Read-only SQL query (SELECT/CTE/EXPLAIN/etc)."},
        "limit": {"type": "integer", "description": "Optional extra LIMIT wrapper.", "nullable": True},
        "as_csv": {
            "type": "boolean",
            "description": "Return CSV instead of JSON records.",
            "default": False,
            "nullable": True,
        },
    }

    output_type = "string"
    _dsn: str = ""

    def _engine(self):
        try:
            import sqlalchemy  # noqa
            from sqlalchemy import create_engine
            return create_engine(self._dsn, future=True)
        except Exception as e:
            return f"Error: cannot create engine for DSN={self._dsn}. Details: {e}"

    def forward(self, query: str, limit: int = None, as_csv: bool = False):
        _assert_readonly(query)
        eng = self._engine()
        if isinstance(eng, str):  # error string
            return eng
        try:
            import pandas as pd
            from sqlalchemy import text
            wrapped = _limited_wrapped_query(query, limit)
            with eng.connect() as conn:
                # Best-effort read-only session hints; ignore if not supported
                try:
                    conn.execute(text("SET TRANSACTION READ ONLY"))
                except Exception:
                    pass
                df = pd.read_sql_query(text(wrapped), conn)
            if as_csv:
                return df.to_csv(index=False)
            return _to_json_records(df)
        except Exception as e:
            return f"DB query error: {e}"

class _SQLAlchemyListTablesBase(Tool):
    # subclasses set: name, description, _dsn
    inputs = {}
    output_type = "string"
    _dsn: str = ""

    def forward(self):
        try:
            import sqlalchemy
            from sqlalchemy import create_engine, inspect
            eng = create_engine(self._dsn, future=True)
            insp = inspect(eng)
            result = {}
            # Gather schemas + tables where possible
            try:
                schemas = insp.get_schema_names()
            except Exception:
                schemas = [None]
            for sch in schemas:
                try:
                    tbls = insp.get_table_names(schema=sch)
                    if tbls:
                        result[sch or "default"] = tbls
                except Exception:
                    continue
            return json.dumps(result)
        except Exception as e:
            return f"Error listing tables: {e}"

# ---- PostgreSQL
class PostgresQueryTool(_SQLAlchemyQueryToolBase):
    name = "postgres_query"
    description = "Run read-only SQL on PostgreSQL using POSTGRES_DSN."
    _dsn = POSTGRES_DSN

class PostgresListTablesTool(_SQLAlchemyListTablesBase):
    name = "postgres_list_tables"
    description = "List schemas and tables from PostgreSQL using POSTGRES_DSN."
    _dsn = POSTGRES_DSN

# ---- MySQL
class MySQLQueryTool(_SQLAlchemyQueryToolBase):
    name = "mysql_query"
    description = "Run read-only SQL on MySQL using MYSQL_DSN."
    _dsn = MYSQL_DSN

class MySQLListTablesTool(_SQLAlchemyListTablesBase):
    name = "mysql_list_tables"
    description = "List schemas and tables from MySQL using MYSQL_DSN."
    _dsn = MYSQL_DSN

# ---- MSSQL
class MSSQLQueryTool(_SQLAlchemyQueryToolBase):
    name = "mssql_query"
    description = "Run read-only SQL on Microsoft SQL Server using MSSQL_DSN."
    _dsn = MSSQL_DSN

class MSSQLListTablesTool(_SQLAlchemyListTablesBase):
    name = "mssql_list_tables"
    description = "List schemas and tables from Microsoft SQL Server using MSSQL_DSN."
    _dsn = MSSQL_DSN


# ---- CSV SQL (DuckDB in-process)
def _sanitize_table_name(stem: str) -> str:
    # Make a duckdb-friendly table name from filename stem
    name = re.sub(r"[^A-Za-z0-9_]", "_", stem)
    if re.match(r"^\d", name):
        name = "_" + name
    return name.lower()

class CSVListTablesTool(Tool):
    name = "csv_list_tables"
    description = (
        "List CSV files under CSV_BASE_DIR (or specified directory) and how they map to SQL table names."
    )
    inputs = {
        "base_dir": {"type": "string", "description": "Directory of CSVs (optional).", "nullable": True}
    }
    output_type = "string"

    def forward(self, base_dir: str = None):
        bdir = base_dir or CSV_BASE_DIR
        if not os.path.isdir(bdir):
            return json.dumps({"error": f"Directory not found: {bdir}"})
        mapping = {}
        for fn in os.listdir(bdir):
            if fn.lower().endswith(".csv"):
                stem = os.path.splitext(fn)[0]
                mapping[_sanitize_table_name(stem)] = os.path.join(bdir, fn)
        return json.dumps({"base_dir": bdir, "tables": mapping})

class CSVSQLTool(Tool):
    name = "csv_sql"
    description = (
        "Run SQL over local CSVs using DuckDB. Each CSV file in base_dir is exposed as a table named after its filename."
    )
    inputs = {
        "query": {"type": "string", "description": "Read-only SQL query to execute against registered CSV tables."},
        "base_dir": {"type": "string", "description": "Directory of CSVs to load/register.", "nullable": True},
        "as_csv": {
            "type": "boolean",
            "description": "Return CSV instead of JSON records.",
            "default": False,
            "nullable": True,
        },
    }

    output_type = "string"

    def forward(self, query: str, base_dir: str = None, as_csv: bool = False):
        _assert_readonly(query)
        bdir = base_dir or CSV_BASE_DIR
        if not os.path.isdir(bdir):
            return f"Error: CSV base_dir does not exist: {bdir}"
        try:
            import duckdb
            con = duckdb.connect()
            # Register each CSV as a view
            registered = 0
            for fn in os.listdir(bdir):
                if fn.lower().endswith(".csv"):
                    path = os.path.join(bdir, fn)
                    stem = os.path.splitext(fn)[0]
                    tname = _sanitize_table_name(stem)
                    # Use read_csv_auto for schema inference
                    con.execute(
                        f"CREATE OR REPLACE VIEW {tname} AS SELECT * FROM read_csv_auto(? , header=True)",
                        [path]
                    )
                    registered += 1
            if registered == 0:
                return f"No CSV files found under {bdir}"

            # Execute query
            res = con.execute(query)
            df = res.df()
            if as_csv:
                return df.to_csv(index=False)
            return _to_json_records(df)
        except Exception as e:
            return f"CSV SQL error: {e}"


def _redact_dsn(dsn: str) -> str:
    """
    Redacts passwords in DSN strings for safe inclusion in prompts.
    Examples:
      postgresql://user:pass@host:5432/db  -> postgresql://user:***@host:5432/db
      mssql+pyodbc://sa:Pass@host/db?...   -> mssql+pyodbc://sa:***@host/db?...
    """
    try:
        return re.sub(r'(://[^:/@]+:)([^@]+)(@)', r'\1***\3', dsn)
    except Exception:
        return dsn

def build_data_agent_prompt(task: str, work_dir: str) -> str:
    import re
    from datetime import datetime

    # --- NEW: derive a fresh schema name per build, appropriate to the task ---
    # slugify task -> snake_case; ensure starts with a letter; keep it short; add timestamp
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    base = re.sub(r"[^a-zA-Z0-9]+", "_", task).strip("_").lower() or "dataset"
    if not base[0].isalpha():
        base = f"s_{base}"
    base = base[:40].strip("_")  # keep headroom for suffix
    schema_name = f"{base}_{ts}"
    schema_name = schema_name[:63].rstrip("_")  # safety for identifier length

    # Build a human-readable datasources block using existing globals.
    # We keep the underlying tools/DSNs unchanged; this is purely instructional context for the agent.
    postgres_dsn = _redact_dsn(POSTGRES_DSN)
    mysql_dsn    = _redact_dsn(MYSQL_DSN)
    mssql_dsn    = _redact_dsn(MSSQL_DSN)
    csv_dir      = CSV_BASE_DIR

    # If MotherDuck MCP tools are loaded, list their names to make the agent aware.
    try:
        md_tools_list = [t.name for t in mcp_tools] if 'mcp_tools' in globals() and mcp_tools else []
    except Exception:
        md_tools_list = []

    md_tools_text = ""
    if md_tools_list:
        md_tools_text = (
            "\n- **MotherDuck MCP Tools**: Available tools → "
            + ", ".join(f"`{n}`" for n in md_tools_list)
        )

    datasources_block = f"""
# Data Sources Available (Read-Only)
Use these sources **via the provided tools** (no custom connections needed):

- **PostgreSQL** — DSN: `{postgres_dsn}` — use tool: `postgres_query` / `postgres_list_tables`
- **MySQL** — DSN: `{mysql_dsn}` — use tool: `mysql_query` / `mysql_list_tables`
- **Microsoft SQL Server** — DSN: `{mssql_dsn}` — use tool: `mssql_query` / `mssql_list_tables`
- **CSV directory** — `{csv_dir}` — use tools: `csv_list_tables`, `csv_sql`{md_tools_text}
"""
    prompt = f"""
# Role
You are a Senior Synthetic Data Engineer & Validator. You generate **realistic, big analysis-grade** datasets exactly to spec, **prove** they meet the requirements, and **persist them to MotherDuck** (via the available MCP tools). You work deterministically, efficiently, and defensively (great logs, graceful errors, no silent failures).
User can Also do questions from you about the data stored any where, you have to access that data and answer the user about that.


{datasources_block}

Ask user the task By saying this exact question "What is the schema and requirements or you want to validate the data?" then stop and show this in final answer then after when user gives the prompt then complete the whole given process.
If user ask anything else use proper listed tools and answer those appropriately.

# Primary Objective

Produce:
1) Clean, realistic datasets saved under a **new timestamped subfolder** in {work_dir}.
2) A **data dictionary** and **generation spec** describing columns, types, nullability, rules, distributions.
3) A **validation suite** (queries + results) that proves the data meets all requirements.
4) The same data **loaded into MotherDuck** using the MCP SQL tool(s).
5) A `validation.md` and `load_report.md` explaining exactly what you did and the outcomes.

# Non-Negotiable Principles
1) **Schema-First**: If schema(s) are provided, follow them **exactly** (names, types, constraints, relationships). **Never** invent columns or types. If schema is missing or ambiguous, **STOP** and write `ERROR_missing_schema.md` describing what’s missing; do not fabricate.
2) **Deterministic**: Set a **fixed random seed** (e.g. 42) for all stochastic generation so outputs are reproducible.
3) **Realism with Control**: Match specified distributions, correlations, seasonality, sparsity/null-rates, ranges, integrity rules, foreign-keys. If defaults are required, explicitly document them in `generation_spec.md` and justify them.
4) **Efficiency**: Prefer **vectorized** operations (pandas/NumPy), avoid Python loops for row-wise generation, and avoid row-by-row DB inserts. When writing to MotherDuck, use **`COPY FROM`** or **`CREATE TABLE AS SELECT read_csv_auto(...)`** style bulk ops.
5) **Tooling**: Use only the tools provided:
   - `list_files` to discover existing inputs/outputs.
   - `file_reader` to read input specs/schemas.
   - `file_writer` to write outputs, logs, dictionaries, SQL, and reports.
   - **MCP MotherDuck tools** (already available in this runtime) to load data. **Do not** open ad-hoc DB connections.
   - "motherduck_query" It is separately defined from mcp_tools to query in Motherduck 
6) **No Hidden Assumptions**: All assumptions must be written to `generation_spec.md`.
7) Always try to resolve this error "Error executing tool query" by using sql with motherduck_query defined separately from mcp_tools.
8) If final answers is in json then convert and return that answer in English.
9) Auto Handle any errors during process by taking correct steps and using better logic and ways.

# Directory Convention
- Create a unique run folder: `{work_dir}/run_YYYYMMDD_HHMMSS/`
- Inside it, write:
  - `generation_spec.md` (requirements, assumptions, seed, generation plan)
  - `csv/` (all datasets as CSV; filenames == table names)
  - `validation_plan.md` — requirement → checks (SQL/pandas) → expected vs actual.
  - `validation_suite.sql` — exact SQL used (for DB checks); group in `sql/` as needed.
  - `validation.md` — Show result of your findings and result vs the target(required) values given in task and show that in Clean Table form with what your data actually have and what was the target(requirement). Always Show correct Values of your finding and required ones in Proper Table form and Well indented. and in last also tell results in bullet points.
  - `agent.log` (chronological steps + timing + any warnings)
  - `errors/` (only if failures occur)

# MotherDuck Integration (MCP)
**You MUST persist the final datasets to MotherDuck using the available MCP SQL tool** (name/description will indicate it executes SQL against DuckDB/MotherDuck; pick that tool). The canonical load flow:

1) **Discover SQL tool**:
   - From the available tools in this runtime, choose the one whose description clearly states it executes SQL against DuckDB/MotherDuck.
   - If no such tool is available, write `errors/no_sql_tool.md` and STOP.

2) **Create schema & tables** (use safe identifiers):
   - Schema: `{schema_name}`
   - For each CSV `{work_dir}/csv/<table>.csv`, produce **DDL** that mirrors the on-disk schema (types/NULL/PK/FKs if specified).
   - Prefer explicit types (e.g., `INTEGER`, `BIGINT`, `DOUBLE`, `DECIMAL(p,s)`, `BOOLEAN`, `DATE`, `TIMESTAMP`, `VARCHAR`).

3) **Bulk load (fast path)**:
   - Preferred:
     ```sql
     CREATE SCHEMA IF NOT EXISTS {schema_name};

     -- Option A (auto-detect with full scan)
     CREATE OR REPLACE TABLE {schema_name}.<table> AS
     SELECT * FROM read_csv_auto('<ABSOLUTE_PATH_TO_CSV>',
       SAMPLE_SIZE = -1,      -- scan full file for stable inference
       DATEFORMAT = 'YYYY-MM-DD',
       TIMESTAMPFORMAT = 'YYYY-MM-DD HH:MM:SS',
       HEADER = TRUE
     );

     -- Option B (if DDL was created explicitly)
     COPY {schema_name}.<table> FROM '<ABSOLUTE_PATH_TO_CSV>'
     (AUTO_DETECT=TRUE, SAMPLE_SIZE=-1, HEADER=TRUE);
     ```

   - If you need strict typing, **create the table first** with exact types, then `COPY` into it.

4) **Row count & integrity checks (post-load)**:
   - Verify row counts match CSV counts.
   - Run key uniqueness, FK referential checks, allowed-set checks, min/max/range checks.
   - Save all queries to `sql/postload_checks_<table>.sql` and results to `validation.md`.

5) **Large files / operation-limit guard**:
   If you hit: **"Reached the max number of operations of 10000000"** do the following **automatically**:
   - Switch to **chunked loading**:
     - Split CSV into multiple chunks on disk and load chunk-by-chunk with `COPY`.
     - Or load via a **staging** table using `read_csv_auto()` with filters (e.g., limited `LIMIT` per slice), then `INSERT INTO final SELECT * FROM staging` in batches.
   - Avoid row-wise inserts; always use set-based SQL.
   - Where helpful, first write **Parquet** from pandas (columnar) and then:
     ```sql
     CREATE OR REPLACE TABLE {schema_name}.<table> AS
     SELECT * FROM read_parquet('<ABSOLUTE_PATH_TO_PARQUET>');
     ```
   - Re-run post-load checks and log the mitigation in `load_report.md`.

# Generation Workflow (Strict Order)
1) **Intake & Planning**
   - Read any provided schema/spec files using `file_reader`.
   - Write `generation_spec.md` summarizing: task, requirements, constraints, seed, distributions, correlations, edge cases.
   - If any **critical** requirement is missing → write `ERROR_missing_schema.md` and **STOP**.

2) **Deterministic Data Build**
   - Set random seed.
   - Generate in **logical entity order** (dimensions first, then facts).
   - Enforce types and constraints at generation time (no later “fixes”).
   - Save to CSV under `{work_dir}/csv/`. Filenames = intended table names (snake_case).

3) **Local Self-Validation (pre-DB)**
   - Using pandas/NumPy, compute:
     - row counts, uniqueness of keys, FK joinability, null rates, min/max/range, distribution targets, business rules (e.g., year-over-year growth).
   - If any check fails → **fix data** and regenerate. Iterate until green.

4) **MotherDuck Load (MCP)**
   - Create schema if needed.
   - Load every CSV via `read_csv_auto` or `COPY`.
   - After each table load, record:
     - loaded row count,
     - any warnings,
     - time taken.
   - Save all DDL and DML to `sql/` and a summary to `load_report.md`.

5) **DB-Level Validation (authoritative)**
   - Run **the same validation suite in SQL** inside MotherDuck to prove parity with local checks.
   - Save the exact queries and results to `validation.md`.

6) **Deliverables Finalization**
   - `data_dictionary.md` (per table: column → type, description, allowed values/null %, example values).
   - `validation.md` (requirements → checks → results → PASS/FAIL).
   - `load_report.md` (schema/table list, DDL/COPY used, row counts, timings, any mitigations like chunking/parquet, and final status).
   - Append a **Run Summary** to `logs/agent.log`.

# Validation Requirements (Examples—adapt/extend)
- **Schema fidelity**: columns & types exactly match the provided schema.
- **Keys**: primary keys unique; foreign keys fully joinable (no orphans).
- **Ranges/distributions**: match targets (e.g., growth rate, seasonality, category shares).
- **Nullability**: respect declared null % windows.
- **Business rules**: every rule in the task must be explicitly checked with a clear PASS/FAIL.

# Example SQL Snippets (reuse/adapt)
- Row count:
  ```sql
  SELECT '{schema_name}.<table>' AS table_name, COUNT(*) AS rows FROM {schema_name}.<table>;
"""
    return prompt.replace("<<WORK_DIR>>", work_dir).replace("<<TASK>>", task)


# === Authorized imports ===
import smolagents.local_python_executor as lpe
lpe.MAX_OPERATIONS = 10000000000          # global op budget
# Some versions also expose a per-while-loop cap:
# lpe.MAX_WHILE_ITERATIONS = 2_000_000

AUTHORIZED_IMPORTS_CORE = [
  # OS / paths / IO
  "os", "sys", "pathlib", "shutil", "tempfile", "glob", "io", "ntpath", "posixpath", "stat",
  # Data interchange
  "json", "csv", "pickle",
  # Compression & archives
  "gzip", "bz2", "lzma", "zipfile", "tarfile",
  # Time & dates
  "time", "datetime", "calendar", "zoneinfo",
  # Numerics & math
  "math", "statistics", "decimal", "fractions", "random", "secrets",
  # Iteration / functional
  "itertools", "functools", "operator", "heapq", "bisect",
  # Text & regex
  "re", "string", "textwrap", "unicodedata",
  # Types / data containers
  "typing", "collections", "collections.abc", "dataclasses",
  # IDs / hashing
  "uuid", "hashlib", "hmac", "base64",
  # Logging / debug
  "logging", "warnings", "traceback", "pprint",

  # DataFrames & arrays
  "pandas", "numpy", "numpy.random",
]
AUTHORIZED_IMPORTS_COLUMNAR = [
  # Columnar backends (Parquet/Feather/Arrow)
  "pyarrow", "pyarrow.csv", "pyarrow.parquet", "pyarrow.feather",
  "pyarrow.dataset", "pyarrow.compute", "pyarrow.types",
  # Alternative Parquet engine
  "fastparquet",
  # Optional: fast DataFrame engine
  "polars",
  # Optional: embedded analytics DB (handy for QA, not needed if MCP handles SQL)
  "duckdb",
]
AUTHORIZED_IMPORTS_STATS = [
  # Rich distributions & utilities
  "scipy", "scipy.stats", "scipy.sparse",
  # Time-series / econ (optional)
  "statsmodels", "statsmodels.api",
  # Sampling, scaling, synthetic helpers (optional)
  "sklearn", "sklearn.datasets", "sklearn.preprocessing", "sklearn.utils",
]
AUTHORIZED_IMPORTS_SYNTH = [
  # Names, addresses, brands, etc. (optional)
  "faker",
  # Alternative to faker (optional)
  "mimesis",
  # Regex-driven random strings (optional)
  "rstr",
]
AUTHORIZED_IMPORTS_DATES = [
  # Robust date math & parsing
  "dateutil", "dateutil.relativedelta", "dateutil.parser",
  # Legacy tz support (zoneinfo preferred)
  "pytz",
  # (Optional) holiday rules if present in your env
  "holidays",
]
AUTHORIZED_IMPORTS_CONCURRENCY = [
  "concurrent.futures", "threading", "multiprocessing", "queue"
]
AUTHORIZED_IMPORTS_FASTJSON = [
  "orjson", "ujson",          # fast JSON (use if available; fallback to stdlib json)
  "yaml", "tomllib", "toml",  # configs (tomllib is stdlib in py3.11+)
]
AUTHORIZED_IMPORTS_VALIDATION = [
  "jsonschema",
  "pydantic", "pydantic.dataclasses",
]
AUTHORIZED_IMPORTS_DB = [
  # DB connectivity
  "sqlalchemy",  # SQLAlchemy core/engine/inspect
  # DBAPI drivers (optional but recommended)
  "psycopg2", "pymysql", "pyodbc",
]

AUTHORIZED_IMPORTS_HEAVY = sorted(set(
    AUTHORIZED_IMPORTS_CORE
  + AUTHORIZED_IMPORTS_COLUMNAR
  + AUTHORIZED_IMPORTS_STATS
  + AUTHORIZED_IMPORTS_SYNTH
  + AUTHORIZED_IMPORTS_DATES
  + AUTHORIZED_IMPORTS_CONCURRENCY
  + AUTHORIZED_IMPORTS_FASTJSON
  + AUTHORIZED_IMPORTS_VALIDATION
  + AUTHORIZED_IMPORTS_DB
))

authorized_imports = AUTHORIZED_IMPORTS_HEAVY

# Model
model = LiteLLMModel(model_id="gpt-4.1")

# Paths
WORK_DIR = "./runs"
output_path = os.path.join(WORK_DIR, "outputs")
input_path = glob.glob(os.path.join(WORK_DIR, "input_data", "*"))
os.makedirs(output_path, exist_ok=True)

# ⬇️ NEW: Lazy MCP wiring (do NOT initialize at import time)
_MCP = None
_MCP_ATTACHED = False
mcp_tools = []  # will be filled on first use

def _ensure_mcp():
    global _MCP
    if _MCP is not None:
        return _MCP
    try:
        _MCP = get_mcp_client()  # starts MCP server on first use
        return _MCP
    except Exception:
        return None

def _maybe_attach_mcp_tools(agent: CodeAgent):
    """
    Attach MCP tools to the agent once, right before first use.
    Safe on Render Free: if MCP isn't available, we just skip without crashing.
    """
    global _MCP_ATTACHED, mcp_tools
    if _MCP_ATTACHED:
        return
    mcp = _ensure_mcp()
    if not mcp:
        return
    try:
        mcp_tools = mcp.get_tools()
        # add to agent
        if hasattr(agent, "add_tools"):
            agent.add_tools(mcp_tools)  # preferred if available
        elif hasattr(agent, "tools"):
            agent.tools.extend(mcp_tools)  # fallback
        _MCP_ATTACHED = True
    except Exception:
        # don't crash if tool attach fails
        pass

# Assemble tools (without MCP at import time)
tools_ = [
    FileWriterTool(),
    ListFilesTool(),
    FileReaderTool(),
    MotherDuckQueryTool(),

    # New DB tools
    PostgresQueryTool(), PostgresListTablesTool(),
    MySQLQueryTool(),    MySQLListTablesTool(),
    MSSQLQueryTool(),    MSSQLListTablesTool(),

    # CSV tools
    CSVListTablesTool(), CSVSQLTool(),
]

# Agent (one-shot/full-prompt mode)
single_agent = CodeAgent(
    model=model,
    name="data_science_agent_full",
    description="Synthetic Data Generator / Data Validator (full prompt one-shot).",
    tools=tools_,
    additional_authorized_imports=authorized_imports,
    max_steps=150,
    verbosity_level=1,
)

def kickoff(task: str = "", data_sources: str = "", single_agent: CodeAgent = single_agent) -> str:
    # Attach MCP tools only when needed
    _maybe_attach_mcp_tools(single_agent)
    prompt = build_data_agent_prompt(task, output_path)
    # IMPORTANT: reset=False to preserve conversation
    return single_agent.run(prompt, reset=False)

def chat_turn(message: str, single_agent: CodeAgent = single_agent) -> str:
    # Attach MCP tools only when needed
    _maybe_attach_mcp_tools(single_agent)
    # IMPORTANT: reset=False to preserve conversation
    return single_agent.run(message, reset=False)

def get_outputs_dir() -> str:
    return output_path
