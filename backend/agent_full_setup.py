# backend/agent_full_setup.py
from dotenv import load_dotenv
load_dotenv()

import os, json, glob
from smolagents import CodeAgent, LiteLLMModel, Tool
# ⬇️ CHANGE: use the lazy getter instead of eager loader
from .m import get_mcp_client

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


def build_data_agent_prompt(task: str, work_dir: str) -> str:
    return f"""
# Role
You are a Senior Synthetic Data Engineer & Validator. You generate **realistic, big analysis-grade** datasets exactly to spec, **prove** they meet the requirements, and **persist them to MotherDuck** (via the available MCP tools). You work deterministically, efficiently, and defensively (great logs, graceful errors, no silent failures).

# Primary Objective
Complete this task:

{task}

…and produce:
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
   - **MCP MotherDuck tools** (already available in this runtime) to execute SQL and load data. **Do not** open ad-hoc DB connections.
6) **No Hidden Assumptions**: All assumptions must be written to `generation_spec.md`. If a critical requirement is unresolvable, **STOP** and produce an error file (and exit).

# Directory Convention
- Create a unique run folder: `{work_dir}/run_YYYYMMDD_HHMMSS/`
- Inside it, write:
  - `generation_spec.md` (requirements, assumptions, seed, generation plan)
  - `data_dictionary.md` (columns, types, null %, constraints, allowed sets)
  - `validation.md` (full checks + SQL + results)
  - `load_report.md` (DDL, COPY statements, table row counts, post-load checks)
  - `sql/` (all executed SQL files)
  - `csv/` (all datasets as CSV; filenames == table names)
  - `parquet/` (optional for efficiency; mirror of CSV)
  - `logs/agent.log` (chronological steps + timing + any warnings)
  - `errors/` (only if failures occur)

# MotherDuck Integration (MCP)
**You MUST persist the final datasets to MotherDuck using the available MCP SQL tool** (name/description will indicate “DuckDB/MotherDuck SQL”; pick that tool). The canonical load flow:

1) **Discover SQL tool**:
   - From the available tools in this runtime, choose the one whose description clearly states it executes SQL against DuckDB/MotherDuck.
   - If no such tool is available, write `errors/no_sql_tool.md` and STOP.

2) **Create schema & tables** (use safe identifiers):
   - Schema: `improve`
   - For each CSV `{work_dir}/csv/<table>.csv`, produce **DDL** that mirrors the on-disk schema (types/NULL/PK/FKs if specified).
   - Prefer explicit types (e.g., `INTEGER`, `BIGINT`, `DOUBLE`, `DECIMAL(p,s)`, `BOOLEAN`, `DATE`, `TIMESTAMP`, `VARCHAR`).

3) **Bulk load (fast path)**:
   - Preferred:
     ```sql
     CREATE SCHEMA IF NOT EXISTS improve;

     -- Option A (auto-detect with full scan)
     CREATE OR REPLACE TABLE improve.<table> AS
     SELECT * FROM read_csv_auto('<ABSOLUTE_PATH_TO_CSV>',
       SAMPLE_SIZE = -1,      -- scan full file for stable inference
       DATEFORMAT = 'YYYY-MM-DD',
       TIMESTAMPFORMAT = 'YYYY-MM-DD HH:MM:SS',
       HEADER = TRUE
     );

     -- Option B (if DDL was created explicitly)
     COPY improve.<table> FROM '<ABSOLUTE_PATH_TO_CSV>'
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
     CREATE OR REPLACE TABLE improve.<table> AS
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
  SELECT 'improve.<table>' AS table_name, COUNT(*) AS rows FROM improve.<table>;
"""


# do this before constructing CodeAgent
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
AUTHORIZED_IMPORTS_HEAVY = sorted(set(
    AUTHORIZED_IMPORTS_CORE
  + AUTHORIZED_IMPORTS_COLUMNAR
  + AUTHORIZED_IMPORTS_STATS
  + AUTHORIZED_IMPORTS_SYNTH
  + AUTHORIZED_IMPORTS_DATES
  + AUTHORIZED_IMPORTS_CONCURRENCY
  + AUTHORIZED_IMPORTS_FASTJSON
  + AUTHORIZED_IMPORTS_VALIDATION
))


authorized_imports = AUTHORIZED_IMPORTS_HEAVY


# Model
model = LiteLLMModel(model_id="gpt-4.1")

# Paths
WORK_DIR = "./runs"
output_path = os.path.join(WORK_DIR, "outputs")
input_path = glob.glob(os.path.join(WORK_DIR, "input_data", "*"))
os.makedirs(output_path, exist_ok=True)

# ⬇️ NEW: Lazy MCP wiring (no import-time connection)
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
    If MCP isn't available, skip without crashing.
    """
    global _MCP_ATTACHED, mcp_tools
    if _MCP_ATTACHED:
        return
    mcp = _ensure_mcp()
    if not mcp:
        return
    try:
        mcp_tools = mcp.get_tools()
        if hasattr(agent, "add_tools"):
            agent.add_tools(mcp_tools)
        elif hasattr(agent, "tools"):
            agent.tools.extend(mcp_tools)
        _MCP_ATTACHED = True
    except Exception:
        pass

# Your original toolset (no MCP at import time)
tools_ = [FileWriterTool(), ListFilesTool(), FileReaderTool()]

# Agent (one-shot/full-prompt mode)
single_agent_full = CodeAgent(
    model=model,
    name="data_science_agent_full",
    description="Synthetic Data Generator (full prompt one-shot).",
    tools=tools_,
    additional_authorized_imports=authorized_imports,
    max_steps=150,
    verbosity_level=1,
)

def start_full_prompt(task: str) -> str:
    # Attach MCP tools only when needed (first call)
    _maybe_attach_mcp_tools(single_agent_full)
    prompt = build_data_agent_prompt(task, output_path)
    # IMPORTANT: keep reset=False as requested
    return single_agent_full.run(prompt, reset=False)

def get_outputs_dir() -> str:
    return output_path
