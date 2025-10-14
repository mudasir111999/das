# backend/m.py
import os
import shutil
from smolagents import MCPClient
from mcp import StdioServerParameters

def _find_uvx() -> str | None:
    # Try common locations first, then PATH
    candidates = [
        os.environ.get("UVX_PATH"),
        "/opt/render/.local/bin/uvx",   # Render (curl installer)
        os.path.expanduser("~/.local/bin/uvx"),
        shutil.which("uvx"),
    ]
    for c in candidates:
        if c and shutil.which(c) or (c and os.path.exists(c)):
            return c
    return None

def _md_env_ok() -> tuple[bool, str | None]:
    url = os.environ.get("MD_DUCKDB_URL", "md:my_db")
    token = os.environ.get("MOTHERDUCK_TOKEN")
    if str(url).startswith("md:") and not token:
        return False, "MOTHERDUCK_TOKEN is required when MD_DUCKDB_URL starts with md:…"
    return True, None

def build_motherduck_stdio(quiet: bool = True) -> StdioServerParameters:
    url = os.environ.get("MD_DUCKDB_URL", "md:my_db")
    token = os.environ.get("MOTHERDUCK_TOKEN")
    args = ["mcp-server-motherduck", "--url", url]
    if token:
        args += ["--token", token]
    if quiet:
        args = ["--quiet", *args]

    env = {"UV_PYTHON": os.environ.get("UV_PYTHON", "3.12"), **os.environ}
    uvx = _find_uvx()
    if not uvx:
        raise RuntimeError(
            "uvx not found. Ensure PATH includes $HOME/.local/bin or set UVX_PATH=/opt/render/.local/bin/uvx"
        )
    return StdioServerParameters(command=uvx, args=args, env=env)

# -------- Lazy singleton --------
_MCP_CLIENT: MCPClient | None = None

def get_mcp_client(connect_timeout: float = 90.0) -> MCPClient:
    """
    Lazily start/connect to the MotherDuck MCP server on first use.
    Never run at import/startup.
    """
    global _MCP_CLIENT
    if _MCP_CLIENT is not None:
        return _MCP_CLIENT

    ok, msg = _md_env_ok()
    if not ok:
        raise RuntimeError(msg)

    params = build_motherduck_stdio(quiet=True)
    client = MCPClient(params, connect_timeout=connect_timeout)
    # Explicit connect() for clarity across smolagents versions
    client.connect()
    _MCP_CLIENT = client
    return _MCP_CLIENT
