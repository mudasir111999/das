# backend/main.py
import os, time, logging
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from typing import Optional, List

# Conversational agent (existing)
from .agent_setup import kickoff as kickoff_convo, chat_turn as chat_convo, get_outputs_dir as outputs_dir_convo
# Full prompt agent (one-shot)
from .agent_full_setup import start_full_prompt, get_outputs_dir as outputs_dir_full

APP_TITLE = "Synthetic Data Agents"
logger = logging.getLogger(__name__)

# --- App ---
app = FastAPI(title=APP_TITLE, docs_url="/docs", redoc_url="/redoc")

# --- Static (guarded mount so startup never fails if folder is missing) ---
FRONTEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "frontend"))
if os.path.isdir(FRONTEND_DIR):
    app.mount("/static", StaticFiles(directory=FRONTEND_DIR), name="static")
else:
    logger.warning("Frontend directory not found: %s (skipping /static mount)", FRONTEND_DIR)

# --- Models ---
class ChatIn(BaseModel):
    message: str

# ---- Run tracking (active/newest) ----
RUN_START_TS: float = 0.0
ACTIVE_RUN_DIR: Optional[str] = None

def _outputs_dir() -> str:
    # both agents currently share the same parent outputs folder
    return outputs_dir_convo()

def _stamp_run_start():
    global RUN_START_TS, ACTIVE_RUN_DIR
    RUN_START_TS = time.time()
    ACTIVE_RUN_DIR = None

def _list_subdirs_sorted_by_mtime(base: str) -> List[str]:
    if not os.path.isdir(base):
        return []
    subs = [os.path.join(base, d) for d in os.listdir(base)]
    subs = [p for p in subs if os.path.isdir(p)]
    subs.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    return subs

def _resolve_active_run_dir() -> Optional[str]:
    """
    Choose the newest subfolder in outputs whose mtime is >= RUN_START_TS.
    If none meets the threshold, fall back to absolute newest subfolder.
    Cache to ACTIVE_RUN_DIR.
    """
    global ACTIVE_RUN_DIR
    if ACTIVE_RUN_DIR and os.path.isdir(ACTIVE_RUN_DIR):
        return ACTIVE_RUN_DIR

    base = _outputs_dir()
    subs = _list_subdirs_sorted_by_mtime(base)
    # prefer subdirs created/modified after the run started
    for p in subs:
        if os.path.getmtime(p) >= RUN_START_TS:
            ACTIVE_RUN_DIR = p
            return ACTIVE_RUN_DIR
    # fallback to newest, if any
    if subs:
        ACTIVE_RUN_DIR = subs[0]
        return ACTIVE_RUN_DIR
    return None

def _list_active_csvs() -> List[str]:
    run_dir = _resolve_active_run_dir()
    if not run_dir:
        return []
    csvs = []
    for root, _, files in os.walk(run_dir):
        for name in files:
            if name.lower().endswith(".csv"):
                full = os.path.join(root, name)
                rel = os.path.relpath(full, run_dir).replace("\\", "/")
                csvs.append(rel)
    csvs.sort()
    return csvs

def _read_active_validation_md() -> Optional[str]:
    run_dir = _resolve_active_run_dir()
    if not run_dir:
        return None
    # common names: Validation.md / validation.md
    for fname in ("Validation.md", "validation.md"):
        p = os.path.join(run_dir, fname)
        if os.path.isfile(p):
            with open(p, "r", encoding="utf-8", errors="ignore") as f:
                return f.read()
    # fallback: search top-level for *.md with 'validation' in name
    try:
        for name in os.listdir(run_dir):
            if name.lower().endswith(".md") and "validation" in name.lower():
                p = os.path.join(run_dir, name)
                if os.path.isfile(p):
                    with open(p, "r", encoding="utf-8", errors="ignore") as f:
                        return f.read()
    except Exception:
        pass
    return None

# --- Startup: ensure PATH includes uv installer location so 'uvx' is found ---
@app.on_event("startup")
def _startup_env_patch():
    extra_paths = [os.path.expanduser("~/.local/bin"), "/opt/render/.local/bin"]
    current = os.environ.get("PATH", "")
    patched = current
    for p in extra_paths:
        if p and p not in current and os.path.isdir(p):
            patched = f"{patched}:{p}" if patched else p
    if patched != current:
        os.environ["PATH"] = patched
        logger.info("Extended PATH for MCP tools: %s", os.environ["PATH"])

# --- Health endpoints (for Render port binding + quick checks) ---
@app.get("/healthz", response_class=JSONResponse)
def healthz():
    return {"status": "ok", "service": APP_TITLE}

@app.get("/live", response_class=PlainTextResponse)
def live():
    return "OK"

@app.get("/ready", response_class=JSONResponse)
def ready():
    # lightweight readiness check (more can be added later)
    return {"ready": True}

# --- Index (guard file existence so startup never explodes) ---
@app.get("/", response_class=HTMLResponse)
def index():
    index_path = os.path.join(FRONTEND_DIR, "index.html")
    if os.path.isfile(index_path):
        with open(index_path, "r", encoding="utf-8") as f:
            return HTMLResponse(f.read())
    # minimal fallback UI
    return HTMLResponse(
        "<!doctype html><html><head><meta charset='utf-8'><title>SDA</title></head>"
        "<body><h1>Synthetic Data Agents</h1>"
        "<p>Frontend not found. API docs: <a href='/docs'>/docs</a></p></body></html>"
    )

# === Conversational mode ===
@app.post("/api/start_convo")
def api_start_convo():
    _stamp_run_start()
    reply = kickoff_convo(task="", data_sources="")
    # seed active dir in case agent already created one
    _resolve_active_run_dir()
    return {"ok": True, "reply": reply}

@app.post("/api/chat_convo")
def api_chat_convo(payload: ChatIn):
    reply = chat_convo(payload.message)
    # if a new folder appears during chat, update active dir
    _resolve_active_run_dir()
    return {"ok": True, "reply": reply}

# === Full prompt (one-shot) ===
@app.post("/api/start_full")
def api_start_full(payload: ChatIn):
    _stamp_run_start()
    reply = start_full_prompt(payload.message)
    _resolve_active_run_dir()
    return {"ok": True, "reply": reply}

# === Files limited to ACTIVE run ===
@app.get("/api/files")
def api_files():
    files = _list_active_csvs()
    return {"ok": True, "files": files, "active_run": _resolve_active_run_dir()}

# === Download limited to ACTIVE run ===
@app.get("/api/download")
def api_download(path: str = Query(..., description="Relative path under ACTIVE run")):
    run_dir = _resolve_active_run_dir()
    if not run_dir:
        raise HTTPException(status_code=404, detail="No active run.")
    safe_path = os.path.normpath(os.path.join(run_dir, path))
    if not os.path.abspath(safe_path).startswith(os.path.abspath(run_dir)):
        raise HTTPException(status_code=400, detail="Invalid path.")
    if not os.path.isfile(safe_path):
        raise HTTPException(status_code=404, detail="File not found.")
    return FileResponse(safe_path, filename=os.path.basename(safe_path))

# === Validation report for ACTIVE run ===
@app.get("/api/validation")
def api_validation():
    md = _read_active_validation_md()
    return {"ok": True, "md": md or ""}
