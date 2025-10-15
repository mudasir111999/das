/* Two-mode UI + newest-run CSVs + Validation drawer + static page scrolling */

const chatEl = document.getElementById("chat");
const inputEl = document.getElementById("msg");
const sendBtn = document.getElementById("sendBtn");
const beginConvoBtn = document.getElementById("beginConvoBtn");
const beginFullBtn = document.getElementById("beginFullBtn");
const refreshBtn = document.getElementById("refreshBtn");
const filesEl = document.getElementById("files");
const runsBackBtn = document.getElementById("runsBackBtn");
const runsFwdBtn = document.getElementById("runsFwdBtn");
const runsUpBtn = document.getElementById("runsUpBtn");
const runsBreadcrumb = document.getElementById("runsBreadcrumb");

// Explorer state
let runsHistory = [""]; // start at root of runs/
let historyIndex = 0;
let cwd = "";
const typingEl = document.getElementById("typing");
const composerEl = document.querySelector(".composer");


/* Drawer elements */
const openValidationBtn = document.getElementById("openValidationBtn");
const validationTab = document.getElementById("validationTab");
const drawer = document.getElementById("validationDrawer");
const drawerBackdrop = document.getElementById("drawerBackdrop");
const refreshValidationBtn = document.getElementById("refreshValidationBtn");
const closeValidationBtn = document.getElementById("closeValidationBtn");
const validationBodyEl = document.getElementById("validationDrawerBody");

let mode = null;        // 'convo' | 'full'
let fullUsed = false;   // one-shot guard

function addMsg(role, text) {
  const row = document.createElement("div");
  row.className = `msg msg--${role}`;
  const bubble = document.createElement("div");
  bubble.className = `bubble bubble--${role}`;
  bubble.textContent = text;
  row.appendChild(bubble);
  chatEl.appendChild(row);
  chatEl.scrollTop = chatEl.scrollHeight;
}

function setThinking(on) {
  typingEl.classList.toggle("hidden", !on);
}

/* Autosize textarea (clamped by CSS) */
function autoSize(el) {
  el.style.height = "auto";
  const max = parseInt(getComputedStyle(el).maxHeight, 10) || 0;
  const next = el.scrollHeight + 2;
  el.style.height = (max && next > max ? max : next) + "px";
}
requestAnimationFrame(() => autoSize(inputEl));

/* Minimal markdown render for Validation.md */
function renderMarkdown(md) {
  if (!md || !md.trim()) return '<div class="muted">No validation report yet.</div>';
  let html = md
    .replace(/^### (.*)$/gm, "<h3>$1</h3>")
    .replace(/^## (.*)$/gm, "<h2>$1</h2>")
    .replace(/^# (.*)$/gm, "<h1>$1</h1>")
    .replace(/```([\s\S]*?)```/g, (_, code) => `<pre><code>${escapeHtml(code)}</code></pre>`)
    .replace(/`([^`]+)`/g, "<code>$1</code>")
    .replace(/\n{2,}/g, "</p><p>");
  return `<p>${html}</p>`;
}
function escapeHtml(s){ return s.replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c])); }

/* Modes */
async function startConversational() {
  mode = "convo"; fullUsed = false;
  composerEl.classList.remove("hidden");
  inputEl.placeholder = "Type your message…  (Enter to send • Shift+Enter for newline)";
  inputEl.value = ""; autoSize(inputEl);

  addMsg("agent", "Starting conversational agent…");
  setThinking(true);
  try {
    const res = await fetch("/api/start_convo", { method: "POST" });
    const data = await res.json();
    if (data.ok) addMsg("agent", data.reply || "(no reply)");
    else addMsg("agent", "Failed to start conversational mode.");
  } catch {
    addMsg("agent", "Network error starting conversational mode.");
  } finally {
    setThinking(false);
    refreshFiles();
    // optional: preload validation into drawer when run starts
    refreshValidation();
  }
}

function startFullPromptMode() {
  mode = "full"; fullUsed = false;
  composerEl.classList.remove("hidden");
  inputEl.value = "";
  inputEl.placeholder = "Paste your full prompt (single use).  Enter to run • Shift+Enter for newline";
  autoSize(inputEl);
  addMsg("agent", "Full Prompt mode: send ONE prompt. After that the input will disappear.");
}

async function sendMessage() {
  const text = (inputEl.value || "").trim();
  if (!text) return;
  if (mode === "full" && fullUsed) return;

  addMsg("user", text);
  inputEl.value = ""; autoSize(inputEl); inputEl.focus();
  sendBtn.disabled = true; setThinking(true);

  try {
    if (mode === "convo") {
      const res = await fetch("/api/chat_convo", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ message: text }),
      });
      const data = await res.json();
      if (data.ok) addMsg("agent", data.reply || "(no reply)");
      else addMsg("agent", "Agent error.");
    } else if (mode === "full") {
  const endpoint = fullUsed ? "/api/chat_full" : "/api/start_full";
  const res = await fetch(endpoint, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ message: text }),
  });
  const data = await res.json();
  if (data.ok) addMsg("agent", data.reply || "(no reply)");
  else addMsg("agent", "Agent error.");
  fullUsed = true;
} else {
      addMsg("agent", "Pick a mode: Conversational or Full Prompt.");
    }
  } catch {
    addMsg("agent", "Network error.");
  } finally {
    sendBtn.disabled = false; setThinking(false);
    refreshFiles();
    refreshValidation();
  }
}

/* Files list: newest run only (backend already filters) */
async function refreshFiles() {
  try {
    const path = runsHistory[historyIndex] || "";
    const res = await fetch(`/api/runs/list?path=${encodeURIComponent(path)}`);
    const data = await res.json();
    filesEl.innerHTML = "";

    if (!data.ok) throw new Error("List failed");
    cwd = data.cwd || "";
    renderBreadcrumb(cwd);

    const entries = Array.isArray(data.entries) ? data.entries : [];
    if (!entries.length) {
      filesEl.innerHTML = `<div class="table__row"><div class="muted">Folder is empty</div><div class="right"></div></div>`;
      return;
    }

    for (const e of entries) {
      const row = document.createElement("div");
      row.className = "table__row";
      const left = document.createElement("div");

      const name = document.createElement("div");
      name.textContent = e.name + (e.type === "dir" ? "/" : "");
      name.style.fontWeight = "500";
      left.appendChild(name);

      const meta = document.createElement("div");
      meta.className = "muted";
      const sizeStr = (e.type === "dir") ? "Folder" : formatSize(e.size);
      const dateStr = e.mtime ? new Date(e.mtime * 1000).toLocaleString() : "";
      meta.textContent = `${sizeStr}${dateStr ? " • " + dateStr : ""}`;
      left.appendChild(meta);

      const right = document.createElement("div");
      right.className = "right";

      if (e.type === "dir") {
        const openBtn = document.createElement("button");
        openBtn.className = "btn btn--ghost";
        openBtn.textContent = "Open";
        openBtn.addEventListener("click", () => navigateTo(e.path));
        right.appendChild(openBtn);
        name.style.cursor = "pointer";
        name.addEventListener("click", () => navigateTo(e.path));
      } else {
        const a = document.createElement("a");
        a.href = `/api/runs/download?path=${encodeURIComponent(e.path)}`;
        a.className = "btn btn--ghost";
        a.textContent = "Download";
        a.setAttribute("download", "");
        right.appendChild(a);
      }

      row.appendChild(left);
      row.appendChild(right);
      filesEl.appendChild(row);
    }
  } catch (err) {
    filesEl.innerHTML = `<div class="table__row"><div class="muted">Failed to load. ${escapeHtml(String(err))}</div><div class="right"></div></div>`;
  }
}

function renderBreadcrumb(path) {
  const parts = path ? path.split('/') : [];
  const crumbs = ['<span data-path="">runs</span>'];
  let acc = "";
  for (const p of parts) {
    if (!p) continue;
    acc = acc ? acc + "/" + p : p;
    crumbs.push('<span class="sep">/</span>');
    crumbs.push(`<span data-path="${acc}">${p}</span>`);
  }
  runsBreadcrumb.innerHTML = crumbs.join("");
  runsBreadcrumb.querySelectorAll("span[data-path]").forEach(el => {
    el.addEventListener("click", () => {
      navigateTo(el.getAttribute("data-path"));
    });
  });
}

function navigateTo(path) {
  runsHistory = runsHistory.slice(0, historyIndex + 1);
  runsHistory.push(path || "");
  historyIndex++;
  refreshFiles();
  updateNavButtons();
}

function goBack() {
  if (historyIndex > 0) {
    historyIndex--;
    refreshFiles();
    updateNavButtons();
  }
}
function goForward() {
  if (historyIndex < runsHistory.length - 1) {
    historyIndex++;
    refreshFiles();
    updateNavButtons();
  }
}
function goUp() {
  const cur = runsHistory[historyIndex] || "";
  if (!cur) return;
  const parent = cur.split('/').slice(0, -1).join('/');
  navigateTo(parent);
}

function updateNavButtons() {
  runsBackBtn.disabled = historyIndex <= 0;
  runsFwdBtn.disabled = historyIndex >= runsHistory.length - 1;
  runsUpBtn.disabled = !(runsHistory[historyIndex] || "");
}

function formatSize(bytes) {
  if (bytes == null) return "";
  const units = ["B","KB","MB","GB","TB"];
  let i=0, n=bytes;
  while (n >= 1024 && i < units.length-1) { n/=1024; i++; }
  return `${n.toFixed(n<10 && i>0 ? 1 : 0)} ${units[i]}`;
}


/* Validation drawer */
function openDrawer() {
  drawer.classList.add("open");
  drawerBackdrop.classList.add("open");
  drawer.setAttribute("aria-hidden", "false");
  drawerBackdrop.setAttribute("aria-hidden", "false");
  refreshValidation();
}
function closeDrawer() {
  drawer.classList.remove("open");
  drawerBackdrop.classList.remove("open");
  drawer.setAttribute("aria-hidden", "true");
  drawerBackdrop.setAttribute("aria-hidden", "true");
}
async function refreshValidation() {
  try {
    const res = await fetch("/api/validation");
    const data = await res.json();
    validationBodyEl.innerHTML = renderMarkdown(data.md || "");
  } catch {
    validationBodyEl.innerHTML = '<div class="muted">Failed to load validation report.</div>';
  }
}

/* Events */
beginConvoBtn?.addEventListener("click", startConversational);
beginFullBtn?.addEventListener("click", startFullPromptMode);
sendBtn.addEventListener("click", sendMessage);

inputEl.addEventListener("input", () => autoSize(inputEl));
inputEl.addEventListener("keydown", (e) => {
  // Enter = send; Shift+Enter = newline
  if (e.key === "Enter" && !e.shiftKey) {
    e.preventDefault();
    sendMessage();
  }
});

refreshBtn.addEventListener("click", refreshFiles);
runsBackBtn?.addEventListener("click", goBack);
runsFwdBtn?.addEventListener("click", goForward);
runsUpBtn?.addEventListener("click", goUp);


/* Drawer event wiring */
openValidationBtn?.addEventListener("click", openDrawer);
validationTab?.addEventListener("click", openDrawer);
closeValidationBtn?.addEventListener("click", closeDrawer);
drawerBackdrop?.addEventListener("click", closeDrawer);
window.addEventListener("keydown", (e) => {
  if (e.key === "Escape") closeDrawer();
});
refreshValidationBtn?.addEventListener("click", refreshValidation);

/* Boot */
refreshFiles();
updateNavButtons();
