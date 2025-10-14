/* Two-mode UI + newest-run CSVs + Validation drawer + static page scrolling */

const chatEl = document.getElementById("chat");
const inputEl = document.getElementById("msg");
const sendBtn = document.getElementById("sendBtn");
const beginConvoBtn = document.getElementById("beginConvoBtn");
const beginFullBtn = document.getElementById("beginFullBtn");
const refreshBtn = document.getElementById("refreshBtn");
const filesEl = document.getElementById("files");
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
    const res = await fetch("/api/files");
    const data = await res.json();
    filesEl.innerHTML = "";
    
    if (data.ok && Array.isArray(data.files)) {
      if (!data.files.length) {
        filesEl.innerHTML = `<div class="table__row"><div class="muted">No files yet</div><div class="right"></div></div>`;
        return;
      }
      data.files.forEach(rel => {
        const row = document.createElement("div");
        row.className = "table__row";
        const name = document.createElement("div"); name.textContent = rel;
        const act = document.createElement("div"); act.className = "right";
        const a = document.createElement("a");
        a.href = `/api/download?path=${encodeURIComponent(rel)}`;
        a.className = "btn btn--ghost"; a.textContent = "Download";
        a.setAttribute("download", "");
        act.appendChild(a); row.appendChild(name); row.appendChild(act);
        filesEl.appendChild(row);
      });
    }
  } catch { /* ignore */ }
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
