"use strict";

const $ = (selector) => document.querySelector(selector);
const NS = "http://www.w3.org/2000/svg";
let state = null;
let pinned = 0;
let hovered = null;
let highlightedFinger = null;
let inflight = false;
let requestQueue = Promise.resolve();
let queuedActions = 0;
let toastTimer;
const nodeButtons = new Map();
const format = (id) => id === null || id === undefined ? "—" : String(id).padStart(3, "0");
const position = (id, radius = 258) => {
  const angle = id / 256 * Math.PI * 2 - Math.PI / 2;
  return { x: 380 + Math.cos(angle) * radius, y: 340 + Math.sin(angle) * radius };
};
const currentNode = () => state?.nodes.find((node) => node.id === (hovered ?? pinned));
const isOffline = (id) => id === null || state.nodes.find((node) => node.id === id)?.online === false;

function toast(message) {
  $("#toast").textContent = message;
  $("#toast").hidden = false;
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => { $("#toast").hidden = true; }, 3500);
}

async function request(path, body) {
  const response = await fetch(path, body === undefined ? { cache: "no-store" } : {
    method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body),
  });
  const data = await response.json();
  if (!response.ok) throw new Error(data.error || `Request failed (${response.status})`);
  return data;
}

function connectionError(error) {
  $("#connection-error").hidden = false;
  $("#connection-error").textContent = `Connection interrupted: ${error.message}. Keep the Python visualizer running; reconnecting automatically.`;
}

function enqueue(task) {
  const result = requestQueue.then(task, task);
  requestQueue = result.catch(() => {});
  return result;
}

function action(path, body = {}) {
  queuedActions += 1;
  return enqueue(async () => {
    try {
      state = await request(path, body);
      $("#connection-error").hidden = true;
      render();
    } catch (error) { connectionError(error); }
    finally { queuedActions -= 1; }
  });
}

async function poll() {
  if (!inflight && queuedActions === 0) {
    inflight = true;
    await enqueue(async () => {
      try {
        state = await request("/api/state");
        $("#connection-error").hidden = true;
        render();
      } catch (error) { connectionError(error); }
      finally { inflight = false; }
    });
  }
  setTimeout(poll, 300);
}

function buildNodes() {
  for (const node of state.nodes) {
    const point = position(node.id);
    const button = document.createElement("button");
    button.className = "node-button";
    button.style.left = `${point.x / 760 * 100}%`;
    button.style.top = `${point.y / 680 * 100}%`;
    button.dataset.id = node.id;
    button.innerHTML = `<span class="node-disc">${format(node.id)}</span>`;
    button.addEventListener("mouseenter", () => { hovered = node.id; highlightedFinger = null; renderSelection(); });
    button.addEventListener("mouseleave", () => { hovered = null; renderSelection(); });
    button.addEventListener("focus", () => { hovered = node.id; renderSelection(); });
    button.addEventListener("blur", () => { hovered = null; renderSelection(); });
    button.addEventListener("click", () => { pinned = node.id; hovered = null; renderSelection(); });
    button.addEventListener("contextmenu", async (event) => {
      event.preventDefault();
      pinned = node.id;
      hovered = null;
      if (state.nodes.find((candidate) => candidate.id === node.id).online) {
        await action("/api/crash", { id: node.id });
        if (!state.nodes.find((candidate) => candidate.id === node.id).online) toast(`Node ${format(node.id)} crashed. Watch periodic maintenance repair the ring.`);
      } else { toast(`Node ${format(node.id)} is already crashed. Restore it in the inspector.`); }
      renderSelection();
    });
    $("#nodes").append(button);
    nodeButtons.set(node.id, button);
    const start = position(node.id, 278), end = position(node.id, 284);
    const line = document.createElementNS(NS, "line");
    Object.entries({ x1: start.x, y1: start.y, x2: end.x, y2: end.y, class: "tick-mark" }).forEach(([key, value]) => line.setAttribute(key, value));
    $("#tick-marks").append(line);
  }
}

function edge(source, target, className, marker, bend = 0) {
  const a = position(source), b = position(target);
  const distance = Math.hypot(b.x - a.x, b.y - a.y);
  if (distance < 1) return null;
  const path = document.createElementNS(NS, "path");
  const inset = 19;
  const controlX = (a.x + b.x) / 2 + (380 - (a.x + b.x) / 2) * bend;
  const controlY = (a.y + b.y) / 2 + (340 - (a.y + b.y) / 2) * bend;
  const fromDistance = Math.hypot(controlX - a.x, controlY - a.y);
  const toDistance = Math.hypot(controlX - b.x, controlY - b.y);
  const ax = a.x + (controlX - a.x) * inset / fromDistance;
  const ay = a.y + (controlY - a.y) * inset / fromDistance;
  const bx = b.x + (controlX - b.x) * inset / toDistance;
  const by = b.y + (controlY - b.y) * inset / toDistance;
  path.setAttribute("d", `M ${ax} ${ay} Q ${controlX} ${controlY} ${bx} ${by}`);
  path.setAttribute("class", className);
  path.setAttribute("marker-end", `url(#arrow-${marker})`);
  return path;
}

function renderEdges() {
  $("#successor-edges").replaceChildren();
  $("#finger-edges").replaceChildren();
  if ($("#show-successors").checked) {
    for (const node of state.nodes.filter((node) => node.online && node.successor !== null)) {
      const stale = isOffline(node.successor);
      const path = edge(node.id, node.successor, `successor-edge${stale ? " stale" : ""}`, stale ? "red" : "mint", -0.02);
      if (path) $("#successor-edges").append(path);
    }
  }
  const node = currentNode();
  if (!node || !$("#show-fingers").checked) return;
  const targets = new Map();
  for (const finger of node.fingers) {
    if (finger.target === null) continue;
    if (!targets.has(finger.target)) targets.set(finger.target, []);
    targets.get(finger.target).push(finger.index);
  }
  for (const [target, indices] of targets) {
    const stale = isOffline(target) || !node.online;
    const highlight = highlightedFinger !== null && indices.includes(highlightedFinger);
    const className = `finger-edge${stale ? " stale" : ""}${highlight ? " highlight" : highlightedFinger !== null ? " dimmed" : ""}`;
    const path = edge(node.id, target, className, stale ? "red" : "violet", 0.12);
    if (path) {
      const title = document.createElementNS(NS, "title");
      title.textContent = `Finger ${indices.join(", ")} → node ${format(target)}`;
      path.append(title);
      $("#finger-edges").append(path);
    }
  }
}

function renderSelection() {
  if (!state) return;
  const node = currentNode();
  for (const candidate of state.nodes) {
    const button = nodeButtons.get(candidate.id);
    button.classList.toggle("selected", candidate.id === node.id);
    button.classList.toggle("crashed", !candidate.online);
    button.setAttribute("aria-label", `Node ${format(candidate.id)}, ${candidate.online ? "online" : "crashed"}. Click to inspect; right-click to crash.`);
    button.setAttribute("aria-pressed", String(candidate.id === pinned));
  }
  $("#selected-id").textContent = format(node.id);
  $("#pin-label").textContent = hovered === null ? "Pinned" : "Hover preview";
  $("#node-status").textContent = node.online ? "Online" : "Crashed";
  $("#node-status").className = `badge${node.online ? "" : " crashed"}`;
  $("#node-identity").textContent = `8-bit ID · 0x${node.id.toString(16).padStart(2, "0").toUpperCase()}`;
  for (const key of ["predecessor", "successor"]) {
    $(`#${key}`).textContent = format(node[key]);
    $(`#${key}`).classList.toggle("unavailable", isOffline(node[key]));
  }
  const tableMarkup = node.fingers.map((finger) => {
    const changed = node.last_check?.changed_fingers.includes(finger.index) && node.last_check.tick === state.tick;
    const status = !node.online ? "Frozen" : finger.stale ? "Unreachable" : finger.correct ? "Ready" : "Pending";
    const statusClass = !node.online ? "offline" : finger.stale ? "stale" : finger.correct ? "" : "pending";
    return `<tr data-finger="${finger.index}" class="${changed ? "changed" : ""}" tabindex="0" aria-label="Finger ${finger.index}, start ${finger.start}, target ${format(finger.target)}, ${status}"><td>${finger.index}</td><td>${format(finger.start)}</td><td class="target ${finger.stale ? "stale" : ""}">↗ ${format(finger.target)}</td><td><span class="entry-status ${statusClass}">${status}</span></td></tr>`;
  }).join("");
  // Keep focused rows intact during background polling when their data is unchanged.
  if ($("#finger-table").innerHTML !== tableMarkup) $("#finger-table").innerHTML = tableMarkup;
  $("#successor-list").innerHTML = node.successors.map((id, index) => `${index ? '<span class="list-arrow">→</span>' : ""}<span class="successor-chip ${isOffline(id) ? "stale" : ""}">${format(id)}</span>`).join("");
  const check = node.last_check;
  $("#last-check-tick").textContent = check ? `Tick ${check.tick}` : "Waiting";
  $("#check-detail").textContent = !node.online ? "Maintenance stopped. This node retains its last routing table." : check?.errors.length ? check.errors.join(" · ") : check ? `Predecessor checked · successor stabilized · finger ${check.finger_index ?? "—"} refreshed.` : "Checks begin on the next maintenance tick.";
  $("#node-action").textContent = `${node.online ? "Crash" : "Restore"} node ${format(node.id)}`;
  $("#node-action").className = `button ${node.online ? "danger" : "restore"}`;
  $("#crash-note").textContent = node.online ? "Peers repair their own tables on subsequent ticks." : "Restores this endpoint; periodic checks rediscover it.";
  renderEdges();
}

function render() {
  if (!nodeButtons.size) buildNodes();
  $("#online").textContent = state.online;
  $("#center-online").textContent = state.online;
  $("#offline-label").textContent = state.online === 32 ? "All peers available" : `${32 - state.online} crashed`;
  $("#tick").textContent = String(state.tick).padStart(3, "0");
  $("#run-label").textContent = state.paused ? "Paused" : `Every ${state.interval}s`;
  $("#stale").textContent = state.stale_fingers;
  $("#stale").closest(".metric").classList.toggle("alert", state.stale_fingers > 0);
  $("#accuracy").textContent = state.total_fingers ? `${Math.round(100 * state.correct_fingers / state.total_fingers)}% correct` : "No live tables";
  $("#repairs").textContent = state.repairs;
  const status = state.online === 0 ? "All nodes offline" : state.converged ? "Ring converged" : "Repair in progress";
  $("#ring-status").textContent = status;
  $("#ring-status").className = `badge${state.online === 0 ? " crashed" : state.converged ? "" : " repairing"}`;
  $("#center-status").textContent = state.online === 0 ? "Restore a peer or reset the ring" : state.converged ? "Routing tables are consistent" : state.paused ? "Repair paused · step or resume" : "Periodic checks are repairing routes";
  $("#center-status").classList.toggle("repairing", !state.converged);
  $("#pause").innerHTML = state.paused ? 'Resume <span aria-hidden="true">▶</span>' : 'Pause <span aria-hidden="true">Ⅱ</span>';
  $("#live-indicator").classList.toggle("paused", state.paused);
  $("#speed").value = String(state.interval);
  $("#step").disabled = !state.paused;
  $("#step").title = state.paused ? "Run one maintenance tick" : "Pause to step through individual ticks";
  $("#checks-count").textContent = `${state.checks.toLocaleString()} operations`;
  $("#event-log").innerHTML = state.events.slice(0, 36).map((event) => `<div class="event-row"><span class="event-tick">T${String(event.tick).padStart(3, "0")}</span><span class="event-kind ${event.kind}">${event.kind.replaceAll("_", " ")}</span><span class="event-node">${event.node === null ? "Network" : `Node ${format(event.node)}`}</span><span class="event-message">${escapeText(event.message)}</span></div>`).join("");
  renderSelection();
}

function escapeText(text) {
  return text.replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;").replaceAll('"', "&quot;");
}

$("#pause").addEventListener("click", () => state && action("/api/control", { paused: !state.paused }));
$("#step").addEventListener("click", () => action("/api/step"));
$("#speed").addEventListener("change", (event) => action("/api/control", { interval: Number(event.target.value) }));
$("#reset").addEventListener("click", () => { pinned = 0; hovered = null; action("/api/reset"); });
$("#node-action").addEventListener("click", () => {
  const node = currentNode();
  if (node) action(node.online ? "/api/crash" : "/api/restore", { id: node.id });
});
$("#show-fingers").addEventListener("change", () => state && renderEdges());
$("#show-successors").addEventListener("change", () => state && renderEdges());
$("#finger-table").addEventListener("mouseover", (event) => {
  const row = event.target.closest("tr[data-finger]");
  if (row && highlightedFinger !== Number(row.dataset.finger)) { highlightedFinger = Number(row.dataset.finger); renderEdges(); }
});
$("#finger-table").addEventListener("mouseleave", () => { highlightedFinger = null; if (state) renderEdges(); });
$("#finger-table").addEventListener("focusin", (event) => { highlightedFinger = Number(event.target.dataset.finger); renderEdges(); });
$("#finger-table").addEventListener("focusout", () => { highlightedFinger = null; renderEdges(); });
poll();
