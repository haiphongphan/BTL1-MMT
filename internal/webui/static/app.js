// app.js — JavaScript thuần, không thư viện. Chỉ nói chuyện với process client local qua fetch().
'use strict';

const $ = (id) => document.getElementById(id);

let me = '';          // username của mình (lấy từ /api/status)
let since = 0;        // seq của event cuối đã thấy
let currentTab = '';  // '' = chat chung, ngược lại = tên người đang DM
const history = { '': [] };   // tab → danh sách tin
const unread = new Set();

// ---------- Poll event (mỗi 400 ms) ----------
async function poll() {
  try {
    const res = await fetch(`/api/events?since=${since}`);
    const events = await res.json();
    for (const ev of events) {
      since = ev.seq;
      handle(ev);
    }
  } catch (e) {
    setState('disconnected', '🔴 Không kết nối được client local');
  }
  setTimeout(poll, 400);
}

function handle(ev) {
  switch (ev.type) {
    case 'chat':   addMsg('', ev.data); break;
    case 'dm':     addMsg(ev.data.from === me ? ev.data.to : ev.data.from, ev.data); break;
    case 'users':  renderUsers(ev.data.users ?? ev.data); break; // AUTH_OK trả mảng, server trả AppMsg
    case 'error':  addSys(ev.data.text, 'err'); break;
    case 'status': addSys(`Trạng thái: ${ev.data}`); break;
    case 'file_start':
    case 'file_progress':
    case 'file_done':  renderFile(ev.data, ev.type); break;
    case 'file_error': addSys(ev.data, 'err'); break;
  }
}

// ---------- Trạng thái mạng (poll /api/status mỗi 1 s) ----------
async function pollStatus() {
  try {
    const st = await (await fetch('/api/status')).json();
    if (!me) { me = st.user; $('me').textContent = me; renderUsers(st.online); }
    const icon = { connected: '🟢', reconnecting: '🟡', disconnected: '🔴' }[st.state] ?? '⚪';
    setState(st.state, `${icon} ${st.state}`);
    $('net').textContent =
      `RTT ${st.rtt_ms.toFixed(1)} ms · cwnd ${(st.cwnd / 1024).toFixed(0)} KB · in-flight ${st.in_flight} B · ` +
      `loss ${st.loss_pct.toFixed(1)}% (${st.lost_pkts}/${st.sent_pkts})`;
  } catch (e) { /* poll() đã báo */ }
  setTimeout(pollStatus, 1000);
}

function setState(cls, text) {
  const el = $('state');
  el.className = `state ${cls}`;
  el.textContent = text;
}

// ---------- Users + tab ----------
function renderUsers(users) {
  const ul = $('users');
  ul.innerHTML = '';
  for (const u of [...users].sort()) {
    const li = document.createElement('li');
    li.innerHTML = `<span class="dot">●</span>${esc(u)}`;
    if (u === me) li.classList.add('me');
    else li.onclick = () => openTab(u);
    ul.appendChild(li);
  }
}

function openTab(name) {
  currentTab = name;
  unread.delete(name);
  if (!history[name]) history[name] = [];
  renderTabs();
  renderMessages();
  $('text').focus();
}

function renderTabs() {
  const nav = $('tabs');
  nav.innerHTML = '';
  for (const name of Object.keys(history)) {
    const b = document.createElement('button');
    b.className = 'tab' + (name === currentTab ? ' active' : '') + (unread.has(name) ? ' unread' : '');
    b.textContent = name === '' ? 'Chung' : name;
    b.onclick = () => openTab(name);
    nav.appendChild(b);
  }
}

// ---------- Tin nhắn ----------
function addMsg(tab, m) {
  if (!history[tab]) history[tab] = [];
  history[tab].push(m);
  if (tab !== currentTab) unread.add(tab);
  renderTabs();
  if (tab === currentTab) appendMsgEl(m);
}

function addSys(text, cls = 'sys') {
  const m = { sys: true, text, cls };
  history[currentTab].push(m);
  appendMsgEl(m);
}

function renderMessages() {
  const box = $('messages');
  box.innerHTML = '';
  for (const m of history[currentTab]) appendMsgEl(m);
}

function appendMsgEl(m) {
  const box = $('messages');
  const div = document.createElement('div');
  if (m.sys) {
    div.className = `msg ${m.cls}`;
    div.textContent = m.text;
  } else {
    div.className = 'msg' + (m.from === me ? ' me' : '');
    const t = m.ts ? new Date(m.ts).toLocaleTimeString() : '';
    div.innerHTML = `<div class="from">${esc(m.from)}<span class="time">${t}</span></div>${esc(m.text)}`;
  }
  box.appendChild(div);
  box.scrollTop = box.scrollHeight;
}

// ---------- Gửi ----------
$('sendform').onsubmit = async (e) => {
  e.preventDefault();
  const text = $('text').value.trim();
  if (!text) return;
  $('text').value = '';
  // Tin của mình không được server dội lại → tự hiển thị
  addMsg(currentTab, { from: me, to: currentTab, text, ts: Date.now() });
  const res = await fetch('/api/send', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ to: currentTab, text }),
  });
  if (!res.ok) addSys((await res.json()).error, 'err');
};

$('file').onchange = async () => {
  const file = $('file').files[0];
  if (!file) return;
  $('file').value = '';
  const q = `to=${encodeURIComponent(currentTab)}&name=${encodeURIComponent(file.name)}`;
  const res = await fetch(`/api/upload?${q}`, { method: 'POST', body: file }); // gửi thô, không multipart
  if (!res.ok) addSys((await res.json()).error, 'err');
};

// ---------- File progress ----------
function renderFile(p, type) {
  let el = document.getElementById(`f-${p.id}`);
  if (!el) {
    el = document.createElement('div');
    el.id = `f-${p.id}`;
    el.className = 'file';
    el.innerHTML = `<div class="name"></div><progress></progress><div class="meta"><span class="dir"></span><span class="pct"></span></div>`;
    $('filelist').prepend(el);
  }
  el.querySelector('.name').textContent = p.name;
  el.querySelector('.dir').textContent = p.dir === 'out' ? '⬆ gửi' : `⬇ từ ${p.from}`;
  const pr = el.querySelector('progress');
  pr.max = p.size || 1;
  pr.value = p.received;
  const pct = p.size ? (100 * p.received / p.size).toFixed(0) : 0;
  el.querySelector('.pct').textContent = `${fmtBytes(p.received)} / ${fmtBytes(p.size)} (${pct}%)`;
  if (type === 'file_done') {
    el.classList.add('done');
    el.querySelector('.pct').textContent = `✔ ${fmtBytes(p.size)}` + (p.path ? ` → ${p.path}` : '');
  }
}

function fmtBytes(n) {
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
  return `${(n / 1024 / 1024).toFixed(1)} MB`;
}

function esc(s) {
  return String(s ?? '').replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
}

renderTabs();
pollStatus();
poll();
