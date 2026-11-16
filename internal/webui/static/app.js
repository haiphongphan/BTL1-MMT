// app.js — JavaScript thuần, không thư viện. Chỉ nói chuyện với process client local qua fetch().
// Hai màn hình: #auth (đăng nhập / đăng ký) khi /api/status báo logged_in = false, và #app sau khi đăng nhập.
'use strict';

const $ = (id) => document.getElementById(id);

let loggedIn = null;  // null = chưa biết, true/false theo /api/status
let session = 0;      // tăng mỗi lần vào #app; kết quả poll của phiên cũ bị bỏ qua
let me = '';          // username của mình (lấy từ /api/status)
let since = 0;        // seq của event cuối đã thấy
let loginSince = -1;  // st.since của phiên đang hiển thị
let currentTab = '';  // '' = chat chung, ngược lại = tên người đang DM
let addrs = {};       // user → "ip:port" server báo kèm danh sách user
let history = { '': [] };   // tab → danh sách tin
const unread = new Set();

// ---------- Poll event (mỗi 400 ms, chỉ khi đã đăng nhập) ----------
async function poll() {
  const my = session;
  if (loggedIn) {
    try {
      const events = await (await fetch(`/api/events?since=${since}`)).json();
      if (my === session) {
        for (const ev of events) {
          since = ev.seq;
          handle(ev);
        }
      }
    } catch (e) {
      setState('disconnected', '🔴 Không kết nối được client local');
    }
  }
  setTimeout(poll, 400);
}

function handle(ev) {
  switch (ev.type) {
    case 'chat':   addMsg('', ev.data); break;
    case 'dm':     addMsg(ev.data.from === me ? ev.data.to : ev.data.from, ev.data); break;
    case 'users':  // AUTH_OK trả mảng tên, server trả AppMsg {users, addrs}
      if (ev.data.addrs) addrs = ev.data.addrs;
      renderUsers(ev.data.users ?? ev.data); break;
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
    applyStatus(await (await fetch('/api/status')).json());
  } catch (e) { /* poll() đã báo */ }
  setTimeout(pollStatus, 1000);
}

// applyStatus chuyển màn hình khi trạng thái đăng nhập đổi (kể cả đăng xuất từ CLI hoặc tab khác),
// rồi cập nhật thanh trạng thái.
function applyStatus(st) {
  if (!st.logged_in) {
    if (loggedIn !== false) showAuth(st);
    return;
  }
  if (loggedIn !== true || st.since !== loginSince) enterApp(st); // phiên mới (kể cả đăng nhập lại từ CLI)
  $('server').textContent = `server ${st.server}`;
  const icon = { connected: '🟢', reconnecting: '🟡', disconnected: '🔴' }[st.state] ?? '⚪';
  setState(st.state, `${icon} ${st.state}`);
  $('net').textContent =
    `RTT ${st.rtt_ms.toFixed(1)} ms · cwnd ${(st.cwnd / 1024).toFixed(0)} KB · in-flight ${st.in_flight} B · ` +
    `loss ${st.loss_pct.toFixed(1)}% (${st.lost_pkts}/${st.sent_pkts})`;
}

// ---------- Đăng nhập / đăng ký / đăng xuất ----------
let authMode = 'login';

function showAuth(st) {
  loggedIn = false;
  session++;
  $('app').hidden = true;
  $('auth').hidden = false;
  if (!$('a-server').value) $('a-server').value = st.server || '127.0.0.1:9000';
  if (!$('a-user').value && st.user) $('a-user').value = st.user;
  $(($('a-user').value ? 'a-pass' : 'a-user')).focus();
}

// enterApp xóa giao diện của phiên trước và đọc event từ st.since (lúc bắt đầu phiên đăng nhập này),
// nên tải lại trang vẫn thấy lại tin của phiên hiện tại nhưng không thấy tin của tài khoản trước.
function enterApp(st) {
  loggedIn = true;
  session++;
  me = st.user;
  since = loginSince = st.since;
  history = { '': [] };
  unread.clear();
  currentTab = '';
  $('me').textContent = me;
  $('messages').innerHTML = '';
  $('filelist').innerHTML = '';
  addrs = st.addrs || {};
  renderUsers(st.online || []);
  renderTabs();
  $('auth').hidden = true;
  $('app').hidden = false;
  $('text').focus();
}

function setAuthMode(mode) {
  authMode = mode;
  for (const b of document.querySelectorAll('.authtabs button')) b.classList.toggle('active', b.dataset.mode === mode);
  const reg = mode === 'register';
  $('a-pass2-row').hidden = !reg;
  $('a-pass2').required = reg;
  $('a-pass').autocomplete = reg ? 'new-password' : 'current-password';
  $('a-submit').textContent = reg ? 'Đăng ký' : 'Đăng nhập';
  $('a-hint').innerHTML = reg
    ? 'Tên 3–20 ký tự: chữ không dấu, số, dấu _. Mật khẩu 6–64 ký tự.'
    : 'Server: <code>ip:port</code> của máy chạy server, hoặc <code>auto</code> để tự tìm trong LAN.';
  $('a-err').textContent = '';
}

for (const b of document.querySelectorAll('.authtabs button')) b.onclick = () => setAuthMode(b.dataset.mode);

$('authform').onsubmit = async (e) => {
  e.preventDefault();
  const server = $('a-server').value.trim(), user = $('a-user').value.trim(), pass = $('a-pass').value;
  const err = $('a-err');
  // Kiểm tra sơ bộ cho người dùng thấy lỗi ngay; server vẫn kiểm tra lại (luật nằm ở server).
  if (authMode === 'register') {
    if (!/^[A-Za-z0-9_]{3,20}$/.test(user)) { err.textContent = 'Tên đăng nhập 3–20 ký tự: chữ không dấu, số, dấu _'; return; }
    if (pass.length < 6 || pass.length > 64) { err.textContent = 'Mật khẩu phải dài 6–64 ký tự'; return; }
    if (pass !== $('a-pass2').value) { err.textContent = 'Hai mật khẩu không khớp'; return; }
  }
  const btn = $('a-submit'), label = btn.textContent;
  btn.disabled = true;
  btn.textContent = server === 'auto' ? 'Đang tìm server…' : 'Đang kết nối…';
  err.textContent = '';
  try {
    const res = await fetch(`/api/${authMode}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ server, user, pass }),
    });
    if (!res.ok) { err.textContent = (await res.json()).error; return; }
    $('a-pass').value = '';
    $('a-pass2').value = '';
    applyStatus(await (await fetch('/api/status')).json());
  } catch (ex) {
    err.textContent = 'Không gọi được client local: ' + ex.message;
  } finally {
    btn.disabled = false;
    btn.textContent = label;
  }
};

$('logout').onclick = async () => {
  await fetch('/api/logout', { method: 'POST' });
  setAuthMode('login');
  applyStatus(await (await fetch('/api/status')).json());
};

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
    li.innerHTML = `<span class="dot">●</span>${esc(u)}<span class="addr">${esc(addrs[u] ?? '')}</span>`;
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
    if (p.dir === 'in' && p.path && !el.querySelector('a.dl')) {
      // file đã lưu trong thư mục downloads của client; nút này tải bản đó về máy qua trình duyệt
      const a = document.createElement('a');
      a.className = 'dl';
      a.textContent = '⬇ Tải xuống';
      a.href = `/api/download?name=${encodeURIComponent(p.path.split(/[\\/]/).pop())}`;
      a.setAttribute('download', '');
      el.appendChild(a);
    }
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

pollStatus();
poll();
