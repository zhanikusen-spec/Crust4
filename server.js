// Лес выживание — сервер: раздаёт игру и ретранслирует онлайн-сообщения по WebSocket.
// Без зависимостей: достаточно Node.js 16+.  Запуск: node server.js  (или npm start)
'use strict';
const http = require('http');
const https = require('https');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');

const PORT = process.env.PORT || 3000;
const ROOT = __dirname;
const FILES = { '/': 'index.html', '/index.html': 'index.html', '/three.min.js': 'three.min.js' };
const TYPES = { '.html': 'text/html; charset=utf-8', '.js': 'application/javascript; charset=utf-8' };
const THREE_URL = 'https://cdnjs.cloudflare.com/ajax/libs/three.js/r128/three.min.js';

// ---------- HTTP ----------
const server = http.createServer((req, res) => {
  const url = req.url.split('?')[0];
  const name = FILES[url];
  if (!name) { res.writeHead(404); return res.end('Not found'); }
  fs.readFile(path.join(ROOT, name), (err, data) => {
    if (err) { res.writeHead(404); return res.end('Not found'); }
    res.writeHead(200, { 'Content-Type': TYPES[path.extname(name)] || 'application/octet-stream' });
    res.end(data);
  });
});

// Один раз скачиваем three.js рядом с игрой, чтобы она работала и без интернета у игроков.
const threeFile = path.join(ROOT, 'three.min.js');
if (!fs.existsSync(threeFile)) {
  https.get(THREE_URL, r => {
    if (r.statusCode !== 200) return r.resume();
    const chunks = [];
    r.on('data', c => chunks.push(c));
    r.on('end', () => { try { fs.writeFileSync(threeFile, Buffer.concat(chunks)); console.log('three.min.js сохранён локально'); } catch (e) {} });
  }).on('error', () => {});
}

// ---------- Минимальный WebSocket (RFC 6455) ----------
const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
const clients = new Map(); // id -> client
let nextId = 1;

function frame(str) {
  const p = Buffer.from(str);
  const n = p.length;
  let h;
  if (n < 126) h = Buffer.from([0x81, n]);
  else if (n < 65536) { h = Buffer.alloc(4); h[0] = 0x81; h[1] = 126; h.writeUInt16BE(n, 2); }
  else { h = Buffer.alloc(10); h[0] = 0x81; h[1] = 127; h.writeBigUInt64BE(BigInt(n), 2); }
  return Buffer.concat([h, p]);
}
function send(c, obj) { if (c.alive) { try { c.sock.write(frame(JSON.stringify(obj))); } catch (e) {} } }

function lobbyBroadcast() {
  const peers = [...clients.values()].map(c => ({ peer: c.id, presence: c.lp }));
  clients.forEach(c => send(c, { t: 'lobby', peers }));
}
function roomPeers(room) {
  return [...clients.values()].filter(c => c.room === room).map(c => ({ peer: c.id, presence: c.rp }));
}
function roomFull(room) {
  const peers = roomPeers(room);
  clients.forEach(c => { if (c.room === room) send(c, { t: 'rfull', peers }); });
}
function leaveRoom(c) {
  const room = c.room;
  if (!room) return;
  c.room = null; c.rp = {};
  clients.forEach(o => { if (o.room === room) send(o, { t: 'pl', peer: c.id }); });
}

function handle(c, m) {
  switch (m.t) {
    case 'lp': c.lp = m.p || {}; lobbyBroadcast(); break;
    case 'join': {
      const room = String(m.room || '').slice(0, 64);
      if (c.room === room) break;
      leaveRoom(c);
      c.room = room; c.rp = {};
      roomFull(room);
      break;
    }
    case 'rp':
      if (!c.room) break;
      c.rp = m.p || {};
      clients.forEach(o => { if (o !== c && o.room === c.room) send(o, { t: 'pu', peer: c.id, presence: c.rp }); });
      break;
    case 'em':
      if (!c.room) break;
      clients.forEach(o => { if (o !== c && o.room === c.room) send(o, { t: 'msg', type: m.type, data: m.data, from: c.id }); });
      break;
  }
}

function drop(c) {
  if (!c.alive) return;
  c.alive = false;
  clients.delete(c.id);
  leaveRoom(c);
  lobbyBroadcast();
  try { c.sock.destroy(); } catch (e) {}
}

server.on('upgrade', (req, sock) => {
  const key = req.headers['sec-websocket-key'];
  if (!key) return sock.destroy();
  const accept = crypto.createHash('sha1').update(key + GUID).digest('base64');
  sock.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + accept + '\r\n\r\n');
  sock.setNoDelay(true);

  const c = { id: 'p' + (nextId++), sock, alive: true, room: null, lp: {}, rp: {} };
  clients.set(c.id, c);
  let buf = Buffer.alloc(0), frags = [];

  sock.on('data', d => {
    buf = Buffer.concat([buf, d]);
    while (buf.length >= 2) {
      const op = buf[0] & 0x0f, fin = !!(buf[0] & 0x80), masked = !!(buf[1] & 0x80);
      let len = buf[1] & 0x7f, off = 2;
      if (len === 126) { if (buf.length < 4) return; len = buf.readUInt16BE(2); off = 4; }
      else if (len === 127) { if (buf.length < 10) return; len = Number(buf.readBigUInt64BE(2)); off = 10; }
      if (len > 1 << 20) return drop(c);
      const need = off + (masked ? 4 : 0) + len;
      if (buf.length < need) return;
      let payload = buf.slice(off + (masked ? 4 : 0), need);
      if (masked) {
        const mask = buf.slice(off, off + 4);
        payload = Buffer.from(payload);
        for (let i = 0; i < payload.length; i++) payload[i] ^= mask[i & 3];
      }
      buf = buf.slice(need);
      if (op === 0x8) return drop(c);
      if (op === 0x9) { try { sock.write(Buffer.concat([Buffer.from([0x8a, payload.length]), payload])); } catch (e) {} continue; }
      if (op === 0x1 || op === 0x0) {
        frags.push(payload);
        if (fin) {
          const text = Buffer.concat(frags).toString('utf8'); frags = [];
          try { handle(c, JSON.parse(text)); } catch (e) {}
        }
      }
    }
  });
  sock.on('close', () => drop(c));
  sock.on('error', () => drop(c));

  send(c, { t: 'hello', id: c.id });
  lobbyBroadcast();
});

server.listen(PORT, () => {
  console.log('🌲 Лес выживание запущен: http://localhost:' + PORT);
  console.log('Друзья в одной сети заходят по http://<ваш-IP>:' + PORT);
});
