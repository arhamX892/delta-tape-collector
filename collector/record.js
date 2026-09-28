/*
 * 24/7 tape collector for Delta India BTCUSD — runs as a GitHub Actions job.
 * Connects to the public WS, records every trade + 1m candle as NDJSON,
 * exits cleanly before the 6h Actions limit, and the workflow commits the file.
 * Run locally too: node collector/record.js   (env: RECORD_MINUTES, default 350)
 */
'use strict';
const fs = require('fs');
const path = require('path');

const WS_URL = 'wss://public-socket.india.delta.exchange';
const SYMBOL = 'BTCUSD';
const MINUTES = Number(process.env.RECORD_MINUTES || 350);
const DATA_DIR = path.join(__dirname, '..', 'data');
fs.mkdirSync(DATA_DIR, { recursive: true });

let ws = null;
let file = null;
let count = 0;
const endAt = Date.now() + MINUTES * 60 * 1000;

function outFile() {
  const d = new Date().toISOString().slice(0, 10);
  return path.join(DATA_DIR, `tape-${d}.ndjson`);
}
function write(line) {
  try {
    if (!file) file = fs.createWriteStream(outFile(), { flags: 'a' });
    file.write(line + '\n');
    count++;
    if (count % 5000 === 0) console.log('records:', count);
  } catch (e) { console.error('write error:', e.message); }
}

function connect() {
  ws = new WebSocket(WS_URL);
  const on = (ev, fn) => (typeof ws.on === 'function' ? ws.on(ev, fn) : ws.addEventListener(ev, fn));
  on('open', () => {
    console.log('connected', WS_URL);
    ws.send(JSON.stringify({ type: 'subscribe', payload: { channels: [
      { name: 'trades', symbols: [SYMBOL] },
      { name: 'candlestick_1m', symbols: [SYMBOL] },
    ] } }));
  });
  on('message', (ev) => {
    const buf = ev.data != null ? (typeof ev.data === 'string' ? ev.data : ev.data.toString()) : '';
    let m; try { m = JSON.parse(buf); } catch { return; }
    if (m.type === 'trades') {
      write(JSON.stringify({ k: 't', t: m.t, p: m.p, s: m.s, r: m.r }));
    } else if (m.type === 'candlestick_1m') {
      write(JSON.stringify({ k: 'c', cst: m.cst, o: m.o, h: m.h, l: m.l, c: m.c, v: m.v }));
    }
  });
  on('error', (e) => console.error('ws error:', e.message || e.error || 'unknown'));
  on('close', () => {
    if (Date.now() < endAt) {
      console.log('disconnected early — reconnecting in 10s');
      setTimeout(connect, 10000);
    }
  });
}

connect();

const flushTimer = setInterval(() => {
  if (file) console.log('alive, records:', count);
}, 60000);

setTimeout(() => {
  console.log('window complete, records:', count);
  clearInterval(flushTimer);
  try { if (file) file.end(); if (ws) ws.close(); } catch (e) { /* */ }
  setTimeout(() => process.exit(0), 2000);
}, MINUTES * 60 * 1000);

process.on('SIGTERM', () => { try { if (file) file.end(); } catch (e) {} process.exit(0); });
