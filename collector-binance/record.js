/*
 * 24/7 blackbox collector for Binance BTCUSDT perpetual — GitHub Actions job.
 *
 * ARCHITECTURE NOTE: Binance archives full trade + kline history forever
 * (data.binance.vision), so recording trades here would duplicate a free source.
 * What history does NOT have is the live order book, funding, and open interest —
 * that is what this collector exists to capture. Book-focused blackbox, schema B1.
 *
 * Record kinds (all timestamps MILLISECONDS, sizes in BTC):
 *   meta — first line of every file: schema, symbol, window start/duration, git sha
 *   o    — bookTicker best bid/ask 1 Hz {lt, b, B, a, A}
 *   x    — raw depth5 snapshots (full ladder top-5), 30 s throttle
 *   k    — markPrice 30 s {lt, T, mark, index, fr funding}
 *   c    — CLOSED 1m klines only {t ms, o,h,l,c, v, tb takerBuy}
 *   f    — REST poll 15 min {lt, oi, vol24, chg24}
 *   hb   — heartbeat 60 s (silence = dead socket, not quiet tape)
 *   rc   — reconnect marker
 *   rc1  — window-end REST kline cross-fill for the whole window (authoritative)
 * Run locally too: node collector-binance/record.js   (env: RECORD_MINUTES, default 350)
 */
'use strict';
const fs = require('fs');
const path = require('path');
const https = require('https');

const WS_URL = 'wss://fstream.binance.com/stream?streams=' + [
  'btcusdt@bookTicker',
  'btcusdt@markPrice@1s',
  'btcusdt@kline_1m',
  'btcusdt@depth5@100ms',
].join('/');
const REST_OI = 'https://fapi.binance.com/fapi/v1/openInterest?symbol=BTCUSDT';
const REST_TICKER = 'https://fapi.binance.com/fapi/v1/ticker/24hr?symbol=BTCUSDT';
const REST_KLINES = 'https://fapi.binance.com/fapi/v1/klines';
const SYMBOL = 'BTCUSDT';
const MINUTES = Number(process.env.RECORD_MINUTES || 350);
const DATA_DIR = path.join(__dirname, '..', 'data-binance');
const SCHEMA = 'B1';
const GIT_SHA = process.env.GIT_SHA || 'local';
const REQ_HEADERS = { 'User-Agent': 'tape-collector-binance/1', 'Accept': 'application/json' };
fs.mkdirSync(DATA_DIR, { recursive: true });

let ws = null;
let file = null;
let count = 0;
let lastBookWrite = 0;
let lastRawWrite = 0;
let lastMarkWrite = 0;
let lastKlineT = 0;
const startedAtIso = new Date().toISOString();
const startMs = Date.parse(startedAtIso);
const endAt = Date.now() + MINUTES * 60 * 1000;
// one file per recording WINDOW: two jobs must never write the same path
function outFile() {
  const d = startedAtIso.slice(0, 10);
  const hm = startedAtIso.slice(11, 16).replace(':', '');
  return path.join(DATA_DIR, `btape-${d}-${hm}.ndjson`);
}
function write(line) {
  try {
    if (!file) file = fs.createWriteStream(outFile(), { flags: 'a' });
    file.write(line + '\n');
    count++;
    if (count % 5000 === 0) console.log('records:', count);
  } catch (e) { console.error('write error:', e.message); }
}
function getJson(url) {
  return new Promise((resolve, reject) => {
    const req = https.get(url, { timeout: 15000, headers: REQ_HEADERS }, (res) => {
      let body = '';
      res.on('data', (ch) => { body += ch; });
      res.on('end', () => {
        try { resolve(JSON.parse(body)); } catch (e) { reject(new Error('non-JSON response')); }
      });
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('timeout')); });
  });
}

function connect() {
  ws = new WebSocket(WS_URL);
  const on = (ev, fn) => (typeof ws.on === 'function' ? ws.on(ev, fn) : ws.addEventListener(ev, fn));
  on('open', () => {
    console.log('connected', WS_URL.slice(0, 60) + '…');
    write(JSON.stringify({ k: 'meta', v: SCHEMA, sym: SYMBOL, start: startedAtIso, durMin: MINUTES, sha: GIT_SHA }));
  });
  on('message', (ev) => {
    const buf = ev.data != null ? (typeof ev.data === 'string' ? ev.data : ev.data.toString()) : '';
    let msg; try { msg = JSON.parse(buf); } catch { return; }
    const stream = msg.stream || '';
    const d = msg.data;
    if (!d) return;
    if (stream.endsWith('@bookTicker')) {
      // hottest stream — throttle to 1 Hz; spread at 1s granularity is plenty (0.01 bps ticks)
      const now = Date.now();
      if (now - lastBookWrite < 1000) return;
      lastBookWrite = now;
      write(JSON.stringify({ k: 'o', lt: now, b: d.b, B: d.B, a: d.a, A: d.A }));
    } else if (stream.includes('markPrice')) {
      const now = Date.now();
      if (now - lastMarkWrite < 30000) return;
      lastMarkWrite = now;
      write(JSON.stringify({ k: 'k', lt: now, T: d.T, mark: d.p, index: d.i, fr: d.r }));
    } else if (stream.endsWith('@kline_1m')) {
      const kl = d.k;
      // closed klines only — the archive covers intraminute detail, we want the final bar
      if (!kl.x || kl.t === lastKlineT) return;
      lastKlineT = kl.t;
      write(JSON.stringify({ k: 'c', t: kl.t, o: kl.o, h: kl.h, l: kl.l, c: kl.c, v: kl.v, tb: kl.V }));
    } else if (stream.includes('depth5')) {
      // raw passthrough of the ladder top-5, 30 s throttle — parse offline anytime
      const now = Date.now();
      if (now - lastRawWrite < 30000) return;
      lastRawWrite = now;
      write(JSON.stringify({ k: 'x', lt: now, ch: 'depth5', bids: d.bids, asks: d.asks }));
    }
  });
  on('error', (e) => console.error('ws error:', e.message || e.error || 'unknown'));
  on('close', () => {
    if (Date.now() < endAt) {
      write(JSON.stringify({ k: 'rc', lt: new Date().toISOString() }));
      console.log('disconnected early — reconnecting in 10s');
      setTimeout(connect, 10000);
    }
  });
}

// REST poll: open interest + 24h volume/change (15 min = 96 tiny calls/day)
async function pollRest() {
  try {
    const [oi, tk] = await Promise.all([getJson(REST_OI), getJson(REST_TICKER)]);
    write(JSON.stringify({
      k: 'f', lt: new Date().toISOString(),
      oi: oi.openInterest, vol24: tk.volume, chg24: tk.priceChangePercent,
    }));
  } catch (e) { console.error('rest poll:', e.message); }
}

const REST_PREMIUM = 'https://fapi.binance.com/fapi/v1/premiumIndex?symbol=BTCUSDT';

// mark/funding fallback: premiumIndex REST every 30s — guarantees the funding
// record even where the markPrice WS stream stays silent (observed locally)
async function pollMark() {
  const now = Date.now();
  if (now - lastMarkWrite < 30000) return;
  try {
    const d = await getJson(REST_PREMIUM);
    lastMarkWrite = now;
    write(JSON.stringify({ k: 'k', lt: now, T: d.time, mark: d.markPrice, index: d.indexPrice, fr: d.lastFundingRate }));
  } catch (e) { /* transient */ }
}

// closed-kline fallback: last 3 klines every 60s — same dedupe guard as the stream
async function pollKline() {
  try {
    const u = new URL(REST_KLINES);
    u.searchParams.set('symbol', SYMBOL);
    u.searchParams.set('interval', '1m');
    u.searchParams.set('limit', '3');
    const rows = await getJson(u.toString());
    const nowMs = Date.now();
    for (const r of rows) {
      const t = Number(r[0]);
      const closeTime = Number(r[6]);
      if (closeTime >= nowMs || t === lastKlineT) continue; // forming or already recorded
      lastKlineT = t;
      write(JSON.stringify({ k: 'c', t, o: r[1], h: r[2], l: r[3], c: r[4], v: r[5], tb: r[9] }));
    }
  } catch (e) { /* transient */ }
}

// window-end REST kline cross-fill: authoritative 1m bars for the whole window
async function crossfillCandles() {
  const u = new URL(REST_KLINES);
  u.searchParams.set('symbol', SYMBOL);
  u.searchParams.set('interval', '1m');
  u.searchParams.set('startTime', String(startMs));
  u.searchParams.set('endTime', String(Date.now()));
  u.searchParams.set('limit', '1500');
  try {
    const rows = await getJson(u.toString());
    for (const r of rows) write(JSON.stringify({ k: 'rc1', t: r[0], o: r[1], h: r[2], l: r[3], c: r[4], v: r[5], tb: r[9] }));
    console.log('crossfill klines:', rows.length);
  } catch (e) { console.error('crossfill:', e.message); }
}

pollRest();
const restTimer = setInterval(pollRest, 15 * 60 * 1000);
pollMark();
const markTimer = setInterval(pollMark, 30000);
pollKline();
const klineTimer = setInterval(pollKline, 60000);
connect();

const hbTimer = setInterval(() => {
  write(JSON.stringify({ k: 'hb', lt: new Date().toISOString() }));
}, 60000);

const flushTimer = setInterval(() => {
  if (file) console.log('alive, records:', count);
}, 60000);

async function finish() {
  console.log('window complete, records:', count);
  for (const t of [hbTimer, flushTimer, restTimer, markTimer, klineTimer]) clearInterval(t);
  await crossfillCandles();
  try { if (file) file.end(); if (ws) ws.close(); } catch (e) { /* */ }
  setTimeout(() => process.exit(0), 2000);
}
setTimeout(finish, MINUTES * 60 * 1000);
process.on('SIGTERM', () => { try { if (file) file.end(); } catch (e) {} process.exit(0); });
