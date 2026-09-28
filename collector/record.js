/*
 * 24/7 tape collector for Delta India BTCUSD — runs as a GitHub Actions job.
 * Connects to the public WS, records every trade + 1m candle + 1Hz book snapshot
 * + 30s ticker as NDJSON, exits cleanly before the 6h Actions limit, and the
 * workflow commits the file.
 * Run locally too: node collector/record.js   (env: RECORD_MINUTES, default 350)
 *
 * v2 (audit 2026-09-28): spread/book, ticker and funding are HARD arming inputs
 * of the advisor engine — they can never be backfilled, so they must be recorded
 * from day one or 6 months of tape trains a filter blind to its own cost gate.
 * Record kinds: meta | t(trade) | c(candle) | o(book L1) | k(ticker) | f(funding) | hb | rc
 */
'use strict';
const fs = require('fs');
const path = require('path');
const https = require('https');

const WS_URL = 'wss://public-socket.india.delta.exchange';
const REST_TICKER = 'https://api.india.delta.exchange/v2/tickers/BTCUSD';
const SYMBOL = 'BTCUSD';
const MINUTES = Number(process.env.RECORD_MINUTES || 350);
const DATA_DIR = path.join(__dirname, '..', 'data');
const SCHEMA_VERSION = 2;
fs.mkdirSync(DATA_DIR, { recursive: true });

let ws = null;
let file = null;
let count = 0;
const startedAtIso = new Date().toISOString();
const endAt = Date.now() + MINUTES * 60 * 1000;
// one file per recording WINDOW (not per day): two jobs must never write the
// same path, or the later git push collides with the earlier one and loses a
// whole window. Merge/dedupe across files at training time.
function outFile() {
  const d = startedAtIso.slice(0, 10);
  const hm = startedAtIso.slice(11, 16).replace(':', '');
  return path.join(DATA_DIR, `tape-${d}-${hm}.ndjson`);
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
    // meta must be the FIRST line of every window: the merge script derives
    // interval ownership (dedupe) and window duration from it
    write(JSON.stringify({ k: 'meta', v: SCHEMA_VERSION, sym: SYMBOL, start: startedAtIso, durMin: MINUTES }));
    ws.send(JSON.stringify({ type: 'subscribe', payload: { channels: [
      { name: 'trades', symbols: [SYMBOL] },
      { name: 'candlestick_1m', symbols: [SYMBOL] },
      { name: 'ob_l1', symbols: [SYMBOL] },
      { name: 'ticker', symbols: [SYMBOL] },
    ] } }));
  });
  on('message', (ev) => {
    const buf = ev.data != null ? (typeof ev.data === 'string' ? ev.data : ev.data.toString()) : '';
    let m; try { m = JSON.parse(buf); } catch { return; }
    if (m.type === 'trades') {
      // t = trade time µs, ts = publish µs (latency + ordering evidence);
      // r: 't' = buyer was taker (aggressive BUY), 'm' = buyer was maker (SELL)
      write(JSON.stringify({ k: 't', t: m.t, p: m.p, s: m.s, r: m.r, ts: m.ts }));
    } else if (m.type === 'candlestick_1m') {
      // `cst` is live-observed but absent from the docs sample — mirror the
      // engine's fallback so the candle stream survives a schema change
      const cst = m.cst != null ? m.cst : (m.ts != null ? Math.floor(m.ts / 60e6) * 60e6 : null);
      write(JSON.stringify({ k: 'c', cst, o: m.o, h: m.h, l: m.l, c: m.c, v: m.v }));
    } else if (m.type === 'ob_l1') {
      // 1 Hz throttle: spread moves in 0.6 bps ticks vs a 2 bps gate, and the
      // engine evaluates ~1/s — full 100ms rate would 60x the file for nothing
      const now = Date.now();
      if (now - lastBookWrite < 1000) return;
      lastBookWrite = now;
      write(JSON.stringify({ k: 'o', ts: m.ts, ap: m.ap, bp: m.bp, as: m.as, bs: m.bs }));
    } else if (m.type === 'ticker') {
      // ticker: { d:[{ s, m (mark), q:[ask, askSz, bid, bidSz, impact] }], sp (spot), ts }
      const now = Date.now();
      if (now - lastTickerWrite < 30000) return;
      lastTickerWrite = now;
      const d = Array.isArray(m.d) ? m.d.find((x) => x.s === SYMBOL || x.i === 27) : null;
      if (!d) return;
      const q = Array.isArray(d.q) ? d.q : [];
      write(JSON.stringify({ k: 'k', ts: m.ts, sp: m.sp, m: d.m, ask: q[0], bid: q[2] }));
    }
  });
  on('error', (e) => console.error('ws error:', e.message || e.error || 'unknown'));
  on('close', () => {
    if (Date.now() < endAt) {
      write(JSON.stringify({ k: 'rc', lt: new Date().toISOString() })); // reconnect marker
      console.log('disconnected early — reconnecting in 10s');
      setTimeout(connect, 10000);
    }
  });
}

let lastBookWrite = 0;
let lastTickerWrite = 0;

// funding rate: the public ticker WS channel carries none — poll the REST
// ticker like the extension does (8h funding; 15min poll = 96 tiny calls/day)
function pollFunding() {
  const req = https.get(REST_TICKER, {
    timeout: 10000,
    headers: { 'User-Agent': 'tape-collector/2', 'Accept': 'application/json' },
  }, (res) => {
    let body = '';
    res.on('data', (ch) => { body += ch; });
    res.on('end', () => {
      try {
        const r = JSON.parse(body).result;
        if (r && r.funding_rate != null) {
          write(JSON.stringify({ k: 'f', lt: new Date().toISOString(), fr: r.funding_rate }));
        }
      } catch (e) { console.error('funding parse:', e.message); }
    });
  });
  req.on('error', (e) => console.error('funding poll:', e.message));
  req.on('timeout', () => req.destroy());
}
pollFunding();
const fundingTimer = setInterval(pollFunding, 15 * 60 * 1000);

connect();

// liveness marker: silence in the tape must mean "quiet tape", never "dead
// socket" — the merge script measures outages from hb gaps
const hbTimer = setInterval(() => {
  write(JSON.stringify({ k: 'hb', lt: new Date().toISOString() }));
}, 60000);

const flushTimer = setInterval(() => {
  if (file) console.log('alive, records:', count);
}, 60000);

function finish() {
  console.log('window complete, records:', count);
  for (const t of [hbTimer, flushTimer, fundingTimer]) clearInterval(t);
  try { if (file) file.end(); if (ws) ws.close(); } catch (e) { /* */ }
  setTimeout(() => process.exit(0), 2000);
}
setTimeout(finish, MINUTES * 60 * 1000);
process.on('SIGTERM', () => { try { if (file) file.end(); } catch (e) {} process.exit(0); });
