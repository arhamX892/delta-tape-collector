/*
 * 24/7 tape collector for Delta India BTCUSD — runs as a GitHub Actions job.
 * BLACKBOX contract: anything a future question could ask about this market
 * must be on disk. Record kinds (schema v3):
 *   meta — first line of every file: schema, symbol, window start/duration, git sha
 *   t    — every trade {t µs, p, s, r, ts publish µs}
 *   c    — 1m candle frames {cst µs, o,h,l,c,v} (cumulative; reduce last-per-cst)
 *   o    — book L1 snapshot 1 Hz {ts, ap, bp, as, bs}
 *   k    — ticker 30 s {ts, sp spot, m mark, ask, bid}
 *   f    — REST ticker poll 15 min {lt, fr funding, oi, vol24, chg24}
 *   hb   — heartbeat 60 s (silence = dead socket, not quiet tape)
 *   rc   — reconnect marker
 *   x    — raw passthrough of experimental channels (funding_rate, ob_updates),
 *          throttled 30 s — schemas not fully documented, stored raw so they can
 *          be parsed offline later
 *   rc1  — window-end REST /history/candles cross-fill: authoritative 1m OHLCV
 *          for the whole window, immune to WS frame drops
 * Run locally too: node collector/record.js   (env: RECORD_MINUTES, default 350)
 */
'use strict';
const fs = require('fs');
const path = require('path');
const https = require('https');

const WS_URL = 'wss://public-socket.india.delta.exchange';
const REST_TICKER = 'https://api.india.delta.exchange/v2/tickers/BTCUSD';
const REST_CANDLES = 'https://api.india.delta.exchange/v2/history/candles';
const SYMBOL = 'BTCUSD';
const MINUTES = Number(process.env.RECORD_MINUTES || 350);
const DATA_DIR = path.join(__dirname, '..', 'data');
const SCHEMA_VERSION = 3;
const GIT_SHA = process.env.GIT_SHA || 'local';
const REQ_HEADERS = { 'User-Agent': 'tape-collector/3', 'Accept': 'application/json' };
fs.mkdirSync(DATA_DIR, { recursive: true });

let ws = null;
let file = null;
let count = 0;
let lastBookWrite = 0;
let lastTickerWrite = 0;
let lastRawWrite = 0;
const startedAtIso = new Date().toISOString();
const startSec = Math.floor(Date.parse(startedAtIso) / 1000);
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
    console.log('connected', WS_URL);
    // meta must be the FIRST line of every window: the merge script derives
    // interval ownership (dedupe) and window duration from it
    write(JSON.stringify({ k: 'meta', v: SCHEMA_VERSION, sym: SYMBOL, start: startedAtIso, durMin: MINUTES, sha: GIT_SHA }));
    // core channels first — schema-verified, the engine's own inputs
    ws.send(JSON.stringify({ type: 'subscribe', payload: { channels: [
      { name: 'trades', symbols: [SYMBOL] },
      { name: 'candlestick_1m', symbols: [SYMBOL] },
      { name: 'ob_l1', symbols: [SYMBOL] },
      { name: 'ticker', symbols: [SYMBOL] },
    ] } }));
    // experimental channels in a SEPARATE subscribe so a rejection can never
    // take down the core streams; frames stored raw (kind x)
    ws.send(JSON.stringify({ type: 'subscribe', payload: { channels: [
      { name: 'funding_rate', symbols: [SYMBOL] },
      { name: 'ob_updates', symbols: [SYMBOL] },
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
    } else if (m.type === 'funding_rate' || m.type === 'ob_updates') {
      // raw passthrough, 30 s throttle — parse offline whenever needed
      const now = Date.now();
      if (now - lastRawWrite < 30000) return;
      lastRawWrite = now;
      write(JSON.stringify({ k: 'x', lt: new Date().toISOString(), ch: m.type, raw: m }));
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

// REST ticker poll: funding rate + open interest + 24h volume/change — the
// public WS ticker carries none of these (8h funding; 15 min poll = 96/day)
async function pollRestTicker() {
  try {
    const r = (await getJson(REST_TICKER)).result;
    if (r) {
      write(JSON.stringify({
        k: 'f', lt: new Date().toISOString(),
        fr: r.funding_rate, oi: r.oi_contracts, vol24: r.volume, chg24: r.mark_change_24h,
      }));
    }
  } catch (e) { console.error('rest ticker poll:', e.message); }
}

// window-end REST candle cross-fill: authoritative 1m OHLCV for the whole
// window — heals any WS frame drops and lets the merge script validate the
// candle stream instead of trusting it
async function crossfillCandles() {
  const endSec = Math.floor(Date.now() / 1000);
  const url = `${REST_CANDLES}?resolution=1m&symbol=${SYMBOL}&start=${startSec}&end=${endSec}`;
  try {
    const rows = (await getJson(url)).result || [];
    for (const c of rows) write(JSON.stringify({ k: 'rc1', t: c.time, o: c.open, h: c.high, l: c.low, c: c.close, v: c.volume }));
    console.log('crossfill candles:', rows.length);
  } catch (e) { console.error('crossfill candles:', e.message); }
}

pollRestTicker();
const fundingTimer = setInterval(pollRestTicker, 15 * 60 * 1000);

connect();

// liveness marker: silence in the tape must mean "quiet tape", never "dead
// socket" — the merge script measures outages from hb gaps
const hbTimer = setInterval(() => {
  write(JSON.stringify({ k: 'hb', lt: new Date().toISOString() }));
}, 60000);

const flushTimer = setInterval(() => {
  if (file) console.log('alive, records:', count);
}, 60000);

async function finish() {
  console.log('window complete, records:', count);
  for (const t of [hbTimer, flushTimer, fundingTimer]) clearInterval(t);
  await crossfillCandles();
  try { if (file) file.end(); if (ws) ws.close(); } catch (e) { /* */ }
  setTimeout(() => process.exit(0), 2000);
}
setTimeout(finish, MINUTES * 60 * 1000);
process.on('SIGTERM', () => { try { if (file) file.end(); } catch (e) {} process.exit(0); });
