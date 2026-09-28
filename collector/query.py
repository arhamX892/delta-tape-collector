#!/usr/bin/env python3
"""
Black-box tape interrogator — answer questions from stored data only.
Stdlib only. Usage (UTC times):

  python query.py stats                      # what's on disk: kinds, range, gaps overview
  python query.py --at "2026-09-28 17:00:27" # full market snapshot at a moment
  python query.py --gaps                     # every outage / reconnect / dead stretch
  python query.py --tape 10                  # last N trades
  python query.py --hour 16                  # per-minute data coverage for a UTC hour

Units: WS timestamps are microseconds (t, ts, cst); REST candle times (rc1.t) are
SECONDS; sizes are contracts (1 = 0.001 BTC).
"""
import json, sys, glob, os
from datetime import datetime, timezone

DATA_DIR = os.environ.get("TAPE_DIR", os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data"))

def load(data_dir=DATA_DIR):
    kinds = {}
    files = 0
    for path in sorted(glob.glob(os.path.join(data_dir, "*.ndjson"))):
        files += 1
        with open(path, encoding="utf-8") as fh:
            for line in fh:
                try:
                    r = json.loads(line)
                except Exception:
                    continue
                kinds.setdefault(r.get("k"), []).append(r)
    for k in ("t", "o", "k", "rc1", "c"):
        kinds.get(k, []).sort(key=lambda r: r.get("t") or r.get("ts") or r.get("cst") or 0)
    return kinds, files

def iso_us(us):
    return datetime.fromtimestamp(us / 1e6, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

def iso_s(s):
    return datetime.fromtimestamp(s, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

def parse_at(text):
    text = text.strip()
    if text == "latest":
        return None
    try:
        return float(text)
    except ValueError:
        pass
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(text, fmt).replace(tzinfo=timezone.utc).timestamp()
        except ValueError:
            pass
    raise SystemExit(f"cannot parse time: {text!r} (use UTC 'YYYY-MM-DD HH:MM:SS', unix sec, or 'latest')")

def nearest(rows, us, key, max_before_us=None):
    """last row with row[key] <= us"""
    lo, best = 0, None
    for r in rows:
        v = r.get(key)
        if v is None or v > us:
            break
        best = r
    if best is None or (max_before_us and us - best.get(key) > max_before_us):
        return None
    return best

def fnum(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None

def snapshot(kinds, at_sec):
    us = at_sec * 1e6 if at_sec else None
    if us is None:
        tr = kinds.get("t", [])
        us = tr[-1]["t"] if tr else (kinds.get("o", [{}])[-1].get("ts") if kinds.get("o") else 0)
    print(f"== SNAPSHOT at {iso_us(us)} UTC ==")
    tr = nearest(kinds.get("t", []), us, "t", max_before_us=30e6)
    if tr:
        print(f"last trade : {tr['p']} size {tr['s']} ctr  buyer_role={'taker(BUY)' if tr['r']=='t' else 'maker(SELL)' if tr['r']=='m' else tr['r']}  ({(us-tr['t'])/1e6:.1f}s before)")
    bk = nearest(kinds.get("o", []), us, "ts", max_before_us=10e6)
    if bk:
        ap, bp = fnum(bk["ap"]), fnum(bk["bp"])
        if ap and bp:
            mid = (ap + bp) / 2
            print(f"book L1    : ask {ap} x {bk['as']} | bid {bp} x {bk['bs']}  spread {ap-bp:.1f} = {(ap-bp)/mid*1e4:.2f} bps  ({(us-bk['ts'])/1e6:.1f}s before)")
    tk = nearest(kinds.get("k", []), us, "ts", max_before_us=300e6)
    if tk:
        print(f"ticker     : spot {tk['sp']}  mark {tk['m']}  (basis {fnum(tk['m'])-fnum(tk['sp']):+.1f})")
    # f rows carry ISO lt, not µs — sort separately
    f_rows = sorted(kinds.get("f", []), key=lambda r: r.get("lt") or "")
    best_f = None
    for r in f_rows:
        t_s = datetime.fromisoformat(r["lt"].replace("Z", "+00:00")).timestamp()
        if t_s <= at_sec:
            best_f = (r, t_s)
    if best_f and at_sec - best_f[1] < 3600:
        r = best_f[0]
        print(f"funding/OI : fr {r.get('fr')}  OI {r.get('oi')} ctr  vol24 {r.get('vol24')}  chg24 {r.get('chg24')}%  (at {r['lt']})")
    min_us = int(us // 6e7) * 6e7  # minute floor in µs
    for r in reversed(kinds.get("c", [])):
        if r.get("cst") == min_us:
            print(f"1m candle  : o {r['o']} h {r['h']} l {r['l']} c {r['c']} v {r['v']} ctr")
            break
    else:
        for r in reversed(kinds.get("rc1", [])):
            if r.get("t") == at_sec // 60 * 60:
                print(f"1m candle(R): o {r['o']} h {r['h']} l {r['l']} c {r['c']} v {r['v']} ctr (REST)")
                break

def gaps(kinds, files):
    hb = sorted(kinds.get("hb", []), key=lambda r: r["lt"])
    print(f"files: {files}   reconnects(rc): {len(kinds.get('rc', []))}   heartbeats: {len(hb)}")
    prev = None
    holes = 0
    for r in hb:
        t = datetime.fromisoformat(r["lt"].replace("Z", "+00:00")).timestamp()
        if prev is not None and t - prev > 90:
            holes += 1
            print(f"GAP {prev_str} -> {r['lt'][:-5]}  ({t-prev:.0f}s)")
        prev, prev_str = t, r["lt"][:-5]
    if not holes:
        print("no heartbeat gaps > 90s — recorder healthy across all windows")
    for r in kinds.get("rc", []):
        print(f"reconnect at {r['lt']}")
    for r in kinds.get("meta", []):
        print(f"window {r['start'][:16]} dur {r.get('durMin')}min schema v{r.get('v')} sha {r.get('sha', '?')}")

def stats(kinds, files):
    print(f"files: {files}")
    labels = {"meta": "window metadata", "t": "trades", "c": "candle frames (WS, cumulative)",
              "o": "book L1 snapshots (1Hz)", "k": "ticker snapshots (30s)", "f": "funding/OI polls (15min)",
              "x": "raw experimental frames", "hb": "heartbeats (60s)", "rc": "reconnect markers",
              "rc1": "REST authoritative candles"}
    for k, rows in sorted(kinds.items()):
        span = ""
        if k == "t" and rows:
            span = f"  {iso_us(rows[0]['t'])} -> {iso_us(rows[-1]['t'])}"
        elif k == "o" and rows:
            span = f"  {iso_us(rows[0]['ts'])} -> {iso_us(rows[-1]['ts'])}"
        print(f"  {k:4} {len(rows):8}  {labels.get(k, '?')}{span}")
    t = kinds.get("t", [])
    o = kinds.get("o", [])
    if t:
        n_min = max(1, (t[-1]["t"] - t[0]["t"]) / 6e7)
        print(f"flow      : {len(t)/n_min:.1f} prints/min, {sum(fnum(r['s']) or 0 for r in t)/n_min/1000:.2f} BTC/min")
    if o:
        sp = [(fnum(r["ap"]) - fnum(r["bp"])) / ((fnum(r["ap"]) + fnum(r["bp"])) / 2) * 1e4
              for r in o if fnum(r["ap"]) and fnum(r["bp"])]
        if sp:
            sp.sort()
            print(f"spread    : median {sp[len(sp)//2]:.2f} bps, p95 {sp[int(len(sp)*0.95)]:.2f} bps, max {sp[-1]:.2f} bps")

def hour(kinds, h):
    seen = {}
    for r in kinds.get("t", []):
        sec = r["t"] / 1e6
        if datetime.fromtimestamp(sec, tz=timezone.utc).hour == h:
            seen[int(sec // 60)] = seen.get(int(sec // 60), 0) + 1
    print(f"UTC hour {h:02d}: {len(seen)} minutes with trades")
    for m in sorted(seen):
        print(f"  {iso_s(m*60)}  {seen[m]:4} prints")

def main():
    kinds, files = load()
    a = sys.argv[1:]
    if not a or a[0] == "stats":
        stats(kinds, files)
    elif a[0] == "--gaps":
        gaps(kinds, files)
    elif a[0] == "--at":
        snapshot(kinds, parse_at(a[1]) if len(a) > 1 else None)
    elif a[0] == "--tape":
        n = int(a[1]) if len(a) > 1 else 10
        for r in kinds.get("t", [])[-n:]:
            side = "BUY " if r["r"] == "t" else ("SELL" if r["r"] == "m" else r["r"])
            print(f"{iso_us(r['t'])}  {side}  {r['s']:>6} ctr @ {r['p']}")
    elif a[0] == "--hour":
        hour(kinds, int(a[1]))
    else:
        print(__doc__)

if __name__ == "__main__":
    main()
