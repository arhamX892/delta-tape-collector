# delta-tape-collector

24/7 recorder for the Delta India BTCUSD aggressor tape (every trade + 1-minute candles),
running free on GitHub Actions. The daily NDJSON files in `data/` are the training dataset
for the ML filter.

**Setup: just push these files to a GitHub repo and enable Actions — nothing else.**

- Records 4 main windows/day (~5h50m each) plus 4 short bridge windows (25 min)
  that cover the seam between main windows → effectively 24h/day, no gaps
- One NDJSON file per recording window (`tape-<date>-<startHHMM>.ndjson`);
  bridge windows overlap each seam by a few minutes, so dedupe by trade
  timestamp when merging files for training
- Commits each window automatically
- Public repo = free; private also fits the free 2,000 min/month
- Data is anonymous market data — nothing personal

## What's on tape (schema v3 — blackbox contract)

| kind | what | rate |
|---|---|---|
| `meta` | window metadata: schema v, symbol, start, duration, git sha | first line |
| `t` | every trade: `t` (µs), price, size (contracts), aggressor role `r`, publish `ts` | every print |
| `c` | 1m candle frames (cumulative — reduce to last frame per `cst`) | many/min |
| `o` | book L1: ask/bid price + size | 1 Hz |
| `k` | ticker: spot, mark, best ask/bid | 30 s |
| `f` | funding rate, open interest, 24h volume + change (REST poll) | 15 min |
| `x` | raw frames from experimental channels (`ob_updates` depth, `funding_rate`) | 30 s throttle |
| `hb` | heartbeat | 60 s |
| `rc` | reconnect markers | on event |
| `rc1` | authoritative REST 1m candles for the whole window | window end |

Units: WS timestamps µs; REST candle times SECONDS; sizes in contracts (1 = 0.001 BTC).

## Asking the tape questions

```bash
python collector/query.py stats                        # what's stored, flow, spread profile
python collector/query.py --at "2026-09-28 17:00:28"   # full market snapshot at any moment (UTC)
python collector/query.py --gaps                       # outages, reconnects, window lineage
python collector/query.py --tape 20                    # last 20 prints
python collector/query.py --hour 16                    # per-minute print counts for a UTC hour
```

If it isn't on tape, the question is unanswerable — that's what the schema is for.
