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
