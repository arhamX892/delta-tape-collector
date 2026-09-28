# delta-tape-collector

24/7 recorder for the Delta India BTCUSD aggressor tape (every trade + 1-minute candles),
running free on GitHub Actions. The daily NDJSON files in `data/` are the training dataset
for the ML filter.

**Setup: just push these files to a GitHub repo and enable Actions — nothing else.**

- Records 4 windows/day (~5h50m each) ≈ 23h20m of tape
- Commits each window automatically
- Public repo = free; private also fits the free 2,000 min/month
- Data is anonymous market data — nothing personal
