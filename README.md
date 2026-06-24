# ETFdb Pilot Scraper

This repository contains a bounded pilot scraper for public ETF detail pages at
`https://etfdb.com/etf/{TICKER}/`.

The scraper is intentionally conservative:

- uses only GET requests;
- considers only the fixed 25 ticker pilot list;
- reads `robots.txt` before fetching ETF pages and aborts if it cannot verify access;
- does not bypass paywalls, login walls, captchas, robots controls, rate limits, or Pro-gated content;
- caches raw HTML under `data/raw_html/`;
- writes parsed output to `data/etfdb_pilot.jsonl` and `data/etfdb_pilot.csv`;
- checkpoints completed tickers in `data/etfdb_pilot.checkpoint` so completed pages are not fetched twice;
- waits a randomized 8-12 seconds between ETF detail requests;
- retries with backoff for 403, 429, 5xx, and timeout-like failures, then stops on repeated 403/429 blocks.

Run:

```bash
python etfdb_pilot_scraper.py
```

Diagnostics only, without fetching ETF detail pages:

```bash
python etfdb_pilot_scraper.py --diagnostics
```

Diagnostics mode requests `robots.txt` with `GET` and checks the SPY detail URL
with `HEAD` only. If `robots.txt` is blocked, it stops immediately.

The default pilot list is exactly 25 tickers: SPY, VOO, IVV, VTI, QQQ, IWM, EFA,
EEM, AGG, BND, GLD, SLV, TLT, HYG, VNQ, XLF, XLK, XLE, XLV, XLY, SCHD, JEPI,
TQQQ, SQQQ, ARKK.
