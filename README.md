# Beauty Market Tracker

Railway-ready FastAPI app for the validated 91-company beauty universe in `data/BEAUTY_UNIVERSE_MASTER.csv`.

It runs as a long-lived web service, not a Railway Cron job. That matters because the app keeps the previous quote snapshot in memory and compares each new refresh against that in-memory baseline.

## What It Does

- Loads the company universe from `data/BEAUTY_UNIVERSE_MASTER.csv`.
- Tracks ticker-backed companies with `yfinance`.
- Fetches quotes with per-symbol retry and a recent-history fallback, because Yahoo's batched `fast_info` path can return `KeyError` even for valid tickers.
- Adds ticker overrides for six companies that were present but missing symbols in the original CSV: Terminal X, Shobido, Coreana Cosmetics, Kimberly-Clark, Nature's Sunshine Products, and Nu Skin Enterprises.
- Excludes non-standalone or Yahoo-unpriceable rows from the app-local CSV: Avon Products, Fancl, Revlon, MAV Beauty Brands, Relativity Holdings, and Scientist Home Future Health.
- Updates Natura from the delisted `NTCO3.SA` symbol to `NATU3.SA`.
- Stores the last quote snapshot in process memory.
- Calculates direction, price change, and percent change on each refresh.
- Pulls company-related news with this fallback order: Finnhub if `FINNHUB_API_KEY` is set, Alpha Vantage if `ALPHA_VANTAGE_API_KEY` is set, yfinance/Yahoo Finance, then Google News RSS.
- Shows a spreadsheet-like dashboard at `/`.
- Exposes raw JSON at `/api/markets`.

## Run Locally

```bash
cd railway_market_tracker
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8000
```

Then open `http://localhost:8000`.

## Deploy On Railway

Deploy the `railway_market_tracker` folder as the Railway service root. Railway will use `railway.json` and start:

```bash
uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8000}
```

## Environment Variables

- `UNIVERSE_CSV`: optional path to the CSV. Defaults to `data/BEAUTY_UNIVERSE_MASTER.csv`, with a local fallback to `../BEAUTY_UNIVERSE_MASTER.csv` for development.
- `REFRESH_SECONDS`: quote/news refresh interval. Defaults to `900` seconds.
- `NEWS_PER_COMPANY`: Google News RSS items per company. Defaults to `3`.
- `QUOTE_BATCH_SIZE`: number of symbols requested per yfinance batch. Defaults to `40`.
- `FINNHUB_API_KEY`: optional. Enables Finnhub company-news lookup before other news providers.
- `ALPHA_VANTAGE_API_KEY`: optional. Enables Alpha Vantage `NEWS_SENTIMENT` lookup if Finnhub is not configured or has no result.

## Important Limits

This uses free data paths, so it is good for monitoring and discovery, not audited trading data. `yfinance`, Finnhub free tier, Alpha Vantage free tier, and Google News RSS can throttle, omit symbols, or change behavior. If this becomes production-critical, swap the quote and news functions for paid APIs with SLAs.

The app-local universe is 91 companies after removing rows that are private, delisted, acquired, or not resolvable through Yahoo Finance. A live validation run confirmed 91/91 rows resolve to quote prices through yfinance.
