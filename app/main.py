from __future__ import annotations

import asyncio
import csv
import html
import json
import os
import re
import threading
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import yfinance as yf
from fastapi import FastAPI, Response
from fastapi.responses import HTMLResponse


APP_DIR = Path(__file__).resolve().parent
PROJECT_DIR = APP_DIR.parent
DEFAULT_UNIVERSE = PROJECT_DIR / "data" / "BEAUTY_UNIVERSE_MASTER.csv"

UNIVERSE_CSV = Path(os.getenv("UNIVERSE_CSV", DEFAULT_UNIVERSE))
REFRESH_SECONDS = int(os.getenv("REFRESH_SECONDS", "900"))
NEWS_PER_COMPANY = int(os.getenv("NEWS_PER_COMPANY", "3"))
QUOTE_BATCH_SIZE = int(os.getenv("QUOTE_BATCH_SIZE", "40"))
FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY", "")
ALPHA_VANTAGE_API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY", "")


SYMBOL_OVERRIDES = {
    # These rows exist in the master universe but had blank ticker fields.
    # Three remaining blanks are private/delisted as standalone stocks:
    # Avon Products, Fancl, and Revlon.
    "Coreana Cosmetics": ["027050.KQ"],
    "Kimberly-Clark": ["KMB"],
    "Nature's Sunshine Products": ["NATR"],
    "Nu Skin Enterprises": ["NUS"],
    "Shobido": ["7819.T"],
    "Terminal X": ["TRX.TA"],
}


@dataclass
class Company:
    company: str
    country: str
    ticker: str
    exchange: str
    isin: str
    sources: str
    notes: str
    symbols: list[str] = field(default_factory=list)


@dataclass
class QuoteState:
    price: float | None = None
    previous_price: float | None = None
    change: float | None = None
    change_pct: float | None = None
    direction: str = "flat"
    currency: str = ""
    market_state: str = ""
    updated_at: str = ""
    error: str = ""


@dataclass
class NewsItem:
    title: str
    url: str
    source: str
    published: str


class MarketTracker:
    def __init__(self, universe_csv: Path):
        self.universe_csv = universe_csv
        self.companies = load_companies(universe_csv)
        self.quotes: dict[str, QuoteState] = {}
        self.news: dict[str, list[NewsItem]] = {}
        self.last_refresh_started = ""
        self.last_refresh_finished = ""
        self.refresh_count = 0
        self.last_error = ""
        self._lock = threading.RLock()

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            rows = []
            for company in self.companies:
                quote = self.quotes.get(company.company, QuoteState())
                rows.append({
                    **asdict(company),
                    "quote": asdict(quote),
                    "news": [asdict(item) for item in self.news.get(company.company, [])],
                })

            movers = sorted(
                rows,
                key=lambda row: abs(row["quote"]["change_pct"] or 0),
                reverse=True,
            )[:10]

            return {
                "companies": rows,
                "movers": movers,
                "company_count": len(self.companies),
                "tracked_count": sum(1 for c in self.companies if c.symbols),
                "last_refresh_started": self.last_refresh_started,
                "last_refresh_finished": self.last_refresh_finished,
                "refresh_count": self.refresh_count,
                "last_error": self.last_error,
                "refresh_seconds": REFRESH_SECONDS,
            }

    def refresh(self) -> None:
        started = utc_now()
        with self._lock:
            self.last_refresh_started = started
            self.last_error = ""

        try:
            quotes = fetch_quotes(self.companies, self.quotes)
            news = fetch_news_for_companies(self.companies)
            with self._lock:
                self.quotes = quotes
                self.news = news
                self.last_refresh_finished = utc_now()
                self.refresh_count += 1
        except Exception as exc:
            with self._lock:
                self.last_error = f"{type(exc).__name__}: {exc}"
                self.last_refresh_finished = utc_now()


def load_companies(path: Path) -> list[Company]:
    if not path.exists() and path == DEFAULT_UNIVERSE:
        path = PROJECT_DIR.parent / "BEAUTY_UNIVERSE_MASTER.csv"
    if not path.exists():
        raise FileNotFoundError(f"Universe CSV not found: {path}")

    companies: list[Company] = []
    with path.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            symbols = parse_symbols(row.get("ticker", ""))
            if not symbols:
                symbols = SYMBOL_OVERRIDES.get(row.get("company", ""), [])
            companies.append(Company(
                company=row.get("company", ""),
                country=row.get("country", ""),
                ticker=row.get("ticker", ""),
                exchange=row.get("exchange", ""),
                isin=row.get("isin", ""),
                sources=row.get("sources", ""),
                notes=row.get("notes", ""),
                symbols=symbols,
            ))
    return companies


def parse_symbols(ticker_value: str) -> list[str]:
    symbols: list[str] = []
    seen: set[str] = set()
    for raw in ticker_value.split("|"):
        symbol = raw.strip()
        if not symbol:
            continue
        if ":" in symbol and " " not in symbol.split(":", 1)[0]:
            symbol = symbol.split(":", 1)[0].strip()
        if symbol and symbol not in seen:
            seen.add(symbol)
            symbols.append(symbol)
    return symbols


def fetch_quotes(companies: list[Company], previous: dict[str, QuoteState]) -> dict[str, QuoteState]:
    symbol_to_company: dict[str, Company] = {}
    for company in companies:
        if company.symbols:
            symbol_to_company[company.symbols[0]] = company

    output: dict[str, QuoteState] = {}
    symbols = list(symbol_to_company)

    for i in range(0, len(symbols), QUOTE_BATCH_SIZE):
        batch = symbols[i:i + QUOTE_BATCH_SIZE]
        tickers = yf.Tickers(" ".join(batch))
        for symbol in batch:
            company = symbol_to_company[symbol]
            prior = previous.get(company.company, QuoteState())
            try:
                info = tickers.tickers[symbol].fast_info
                price = safe_float(get_fast_info(info, "last_price"))
                prev_close = safe_float(get_fast_info(info, "previous_close"))
                currency = str(get_fast_info(info, "currency") or "")
                market_state = str(get_fast_info(info, "market_state") or "")

                baseline = prior.price if prior.price is not None else prev_close
                change = price - baseline if price is not None and baseline is not None else None
                change_pct = (change / baseline * 100) if change is not None and baseline else None
                direction = "flat"
                if change and change > 0:
                    direction = "up"
                elif change and change < 0:
                    direction = "down"

                output[company.company] = QuoteState(
                    price=price,
                    previous_price=baseline,
                    change=change,
                    change_pct=change_pct,
                    direction=direction,
                    currency=currency,
                    market_state=market_state,
                    updated_at=utc_now(),
                )
            except Exception as exc:
                output[company.company] = QuoteState(
                    previous_price=prior.price,
                    updated_at=utc_now(),
                    error=f"{symbol}: {type(exc).__name__}",
                )

    return output


def get_fast_info(info: Any, key: str) -> Any:
    if hasattr(info, key):
        return getattr(info, key)
    try:
        return info[key]
    except Exception:
        return None


def safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def fetch_news_for_companies(companies: list[Company]) -> dict[str, list[NewsItem]]:
    news: dict[str, list[NewsItem]] = {}
    for company in companies[:]:
        symbol = company.symbols[0] if company.symbols else ""
        items: list[NewsItem] = []
        if FINNHUB_API_KEY and symbol:
            items = fetch_finnhub_news(symbol, NEWS_PER_COMPANY)
        if not items and ALPHA_VANTAGE_API_KEY and symbol:
            items = fetch_alpha_vantage_news(symbol, NEWS_PER_COMPANY)
        if not items and symbol:
            items = fetch_yfinance_news(company.company, symbol, NEWS_PER_COMPANY)
        if not items:
            items = fetch_google_news(company.company, NEWS_PER_COMPANY)
        news[company.company] = items
    return news


def fetch_finnhub_news(symbol: str, limit: int) -> list[NewsItem]:
    to_date = date.today()
    from_date = to_date - timedelta(days=10)
    params = urllib.parse.urlencode({
        "symbol": symbol,
        "from": from_date.isoformat(),
        "to": to_date.isoformat(),
        "token": FINNHUB_API_KEY,
    })
    data = fetch_json(f"https://finnhub.io/api/v1/company-news?{params}", timeout=8)
    if not isinstance(data, list):
        return []

    items: list[NewsItem] = []
    for item in data[:limit]:
        title = str(item.get("headline") or "")
        url = str(item.get("url") or "")
        if title and url:
            published = ""
            if item.get("datetime"):
                published = datetime.fromtimestamp(int(item["datetime"]), timezone.utc).isoformat()
            items.append(NewsItem(
                title=title,
                url=url,
                source=str(item.get("source") or "Finnhub"),
                published=published,
            ))
    return items


def fetch_alpha_vantage_news(symbol: str, limit: int) -> list[NewsItem]:
    params = urllib.parse.urlencode({
        "function": "NEWS_SENTIMENT",
        "tickers": symbol,
        "sort": "LATEST",
        "limit": limit,
        "apikey": ALPHA_VANTAGE_API_KEY,
    })
    data = fetch_json(f"https://www.alphavantage.co/query?{params}", timeout=8)
    feed = data.get("feed", []) if isinstance(data, dict) else []
    items: list[NewsItem] = []
    for item in feed[:limit]:
        title = str(item.get("title") or "")
        url = str(item.get("url") or "")
        if title and url:
            items.append(NewsItem(
                title=title,
                url=url,
                source=str(item.get("source") or "Alpha Vantage"),
                published=str(item.get("time_published") or ""),
            ))
    return items


def fetch_yfinance_news(company_name: str, symbol: str, limit: int) -> list[NewsItem]:
    candidates: list[dict[str, Any]] = []
    try:
        candidates = yf.Ticker(symbol).get_news(count=limit, tab="news")
    except Exception:
        candidates = []

    if not candidates:
        try:
            candidates = yf.Search(company_name, news_count=limit).news
        except Exception:
            candidates = []

    items: list[NewsItem] = []
    for item in candidates[:limit]:
        title = nested_get(item, "title") or nested_get(item, "content", "title")
        url = (
            nested_get(item, "link")
            or nested_get(item, "url")
            or nested_get(item, "content", "canonicalUrl", "url")
            or nested_get(item, "content", "clickThroughUrl", "url")
        )
        source = (
            nested_get(item, "publisher")
            or nested_get(item, "source")
            or nested_get(item, "content", "provider", "displayName")
            or "Yahoo Finance"
        )
        published = str(
            nested_get(item, "providerPublishTime")
            or nested_get(item, "content", "pubDate")
            or ""
        )
        if title and url:
            items.append(NewsItem(
                title=str(title),
                url=str(url),
                source=str(source),
                published=published,
            ))
    return items


def fetch_json(url: str, timeout: int) -> Any:
    request = urllib.request.Request(url, headers={"User-Agent": "beauty-market-tracker/1.0"})
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return json.loads(response.read().decode("utf-8"))
    except Exception:
        return {}


def fetch_google_news(company_name: str, limit: int) -> list[NewsItem]:
    query = urllib.parse.quote_plus(f'"{company_name}" stock OR earnings OR shares')
    url = f"https://news.google.com/rss/search?q={query}&hl=en-US&gl=US&ceid=US:en"
    request = urllib.request.Request(url, headers={"User-Agent": "beauty-market-tracker/1.0"})
    try:
        with urllib.request.urlopen(request, timeout=8) as response:
            raw = response.read()
    except Exception:
        return []

    try:
        root = ET.fromstring(raw)
    except ET.ParseError:
        return []

    items: list[NewsItem] = []
    for item in root.findall("./channel/item")[:limit]:
        title = text_of(item, "title")
        link = text_of(item, "link")
        published = text_of(item, "pubDate")
        source_el = item.find("source")
        source = source_el.text if source_el is not None and source_el.text else "Google News"
        if title and link:
            items.append(NewsItem(title=title, url=link, source=source, published=published))
    return items


def text_of(parent: ET.Element, tag: str) -> str:
    child = parent.find(tag)
    return child.text or "" if child is not None else ""


def nested_get(data: Any, *keys: str) -> Any:
    current = data
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


tracker = MarketTracker(UNIVERSE_CSV)
app = FastAPI(title="Beauty Market Tracker")


@app.on_event("startup")
async def startup() -> None:
    asyncio.create_task(refresh_loop())


async def refresh_loop() -> None:
    await asyncio.to_thread(tracker.refresh)
    while True:
        await asyncio.sleep(REFRESH_SECONDS)
        await asyncio.to_thread(tracker.refresh)


@app.get("/", response_class=HTMLResponse)
def index() -> str:
    return render_html(tracker.snapshot())


@app.get("/api/markets")
def api_markets() -> dict[str, Any]:
    return tracker.snapshot()


@app.post("/api/refresh")
async def api_refresh() -> dict[str, Any]:
    await asyncio.to_thread(tracker.refresh)
    return tracker.snapshot()


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "last_refresh_finished": tracker.snapshot()["last_refresh_finished"]}


@app.get("/styles.css")
def styles() -> Response:
    return Response(CSS, media_type="text/css")


def render_html(data: dict[str, Any]) -> str:
    rows = "\n".join(render_row(row) for row in data["companies"])
    movers = "\n".join(render_mover(row) for row in data["movers"])
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta http-equiv="refresh" content="120">
  <title>Beauty Market Tracker</title>
  <link rel="stylesheet" href="/styles.css">
</head>
<body>
  <main>
    <section class="hero">
      <div>
        <p class="eyebrow">In-memory market watch</p>
        <h1>Beauty companies, prices, movement, and news.</h1>
      </div>
      <div class="stats">
        <div><strong>{data["company_count"]}</strong><span>companies</span></div>
        <div><strong>{data["tracked_count"]}</strong><span>ticker-backed</span></div>
        <div><strong>{data["refresh_seconds"] // 60}m</strong><span>refresh</span></div>
      </div>
    </section>

    <section class="status">
      <span>Last refresh: {esc(data["last_refresh_finished"] or "pending")}</span>
      <span>Runs: {data["refresh_count"]}</span>
      <span>{esc(data["last_error"] or "No tracker errors")}</span>
      <button id="refreshBtn">Refresh now</button>
    </section>

    <section class="movers">
      <h2>Largest moves since last in-memory baseline</h2>
      <div class="mover-grid">{movers}</div>
    </section>

    <section class="sheet">
      <div class="sheet-title">
        <h2>Market sheet</h2>
        <input id="search" placeholder="Filter companies, tickers, countries, notes...">
      </div>
      <div class="table-wrap">
        <table id="marketTable">
          <thead>
            <tr>
              <th>Company</th>
              <th>Country</th>
              <th>Ticker</th>
              <th>Price</th>
              <th>Move</th>
              <th>Status</th>
              <th>News</th>
              <th>Notes</th>
            </tr>
          </thead>
          <tbody>{rows}</tbody>
        </table>
      </div>
    </section>
  </main>
  <script>
    const input = document.getElementById('search');
    const rows = [...document.querySelectorAll('#marketTable tbody tr')];
    input.addEventListener('input', () => {{
      const q = input.value.toLowerCase();
      rows.forEach(row => row.hidden = !row.innerText.toLowerCase().includes(q));
    }});
    document.getElementById('refreshBtn').addEventListener('click', async () => {{
      await fetch('/api/refresh', {{ method: 'POST' }});
      location.reload();
    }});
  </script>
</body>
</html>"""


def render_row(row: dict[str, Any]) -> str:
    quote = row["quote"]
    news = row["news"]
    price = money(quote["price"], quote["currency"])
    move = pct(quote["change_pct"])
    direction = esc(quote["direction"])
    news_links = "".join(
        f'<a href="{esc(item["url"])}" target="_blank" rel="noreferrer">{esc(item["source"])}: {esc(trim(item["title"], 90))}</a>'
        for item in news
    )
    if not news_links:
        query = urllib.parse.quote_plus(f'"{row["company"]}" stock earnings shares')
        news_links = f'<a href="https://news.google.com/search?q={query}" target="_blank" rel="noreferrer">Search free news</a>'

    status = quote["error"] or quote["market_state"] or quote["updated_at"] or "waiting"
    return f"""<tr class="{direction}">
  <td><strong>{esc(row["company"])}</strong><small>{esc(row["sources"])}</small></td>
  <td>{esc(row["country"])}</td>
  <td><code>{esc(row["ticker"])}</code></td>
  <td>{price}</td>
  <td class="move">{move}</td>
  <td>{esc(status)}</td>
  <td class="news">{news_links}</td>
  <td>{esc(trim(row["notes"], 140))}</td>
</tr>"""


def render_mover(row: dict[str, Any]) -> str:
    quote = row["quote"]
    return f"""<article class="{esc(quote["direction"])}">
  <strong>{esc(row["company"])}</strong>
  <span>{esc(row["ticker"])}</span>
  <b>{pct(quote["change_pct"])}</b>
</article>"""


def esc(value: Any) -> str:
    return html.escape(str(value or ""))


def trim(value: str, length: int) -> str:
    value = re.sub(r"\s+", " ", value or "").strip()
    return value if len(value) <= length else value[:length - 1] + "..."


def money(value: float | None, currency: str) -> str:
    if value is None:
        return "n/a"
    return f"{value:,.2f} {esc(currency)}".strip()


def pct(value: float | None) -> str:
    if value is None:
        return "n/a"
    sign = "+" if value > 0 else ""
    return f"{sign}{value:.2f}%"


CSS = """
:root {
  --ink: #1b1715;
  --muted: #766a62;
  --paper: #fbf3e9;
  --line: #dfcdbd;
  --up: #087f5b;
  --down: #b42318;
  --card: rgba(255, 252, 247, .88);
}
* { box-sizing: border-box; }
body {
  margin: 0;
  color: var(--ink);
  font-family: Georgia, 'Times New Roman', serif;
  background:
    radial-gradient(circle at 20% 10%, #ffd6bf 0, transparent 32%),
    radial-gradient(circle at 88% 0%, #d5ead8 0, transparent 28%),
    linear-gradient(135deg, #fff9f0 0%, var(--paper) 45%, #efe1d4 100%);
}
main { width: min(1480px, calc(100vw - 32px)); margin: 0 auto; padding: 34px 0; }
.hero { display: flex; justify-content: space-between; gap: 24px; align-items: end; margin-bottom: 18px; }
.eyebrow { text-transform: uppercase; letter-spacing: .14em; color: var(--muted); font-size: 12px; margin: 0 0 10px; }
h1 { font-size: clamp(38px, 6vw, 82px); line-height: .9; max-width: 850px; margin: 0; letter-spacing: -0.06em; }
h2 { margin: 0; font-size: 22px; letter-spacing: -0.03em; }
.stats { display: grid; grid-template-columns: repeat(3, 1fr); gap: 10px; min-width: 360px; }
.stats div, .status, .movers article, .sheet { background: var(--card); border: 1px solid var(--line); box-shadow: 0 18px 50px rgba(59, 38, 22, .08); }
.stats div { padding: 16px; }
.stats strong { display: block; font-size: 34px; }
.stats span, small { color: var(--muted); }
.status { display: flex; gap: 18px; align-items: center; flex-wrap: wrap; padding: 12px 14px; margin-bottom: 18px; font-size: 14px; }
button { background: var(--ink); color: #fffaf3; border: 0; border-radius: 999px; padding: 9px 14px; cursor: pointer; }
.movers { margin-bottom: 18px; }
.movers h2 { margin-bottom: 10px; }
.mover-grid { display: grid; grid-template-columns: repeat(5, minmax(0, 1fr)); gap: 10px; }
.movers article { padding: 14px; min-height: 106px; display: grid; align-content: space-between; }
.movers article span { color: var(--muted); font-size: 13px; overflow-wrap: anywhere; }
.movers article b { font-size: 28px; }
.up .move, article.up b { color: var(--up); }
.down .move, article.down b { color: var(--down); }
.sheet { overflow: hidden; }
.sheet-title { display: flex; justify-content: space-between; gap: 14px; align-items: center; padding: 16px; border-bottom: 1px solid var(--line); }
input { width: min(520px, 100%); border: 1px solid var(--line); background: #fffaf3; padding: 12px 14px; font: inherit; }
.table-wrap { overflow: auto; max-height: 72vh; }
table { width: 100%; border-collapse: collapse; font-size: 14px; background: rgba(255,255,255,.35); }
th, td { padding: 12px 10px; border-bottom: 1px solid var(--line); text-align: left; vertical-align: top; }
th { position: sticky; top: 0; background: #eadccd; z-index: 2; font-size: 12px; text-transform: uppercase; letter-spacing: .08em; }
td:first-child { min-width: 260px; }
td:nth-child(7) { min-width: 320px; }
code { white-space: normal; overflow-wrap: anywhere; color: #5b2d1f; }
.news a { display: block; color: var(--ink); margin-bottom: 7px; text-decoration-color: #b98b6e; }
small { display: block; margin-top: 5px; font-size: 12px; }
@media (max-width: 900px) {
  main { width: min(100% - 20px, 1480px); padding: 20px 0; }
  .hero, .sheet-title { display: block; }
  .stats { min-width: 0; margin-top: 18px; grid-template-columns: repeat(3, 1fr); }
  .mover-grid { grid-template-columns: 1fr 1fr; }
  input { margin-top: 12px; }
}
"""
