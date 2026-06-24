#!/usr/bin/env python3
"""Respectful, bounded ETFdb pilot scraper.

Fetches only the configured public ETF detail URLs, honors robots.txt when it can
be read, caches raw HTML, checkpoints completed tickers, and writes parsed JSONL
and CSV output. It deliberately uses only ordinary GET requests and stops on
repeated blocking responses.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import random
import re
import sys
import time
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
import urllib.robotparser

BASE_URL = "https://etfdb.com"
PILOT_TICKERS = [
    "SPY", "VOO", "IVV", "VTI", "QQQ", "IWM", "EFA", "EEM", "AGG", "BND",
    "GLD", "SLV", "TLT", "HYG", "VNQ", "XLF", "XLK", "XLE", "XLV", "XLY",
    "SCHD", "JEPI", "TQQQ", "SQQQ", "ARKK",
]
USER_AGENT = "ETFDBPilotScraper/0.1 (+https://example.invalid/research; respectful pilot)"
BLOCK_STATUSES = {403, 429}
RETRY_STATUSES = {403, 429, 500, 502, 503, 504}
FIELDNAMES = [
    "ticker", "name", "price", "change", "category", "last_updated", "issuer",
    "brand", "structure", "expense_ratio", "home_page", "inception",
    "index_tracked", "analyst_report_text", "etf_database_themes",
    "factset_classifications", "trading_data", "historical_volume",
    "top_15_holdings", "holdings_comparison", "valuation", "dividend",
    "fund_flows", "aum_influence", "realtime_rating", "expenses_and_fees",
    "tax_analysis", "esg_summary_submetrics", "source_url", "fetched_at",
]



def normalize_label(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", text.lower()).strip("_")


def clean_text(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def get_with_retries(url: str, *, retries: int, timeout: int, block_limit: int) -> tuple[int, str, dict[str, str]]:
    block_count = 0
    last_error = None
    for attempt in range(1, retries + 1):
        req = Request(url, headers={"User-Agent": USER_AGENT, "Accept": "text/html,application/xhtml+xml"})
        try:
            with urlopen(req, timeout=timeout) as response:
                body = response.read().decode(response.headers.get_content_charset() or "utf-8", "replace")
                return response.status, body, dict(response.headers.items())
        except HTTPError as exc:
            last_error = f"HTTP {exc.code}: {exc.reason}"
            if exc.code in BLOCK_STATUSES:
                block_count += 1
                if block_count >= block_limit:
                    raise RuntimeError(f"blocked repeatedly while fetching {url}: {last_error}") from exc
            if exc.code not in RETRY_STATUSES or attempt == retries:
                raise
        except (TimeoutError, URLError) as exc:
            last_error = str(exc)
            if attempt == retries:
                raise
        sleep_for = min(60, (2 ** attempt) + random.uniform(0, 3))
        print(f"retrying {url} after {last_error}; sleeping {sleep_for:.1f}s", file=sys.stderr)
        time.sleep(sleep_for)
    raise RuntimeError(f"failed to fetch {url}: {last_error}")


def load_checkpoint(path: Path) -> set[str]:
    if not path.exists():
        return set()
    return {line.strip() for line in path.read_text().splitlines() if line.strip()}


def append_checkpoint(path: Path, ticker: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(ticker + "\n")


def robots_allowed(url: str, robots_body: str) -> bool:
    rp = urllib.robotparser.RobotFileParser()
    rp.parse(robots_body.splitlines())
    return rp.can_fetch(USER_AGENT, url) and rp.can_fetch("*", url)


def html_to_text(html: str) -> str:
    without_scripts = re.sub(r"<\s*(script|style|noscript)[^>]*>.*?<\s*/\s*\1\s*>", " ", html, flags=re.I | re.S)
    return clean_text(re.sub(r"<[^>]+>", " ", without_scripts))


def extract_tag_text(html: str, tag: str) -> str | None:
    match = re.search(rf"<\s*{tag}[^>]*>(.*?)<\s*/\s*{tag}\s*>", html, flags=re.I | re.S)
    return html_to_text(match.group(1)) if match else None


def parse_public_key_values(text: str) -> dict[str, str]:
    """Best-effort extraction of visible label/value pairs from page text."""
    labels = [
        "ETF Database Category", "Category", "Issuer", "Brand", "Structure",
        "Expense Ratio", "Home Page", "Homepage", "Inception", "Inception Date",
        "Index Tracked", "Index", "Last Updated",
    ]
    data: dict[str, str] = {}
    for label in labels:
        pattern = rf"{re.escape(label)}\s+(.{{1,160}}?)(?=" + "|".join(re.escape(x) for x in labels) + r"|$)"
        match = re.search(pattern, text, flags=re.I)
        if match:
            data[normalize_label(label)] = clean_text(match.group(1))
    return data


def section_text_from_plaintext(text: str, candidates: list[str]) -> str | None:
    for candidate in candidates:
        match = re.search(rf"{re.escape(candidate)}\s+(.{{1,3000}}?)(?=\s[A-Z][A-Za-z0-9 /&-]{{3,60}}\s|$)", text, flags=re.I)
        if match:
            return clean_text(match.group(1))[:3000]
    return None


def parse_etf_page(ticker: str, url: str, html: str, fetched_at: str) -> dict[str, Any]:
    record: dict[str, Any] = {name: None for name in FIELDNAMES}
    record.update({"ticker": ticker, "source_url": url, "fetched_at": fetched_at})
    text = html_to_text(html)
    title = extract_tag_text(html, "title")
    h1 = extract_tag_text(html, "h1")
    record["name"] = h1 or (title.replace("ETF Database", "") if title else None)
    price_match = re.search(r"\$\s?\d[\d,.]*(?:\.\d+)?", text)
    record["price"] = price_match.group(0) if price_match else None
    change_match = re.search(r"(?:Change|Daily Change)\s*([-+]?\$?\d[\d,.]*%?)", text, re.I)
    record["change"] = change_match.group(1) if change_match else None

    page_data = parse_public_key_values(text)
    mappings = {
        "category": ["etf_database_category", "category"], "issuer": ["issuer"],
        "brand": ["brand"], "structure": ["structure"], "expense_ratio": ["expense_ratio"],
        "home_page": ["homepage", "home_page"], "inception": ["inception", "inception_date"],
        "index_tracked": ["index_tracked", "index"], "last_updated": ["last_updated"],
    }
    for field, keys in mappings.items():
        for key in keys:
            if page_data.get(key):
                record[field] = page_data[key]
                break
    record["analyst_report_text"] = section_text_from_plaintext(text, ["Analyst Report"])
    record["top_15_holdings"] = section_text_from_plaintext(text, ["Top 15 Holdings", "Top 10 Holdings", "Top Holdings", "Holdings"])
    section_map = {
        "etf_database_themes": ["ETF Database Themes", "Themes"],
        "factset_classifications": ["FactSet", "Classification"],
        "trading_data": ["Trading Data"], "historical_volume": ["Historical Volume"],
        "holdings_comparison": ["Holdings Comparison"], "valuation": ["Valuation"],
        "dividend": ["Dividend"], "fund_flows": ["Fund Flows"],
        "aum_influence": ["AUM Influence"], "realtime_rating": ["Realtime Rating", "ETFdb Rating"],
        "expenses_and_fees": ["Expenses", "Fees"], "tax_analysis": ["Tax Analysis"],
        "esg_summary_submetrics": ["ESG", "ESG Summary"],
    }
    for field, candidates in section_map.items():
        record[field] = section_text_from_plaintext(text, candidates)
    return record


def write_outputs(records: list[dict[str, Any]], jsonl_path: Path, csv_path: Path) -> None:
    jsonl_path.parent.mkdir(parents=True, exist_ok=True)
    with jsonl_path.open("w", encoding="utf-8") as fh:
        for row in records:
            fh.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
    with csv_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(records)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--limit", type=int, default=25, help="maximum pilot tickers to consider (10-25)")
    parser.add_argument("--delay-min", type=float, default=8.0)
    parser.add_argument("--delay-max", type=float, default=12.0)
    parser.add_argument("--retries", type=int, default=3)
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument("--block-limit", type=int, default=2)
    parser.add_argument("--data-dir", type=Path, default=Path("data"))
    args = parser.parse_args()
    if not 10 <= args.limit <= 25:
        parser.error("--limit must be between 10 and 25")
    if args.delay_min < 0 or args.delay_max < args.delay_min:
        parser.error("invalid delay range")

    tickers = PILOT_TICKERS[:args.limit]
    raw_dir = args.data_dir / "raw_html"
    checkpoint_path = args.data_dir / "etfdb_pilot.checkpoint"
    jsonl_path = args.data_dir / "etfdb_pilot.jsonl"
    csv_path = args.data_dir / "etfdb_pilot.csv"
    completed = load_checkpoint(checkpoint_path)
    records: list[dict[str, Any]] = []
    attempted = succeeded = skipped = failed = 0

    print(f"Fetching robots.txt before any ETF pages: {BASE_URL}/robots.txt")
    try:
        _, robots_body, _ = get_with_retries(f"{BASE_URL}/robots.txt", retries=args.retries, timeout=args.timeout, block_limit=args.block_limit)
    except Exception as exc:
        print(f"ABORT: could not read robots.txt without being blocked: {exc}", file=sys.stderr)
        write_outputs(records, jsonl_path, csv_path)
        print(json.dumps({"attempted": attempted, "succeeded": succeeded, "skipped": skipped, "failed": len(tickers), "outputs": [str(jsonl_path), str(csv_path)], "aborted": True}, indent=2))
        return 2

    for i, ticker in enumerate(tickers, 1):
        url = f"{BASE_URL}/etf/{ticker}/"
        if ticker in completed:
            skipped += 1
            print(f"skip checkpointed {ticker}")
            continue
        if not robots_allowed(url, robots_body):
            failed += 1
            print(f"robots disallows {url}; not fetching", file=sys.stderr)
            continue
        if attempted:
            sleep_for = random.uniform(args.delay_min, args.delay_max)
            print(f"sleeping {sleep_for:.1f}s before next request")
            time.sleep(sleep_for)
        attempted += 1
        print(f"fetching {i}/{len(tickers)} {url}")
        fetched_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        try:
            status, html, _ = get_with_retries(url, retries=args.retries, timeout=args.timeout, block_limit=args.block_limit)
            cache_path = raw_dir / f"{ticker}_{hashlib.sha256(url.encode()).hexdigest()[:10]}.html"
            raw_dir.mkdir(parents=True, exist_ok=True)
            cache_path.write_text(html, encoding="utf-8")
            records.append(parse_etf_page(ticker, url, html, fetched_at))
            append_checkpoint(checkpoint_path, ticker)
            succeeded += 1
            print(f"saved {ticker} HTTP {status} -> {cache_path}")
        except RuntimeError as exc:
            failed += 1
            print(f"ABORT: {exc}", file=sys.stderr)
            break
        except Exception as exc:
            failed += 1
            print(f"failed {ticker}: {exc}", file=sys.stderr)
    write_outputs(records, jsonl_path, csv_path)
    print(json.dumps({"attempted": attempted, "succeeded": succeeded, "skipped": skipped, "failed": failed, "outputs": [str(jsonl_path), str(csv_path)], "raw_html_dir": str(raw_dir)}, indent=2))
    return 0 if failed == 0 else 1

if __name__ == "__main__":
    raise SystemExit(main())
