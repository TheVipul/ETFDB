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
import html as html_lib
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

try:
    from bs4 import BeautifulSoup
except ImportError:  # pragma: no cover - fallback keeps the scraper runnable.
    BeautifulSoup = None

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
    "performance", "holdings_analysis_charts",
]
DIAGNOSTIC_HEADER_NAMES = {
    "content-type", "content-length", "date", "server", "cf-ray", "cf-cache-status",
    "location", "retry-after", "x-cache", "x-served-by", "via",
}



def normalize_label(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", text.lower()).strip("_")


def clean_text(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def soup_text(node: Any) -> str:
    return clean_text(html_lib.unescape(node.get_text(" ", strip=True))) if node else ""


def request_with_retries(
    url: str,
    *,
    method: str,
    retries: int,
    timeout: int,
    block_limit: int,
) -> tuple[int, str, dict[str, str], str]:
    block_count = 0
    last_error = None
    for attempt in range(1, retries + 1):
        req = Request(
            url,
            method=method,
            headers={"User-Agent": USER_AGENT, "Accept": "text/html,application/xhtml+xml,text/plain"},
        )
        try:
            with urlopen(req, timeout=timeout) as response:
                body = response.read().decode(response.headers.get_content_charset() or "utf-8", "replace")
                return response.status, body, dict(response.headers.items()), response.url
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


def get_with_retries(url: str, *, retries: int, timeout: int, block_limit: int) -> tuple[int, str, dict[str, str]]:
    status, body, headers, _ = request_with_retries(
        url, method="GET", retries=retries, timeout=timeout, block_limit=block_limit
    )
    return status, body, headers


def diagnostic_headers(headers: dict[str, str]) -> dict[str, str]:
    return {key: value for key, value in headers.items() if key.lower() in DIAGNOSTIC_HEADER_NAMES}


def run_diagnostics(args: argparse.Namespace) -> int:
    print("Diagnostics mode: no ETF detail pages will be fetched with GET.")
    urls = [
        ("robots", f"{BASE_URL}/robots.txt", "GET"),
        ("spy_head", f"{BASE_URL}/etf/SPY/", "HEAD"),
    ]
    results = []
    for name, url, method in urls:
        print(f"diagnostic {method} {url}")
        try:
            status, body, headers, final_url = request_with_retries(
                url,
                method=method,
                retries=args.retries,
                timeout=args.timeout,
                block_limit=args.block_limit,
            )
            result = {
                "name": name,
                "method": method,
                "url": url,
                "final_url": final_url,
                "status": status,
                "headers": diagnostic_headers(headers),
            }
            if name == "robots" and status == 200:
                result["robots_preview"] = body[:300]
            results.append(result)
        except Exception as exc:
            results.append({"name": name, "method": method, "url": url, "error": str(exc)})
            if name == "robots":
                print("robots.txt could not be read safely; stopping diagnostics before any ETF page GET.", file=sys.stderr)
                print(json.dumps({"diagnostics": results, "aborted": True}, indent=2))
                return 2
    print(json.dumps({"diagnostics": results, "aborted": False}, indent=2))
    return 0


def load_checkpoint(path: Path) -> set[str]:
    if not path.exists():
        return set()
    return {line.strip() for line in path.read_text().splitlines() if line.strip()}


def load_existing_records(path: Path) -> dict[str, dict[str, Any]]:
    if not path.exists():
        return {}
    records = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        ticker = row.get("ticker")
        if ticker:
            records[ticker] = row
    return records


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


def parse_labeled_rows(container: Any) -> dict[str, str]:
    data: dict[str, str] = {}
    if not container:
        return data
    for row in container.select(".row"):
        spans = row.find_all("span", recursive=False)
        if len(spans) < 2:
            continue
        label = normalize_label(soup_text(spans[0]))
        value = soup_text(spans[-1])
        if label and value:
            data[label] = value
    return data


def find_section_after_heading(soup: Any, heading_text: str) -> Any:
    pattern = re.compile(rf"\b{re.escape(heading_text)}\b", re.I)
    heading = soup.find(["h2", "h3", "h4"], string=pattern)
    if not heading:
        for candidate in soup.find_all(["h2", "h3", "h4"]):
            if pattern.search(soup_text(candidate)):
                heading = candidate
                break
    return heading.find_parent(["div", "section"]) if heading else None


def clean_header(text: str) -> str:
    words = text.split()
    if len(words) % 2 == 0 and words[: len(words) // 2] == words[len(words) // 2 :]:
        return " ".join(words[: len(words) // 2])
    return text


def table_rows(table: Any) -> list[dict[str, str]]:
    if not table:
        return []
    headers = [clean_header(soup_text(th)) for th in table.select("thead th")]
    rows: list[dict[str, str]] = []
    body_rows = table.select("tbody tr")
    candidate_rows = body_rows or [
        tr for tr in table.find_all("tr", recursive=False)
        if not tr.find_parent("thead") and not tr.find("th")
    ]
    for tr in candidate_rows:
        cells = tr.find_all(["td", "th"], recursive=False)
        values = [soup_text(cell) for cell in cells]
        if not any(values):
            continue
        if headers and len(headers) == len(values):
            rows.append({headers[i] or f"column_{i+1}": values[i] for i in range(len(values))})
        else:
            row = {}
            for i, cell in enumerate(cells):
                key = cell.get("data-th") or (headers[i] if i < len(headers) else f"column_{i+1}")
                row[key or f"column_{i+1}"] = values[i]
            rows.append(row)
    return rows


def two_column_table(table: Any) -> dict[str, str]:
    data = {}
    for row in table_rows(table):
        values = list(row.values())
        if len(values) >= 2 and values[0]:
            data[normalize_label(values[0])] = values[1]
    return data


def metric_comparison_rows(table: Any, fund_key: str, value_key: str = "value") -> list[dict[str, str]]:
    rows = []
    for row in table_rows(table):
        label = row.get("") or row.get("column_1")
        fund_value = row.get(fund_key) or row.get("Fund")
        category_average = row.get("ETF Database Category Average")
        segment_average = row.get("FactSet Segment Average")
        if label and (fund_value or category_average or segment_average):
            rows.append({
                "metric": label,
                value_key: fund_value,
                "category_average": category_average,
                "factset_segment_average": segment_average,
            })
    return rows


def regex_metrics(text: str, suffix: str) -> dict[str, str]:
    metrics = {}
    pattern = rf"((?:5 Day|1 Month|3 Month|6 Month|1 Year|3 Year|5 Year|10 Year) {re.escape(suffix)}):\s*([-+]?\$?\s?[\d,.]+%?(?:\s?[BMK])?|N/A)"
    for label, value in re.findall(pattern, text):
        metrics[normalize_label(label.replace(suffix, ""))] = clean_text(value)
    return metrics


def valuation_metrics(section: Any, ticker: str) -> dict[str, str]:
    if not section:
        return {}
    text = soup_text(section)
    patterns = {
        "fund_pe_ratio": rf"{re.escape(ticker)} P/E Ratio\s+([^\s]+)",
        "category_average_pe_ratio": r"ETF Database Category Average P/E Ratio\s+([^\s]+)",
        "factset_segment_average_pe_ratio": r"FactSet Segment Average P/E Ratio\s+([^\s]+)",
    }
    data = {}
    for key, pattern in patterns.items():
        match = re.search(pattern, text, re.I)
        if match:
            data[key] = match.group(1)
    return data


def expense_metrics(section: Any, ticker: str) -> dict[str, str]:
    if not section:
        return {}
    text = soup_text(section)
    patterns = {
        "fund_expense_ratio": rf"{re.escape(ticker)} Expense Ratio\s+([^\s]+)",
        "category_average_expense_ratio": r"ETF Database Category Average Expense Ratio\s+([^\s]+)",
        "factset_segment_average_expense_ratio": r"FactSet Segment Average Expense Ratio\s+([^\s]+)",
    }
    data = {}
    for key, pattern in patterns.items():
        match = re.search(pattern, text, re.I)
        if match:
            data[key] = match.group(1)
    return data


def performance_rows(table: Any, ticker: str) -> list[dict[str, str]]:
    rows = []
    for row in table_rows(table):
        period = row.get("") or row.get("column_1")
        fund_return = row.get(ticker)
        category_average = row.get("ETF Database Category Average")
        if period and (fund_return or category_average):
            rows.append({
                "period": period,
                "fund_return": fund_return,
                "category_average": category_average,
            })
    return rows


def rating_data(section: Any) -> dict[str, Any]:
    if not section:
        return {}
    overall = None
    score = section.select_one(".realtime-rating-score .badge")
    if score:
        overall = soup_text(score)
    ratings = []
    for row in table_rows(section.select_one("table")):
        metric = row.get("Metric")
        if metric:
            ratings.append({
                "metric": metric,
                "rating": row.get("Metric Realtime Rating"),
                "a_plus_metric_rated_etf": row.get("A+ Metric Rated ETF"),
            })
    result: dict[str, Any] = {}
    if overall:
        result["overall_rating"] = overall
    if ratings:
        result["metrics"] = ratings
    overall_link = section.select_one(".a-plus-rating a[href*='/etf/']")
    if overall_link:
        result["a_plus_overall_rated_etf"] = soup_text(overall_link)
    return result


def esg_data(section: Any) -> dict[str, Any]:
    if not section:
        return {}
    text = soup_text(section)
    result: dict[str, Any] = {}
    summary_patterns = {
        "esg_score": r"([\d.]+/10)\s+ESG Score",
        "global_percentile": r"([\d.]+%)\s+Global Percentile",
        "peer_percentile": r"([\d.]+%)\s+Peer Percentile",
    }
    for key, pattern in summary_patterns.items():
        match = re.search(pattern, text, re.I)
        if match:
            result[key] = match.group(1)

    submetrics = []
    seen_esg = {}
    for item in section.select("li"):
        row = item.select_one(".esg-colum-row")
        if not row:
            continue
        label_node = row.find("span")
        value_node = row.select_one(".pull-right")
        hint_node = item.select_one(".esg-hint")
        label = soup_text(label_node)
        value = soup_text(value_node)
        if not label or not value:
            continue
        group = None
        parent = item.parent
        if parent:
            group_node = parent.find_previous("div", class_="weak")
            if group_node:
                group = soup_text(group_node)
        entry = {
            "group": group,
            "metric": label,
            "value": value,
            "interpretation": soup_text(hint_node) or None,
        }
        key = (entry["metric"], entry["value"], entry["interpretation"])
        if key not in seen_esg or (seen_esg[key]["group"] is None and group):
            seen_esg[key] = entry
    submetrics = list(seen_esg.values())
    if submetrics:
        result["submetrics"] = submetrics
    return result


def technical_metrics(section: Any) -> dict[str, Any]:
    if not section:
        return {}
    summary = two_column_table(section.select_one("table"))
    indicators: dict[str, dict[str, str]] = {}
    for block in section.select(".col-md-6.col-xs-12"):
        heading = block.find(["h3", "h4"])
        group = normalize_label(soup_text(heading)) if heading else None
        if not group or group in {"spy_technicals", "volatility_analysis"}:
            continue
        values = {}
        for value_node in block.select("span.pull-right"):
            label_node = value_node.find_previous_sibling("span")
            label = soup_text(label_node)
            value = soup_text(value_node)
            if label and value:
                values[normalize_label(label)] = value
        if values:
            indicators[group] = values
    result: dict[str, Any] = {}
    if summary:
        result["volatility"] = summary
    if indicators:
        result["indicators"] = indicators
    return result


def chart_tables(section: Any) -> dict[str, list[dict[str, str]]]:
    charts = {}
    if not section:
        return charts
    for table in section.select("table.chart"):
        title = table.get("data-title") or ""
        title = clean_text(re.sub(r"<br\s*/?>", " ", html_lib.unescape(title), flags=re.I))
        if not title:
            headers = [soup_text(th) for th in table.select("thead th")]
            title = headers[0] if headers else "chart"
        rows = table_rows(table)
        if rows:
            key = normalize_label(title)
            if key in charts:
                key = f"{key}_{len(charts) + 1}"
            charts[key] = rows
    return charts


def parse_structured_page(html: str) -> dict[str, Any]:
    if BeautifulSoup is None:
        return {}
    soup = BeautifulSoup(html, "html.parser")
    structured: dict[str, Any] = {}

    h1 = soup.find("h1")
    if h1:
        structured["name"] = soup_text(h1)
    ticker = (structured.get("name", "").split() or [""])[0]

    overview = soup.select_one("#overview")
    if overview:
        vitals_heading = overview.find(["h3", "h4"], string=re.compile(r"Vitals", re.I))
        vitals_block = vitals_heading.find_next("div", class_="ticker-assets") if vitals_heading else None
        vitals = parse_labeled_rows(vitals_block)
        structured.update({
            "issuer": vitals.get("issuer"),
            "brand": vitals.get("brand"),
            "structure": vitals.get("structure"),
            "expense_ratio": vitals.get("expense_ratio"),
            "home_page": vitals.get("etf_home_page"),
            "inception": vitals.get("inception"),
            "index_tracked": vitals.get("index_tracked"),
        })

        themes_heading = overview.find(["h3", "h4"], string=re.compile(r"ETF Database Themes", re.I))
        themes_block = themes_heading.find_next("div", class_="ticker-assets") if themes_heading else None
        themes = parse_labeled_rows(themes_block)
        if themes:
            structured["category"] = themes.get("category")
            structured["etf_database_themes"] = themes

        factset = soup.select_one("#factset-classification table")
        factset_rows = table_rows(factset)
        if factset_rows:
            structured["factset_classifications"] = {
                next(iter(row.values())): list(row.values())[-1]
                for row in factset_rows
                if len(row) >= 2
            }

        analyst = soup.select_one("#analyst-report #full-content") or soup.select_one("#analyst-report #truncated-content")
        if analyst:
            structured["analyst_report_text"] = soup_text(analyst)

    holdings_heading = None
    for candidate in soup.find_all(["h2", "h3", "h4"]):
        if re.search(r"\bTop 15 Holdings\b", soup_text(candidate), re.I):
            holdings_heading = candidate
            break
    holdings_table = holdings_heading.find_next("table") if holdings_heading else None
    holdings = table_rows(holdings_table)
    if holdings:
        structured["top_15_holdings"] = holdings[:15]

    holdings_comparison = soup.select_one("#holdings-table")
    comparison_rows = table_rows(holdings_comparison)
    if comparison_rows:
        structured["holdings_comparison"] = [
            {
                "metric": row.get("") or row.get("column_1"),
                "fund_value": row.get(ticker),
                "category_average": row.get("ETF Database Category Average"),
            }
            for row in comparison_rows
        ]

    performance_table = soup.select_one("#performance_tab table")
    performance = performance_rows(performance_table, ticker)
    if performance:
        structured["performance"] = performance

    valuation = valuation_metrics(soup.select_one("#valuation"), ticker)
    if valuation:
        structured["valuation"] = valuation

    dividend = metric_comparison_rows(soup.select_one("#dividend table"), ticker, "fund_value")
    if dividend:
        structured["dividend"] = dividend

    fund_flows = regex_metrics(soup_text(soup.select_one("#fund-flows_tab")), "Net Flows")
    if fund_flows:
        structured["fund_flows"] = fund_flows

    aum_influence = regex_metrics(soup_text(soup.select_one("#price-vs-flows_tab")), "Net AUM Change")
    if aum_influence:
        structured["aum_influence"] = aum_influence

    ratings = rating_data(soup.select_one("#realtime-rating_tab"))
    if ratings:
        structured["realtime_rating"] = ratings

    expense = expense_metrics(soup.select_one("#expense_tab"), ticker)
    if expense:
        structured["expenses_and_fees"] = expense

    tax = two_column_table(soup.select_one("#expense_tab table"))
    if tax:
        structured["tax_analysis"] = tax

    esg = esg_data(soup.select_one("#esg_tab"))
    if esg:
        structured["esg_summary_submetrics"] = esg

    technicals = technical_metrics(soup.select_one("#technicals_tab"))
    if technicals:
        structured["trading_data"] = {**(structured.get("trading_data") or {}), "technicals": technicals}

    charts = chart_tables(soup.select_one("#charts_tab"))
    if charts:
        structured["holdings_analysis_charts"] = charts

    return {key: value for key, value in structured.items() if value not in (None, "", [], {})}


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
    record.update(parse_structured_page(html))
    return record


def cached_html_path(raw_dir: Path, ticker: str) -> Path | None:
    matches = sorted(raw_dir.glob(f"{ticker}_*.html"))
    return matches[-1] if matches else None


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
    parser.add_argument("--diagnostics", action="store_true", help="check access only; do not fetch ETF detail pages")
    parser.add_argument("--parse-cache", action="store_true", help="rebuild outputs from cached raw HTML without network access")
    args = parser.parse_args()
    if args.diagnostics:
        return run_diagnostics(args)
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
    existing_records = load_existing_records(jsonl_path)
    records: list[dict[str, Any]] = [existing_records[ticker] for ticker in tickers if ticker in completed and ticker in existing_records]
    attempted = succeeded = skipped = failed = 0

    if args.parse_cache:
        records = []
        for ticker in tickers:
            cache_path = cached_html_path(raw_dir, ticker)
            if not cache_path:
                failed += 1
                print(f"no cached HTML for {ticker}", file=sys.stderr)
                continue
            html = cache_path.read_text(encoding="utf-8", errors="replace")
            records.append(parse_etf_page(ticker, f"{BASE_URL}/etf/{ticker}/", html, "cached"))
            succeeded += 1
        write_outputs(records, jsonl_path, csv_path)
        print(json.dumps({"attempted": 0, "succeeded": succeeded, "skipped": 0, "failed": failed, "outputs": [str(jsonl_path), str(csv_path)], "source": "cache"}, indent=2))
        return 0 if failed == 0 else 1

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
