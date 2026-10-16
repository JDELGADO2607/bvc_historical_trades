"""
bvc_historical_trades / extractor.py — BVC Historical Trades Batch Extractor.

Extracts tick-by-tick trading data (Time & Sales) directly from the BVC
WebTerminalCoreDB endpoint without requiring a browser or persistent socket.

Designed for automated daily scheduled runs (GitHub Actions) and 90-day backfills.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import ssl
import sys
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger("bvc_extractor")

# ---------------------------------------------------------------------------
# Configuration & Credentials
# ---------------------------------------------------------------------------

def load_dotenv(path: str = ".env") -> None:
    """Loads key-value pairs from .env into os.environ if not already set."""
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    k, v = k.strip(), v.strip().strip("'\"")
                    if k not in os.environ:
                        os.environ[k] = v

load_dotenv()

AUTH_URL = "https://suscripcion.e-bvc.com.co/GestorUsuarios/GestorUsuarios/Login/Login"
DB_URL = "https://terminal.e-bvc.com.co/WebTerminalCoreDB/DatosNegociaciones"

DEFAULT_USER = os.getenv("BVC_USER", "BVCROSARIO4")
DEFAULT_PASS = os.getenv("BVC_PASS", "BvcUR2023*")

DATA_DIR = os.getenv("DATA_DIR", "data")
RETENTION_START_DATE = "20260714"  # 14 de Julio de 2026 (límite histórico BVC)


# ---------------------------------------------------------------------------
# Authentication
# ---------------------------------------------------------------------------

def authenticate(username: str = DEFAULT_USER, password: str = DEFAULT_PASS) -> str:
    """Authenticates with BVC and returns the active Session token."""
    data = urllib.parse.urlencode({
        "Usuario": username,
        "Password": password,
        "Producto": "BVCHT",
        "Idioma": "CO",
        "Version": "3.2.6"
    }).encode("utf-8")

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Origin": "https://terminal.e-bvc.com.co",
        "Referer": "https://terminal.e-bvc.com.co/"
    }

    ctx = ssl.create_default_context()
    req = urllib.request.Request(AUTH_URL, data=data, headers=headers, method="POST")

    with urllib.request.urlopen(req, context=ctx, timeout=10.0) as resp:
        res_json = json.loads(resp.read().decode("utf-8"))

    if res_json.get("Error") != 0 or "Data" not in res_json:
        raise PermissionError(f"BVC Login rejected: {res_json}")

    session_id = res_json["Data"]["Sesion"]
    logger.info(f"Authenticated successfully as {username} (Session: {session_id[:8]}...)")
    return session_id


# ---------------------------------------------------------------------------
# Core Tick Fetcher
# ---------------------------------------------------------------------------

def fetch_ticker_trades(
    session_id: str,
    ticker: str,
    date_str: str,
    max_records: str = "2000"
) -> List[dict]:
    """
    Fetches raw tick-by-tick trades for a single instrument on a given date.
    date_str must be in YYYYMMDD format.
    """
    payload = json.dumps({
        "Sesion": session_id,
        "TipoPeticion": "4",
        "Instrumentos": ticker,
        "Descriptores": "3710,3700,760,9738,9747,5029,3985,3990",
        "Fecha": date_str,
        "NumRegistros": max_records
    }).encode("utf-8")

    headers = {
        "Content-Type": "application/json; charset=UTF-8",
        "Origin": "https://terminal.e-bvc.com.co",
        "User-Agent": "Mozilla/5.0"
    }

    ctx = ssl.create_default_context()
    req = urllib.request.Request(DB_URL, data=payload, headers=headers, method="POST")

    try:
        with urllib.request.urlopen(req, context=ctx, timeout=10.0) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            raw_ticks = data.get("datos", {}).get("V", [])
            if not isinstance(raw_ticks, list):
                raw_ticks = [raw_ticks] if raw_ticks else []

            # Normalize raw fields
            normalized = []
            for t in raw_ticks:
                raw_date = t.get("D3990", date_str)
                raw_time = t.get("D3710", "000000").zfill(6)
                
                # Format date to YYYY-MM-DD
                formatted_date = f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:8]}" if len(raw_date) == 8 else raw_date
                # Format time to HH:MM:SS
                formatted_time = f"{raw_time[:2]}:{raw_time[2:4]}:{raw_time[4:6]}" if len(raw_time) == 6 else raw_time

                normalized.append({
                    "trade_id": str(t.get("D3985", "")),
                    "ticker": t.get("MV", ticker).strip(),
                    "trade_date": formatted_date,
                    "trade_time": formatted_time,
                    "price": float(t.get("D3700", 0.0) or 0.0),
                    "quantity": float(t.get("D760", 0.0) or 0.0),
                    "total_value": float(t.get("D9738", 0.0) or 0.0),
                    "operation_type": str(t.get("D5029", "")).strip(),
                })
            return normalized

    except Exception as e:
        logger.warning(f"Failed to fetch trades for {ticker} on {date_str}: {e}")
        return []


def extract_day(
    session_id: str,
    tickers: List[str],
    date_str: str,
    max_workers: int = 10
) -> List[dict]:
    """
    Concurrently extracts trades for all requested tickers for a specific date (YYYYMMDD).
    """
    logger.info(f"Extracting trades for {len(tickers)} instruments on date {date_str}...")
    all_trades: List[dict] = []
    active_count = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(fetch_ticker_trades, session_id, ticker, date_str): ticker
            for ticker in tickers
        }

        for future in as_completed(futures):
            ticker = futures[future]
            try:
                trades = future.result()
                if trades:
                    active_count += 1
                    all_trades.extend(trades)
            except Exception as e:
                logger.error(f"Error processing {ticker}: {e}")

    logger.info(f"Date {date_str} extraction complete: {len(all_trades)} trades across {active_count} active instruments.")
    return all_trades


# ---------------------------------------------------------------------------
# Exporters & Storage
# ---------------------------------------------------------------------------

def save_to_csv(trades: List[dict], filepath: str) -> None:
    """Exports trades list to a CSV file."""
    if not trades:
        logger.info(f"No trades to save for {filepath}.")
        return

    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    fieldnames = ["trade_id", "ticker", "trade_date", "trade_time", "price", "quantity", "total_value", "operation_type"]

    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(trades)

    logger.info(f"Saved {len(trades)} trades to CSV: {filepath} ({os.path.getsize(filepath) / 1024:.1f} KB)")


def save_to_parquet(trades: List[dict], filepath: str) -> bool:
    """
    Exports trades list to a compressed Parquet file using pyarrow.
    Returns True if successful, False if pyarrow is not installed.
    """
    if not trades:
        return True

    try:
        import pyarrow as pa
        import pyarrow.parquet as pq

        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        table = pa.Table.from_pylist(trades)
        pq.write_table(table, filepath, compression="snappy")
        logger.info(f"Saved {len(trades)} trades to Parquet: {filepath} ({os.path.getsize(filepath) / 1024:.1f} KB)")
        return True
    except ImportError:
        logger.warning("pyarrow is not installed. Falling back to CSV export.")
        csv_path = filepath.replace(".parquet", ".csv")
        save_to_csv(trades, csv_path)
        return False


def save_to_json(trades: List[dict], filepath: str) -> None:
    """Exports trades list to JSON."""
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(trades, f, indent=2, ensure_ascii=False)
    logger.info(f"Saved {len(trades)} trades to JSON: {filepath} ({os.path.getsize(filepath) / 1024:.1f} KB)")


# ---------------------------------------------------------------------------
# Catalog Loader
# ---------------------------------------------------------------------------

def load_tickers(catalog_path: str = "catalog.json", group: Optional[str] = None) -> List[str]:
    """Loads ticker symbols from catalog.json or group files."""
    if not os.path.exists(catalog_path):
        raise FileNotFoundError(f"Catalog file not found: {catalog_path}")

    with open(catalog_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if group and group != "all":
        group_items = data.get("groups", {}).get(group, [])
        if not group_items:
            # Check catalog_by_group/
            group_file = os.path.join("catalog_by_group", f"{group}.json")
            if os.path.exists(group_file):
                with open(group_file, "r", encoding="utf-8") as gf:
                    g_data = json.load(gf)
                    return [it["ticker"] for it in g_data.get("instruments", [])]
        return [it["ticker"] for it in group_items]

    return list(data.get("tickers", {}).keys())


# ---------------------------------------------------------------------------
# CLI & Execution Modes
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="BVC Historical Trades Extractor")
    parser.add_argument("--date", help="Specific date to extract (YYYY-MM-DD or YYYYMMDD)")
    parser.add_argument("--daily", action="store_true", help="Extract today's session (or latest market day)")
    parser.add_argument("--backfill", action="store_true", help="Extract the full 90-day retention window (~Jul 14 to today)")
    parser.add_argument("--start", help="Custom start date (YYYY-MM-DD)")
    parser.add_argument("--end", help="Custom end date (YYYY-MM-DD)")
    parser.add_argument("--group", choices=["renta_variable", "renta_fija", "divisas", "indices", "all"], default="all")
    parser.add_argument("--format", choices=["parquet", "csv", "json"], default="parquet")
    parser.add_argument("--out-dir", default=DATA_DIR, help="Destination directory for output files")
    parser.add_argument("--push-supabase", action="store_true", help="Upload data to Supabase after extraction")

    args = parser.parse_args()

    # 1. Load tickers
    tickers = load_tickers(group=args.group)
    logger.info(f"Loaded {len(tickers)} tickers for extraction (Group: {args.group})")

    # 2. Authenticate
    session_id = authenticate()

    # 3. Determine dates to extract
    dates_to_extract: List[datetime] = []
    today = datetime.now()

    if args.daily:
        # If Saturday or Sunday, rollback to Friday
        target_dt = today
        if target_dt.weekday() == 5:
            target_dt -= timedelta(days=1)
        elif target_dt.weekday() == 6:
            target_dt -= timedelta(days=2)
        dates_to_extract = [target_dt]

    elif args.date:
        clean = args.date.replace("-", "")
        dates_to_extract = [datetime.strptime(clean, "%Y%m%d")]

    elif args.backfill:
        start_dt = datetime.strptime(RETENTION_START_DATE, "%Y%m%d")
        curr = start_dt
        while curr <= today:
            if curr.weekday() not in (5, 6):  # Skip weekends
                dates_to_extract.append(curr)
            curr += timedelta(days=1)
        logger.info(f"Backfill mode: Queued {len(dates_to_extract)} trading days ({start_dt.strftime('%Y-%m-%d')} to {today.strftime('%Y-%m-%d')})")

    elif args.start and args.end:
        start_dt = datetime.strptime(args.start.replace("-", ""), "%Y%m%d")
        end_dt = datetime.strptime(args.end.replace("-", ""), "%Y%m%d")
        curr = start_dt
        while curr <= end_dt:
            if curr.weekday() not in (5, 6):
                dates_to_extract.append(curr)
            curr += timedelta(days=1)
    else:
        # Default to daily
        dates_to_extract = [today]

    # 4. Extraction loop
    total_extracted_trades = 0
    extracted_files = []

    for dt in dates_to_extract:
        d_str = dt.strftime("%Y%m%d")
        d_dashed = dt.strftime("%Y-%m-%d")

        trades = extract_day(session_id, tickers, d_str)
        if not trades:
            logger.info(f"Skipping file generation for {d_dashed} (0 trades).")
            continue

        total_extracted_trades += len(trades)
        out_filename = f"bvc_trades_{d_dashed}.{args.format}"
        out_filepath = os.path.join(args.out_dir, out_filename)

        if args.format == "parquet":
            save_to_parquet(trades, out_filepath)
        elif args.format == "csv":
            save_to_csv(trades, out_filepath)
        elif args.format == "json":
            save_to_json(trades, out_filepath)

        extracted_files.append((d_dashed, out_filepath, trades))

    logger.info(f"\n============================================================")
    logger.info(f"JOB COMPLETE: Extracted {total_extracted_trades:,} trades across {len(dates_to_extract)} days.")
    logger.info(f"============================================================")

    # 5. Optional Supabase upload
    if args.push_supabase:
        try:
            import supabase_sync
            for d_dashed, filepath, trades in extracted_files:
                logger.info(f"Pushing {d_dashed} trades to Supabase...")
                supabase_sync.upload_trades(trades, filepath)
        except ImportError:
            logger.warning("supabase_sync module not found or missing credentials.")
        except Exception as e:
            logger.error(f"Failed to upload to Supabase: {e}")


if __name__ == "__main__":
    main()
