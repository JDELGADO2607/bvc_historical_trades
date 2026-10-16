"""
bvc_historical_trades / supabase_sync.py — Supabase Integration Module.

Handles automated syncing of extracted BVC tick-by-tick trades to Supabase:
1. Direct relational batch insertion into the `bvc_trades` PostgreSQL table.
2. File archival into Supabase Storage bucket (`bvc-trades-archive`).

Uses standard library HTTP requests to work with zero external dependencies.
"""

from __future__ import annotations

import json
import logging
import os
import ssl
import urllib.request
from typing import List, Optional

logger = logging.getLogger("bvc_supabase")

SUPABASE_URL = os.getenv("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")
SUPABASE_TABLE = os.getenv("SUPABASE_TABLE", "bvc_trades")
SUPABASE_BUCKET = os.getenv("SUPABASE_BUCKET", "bvc-trades-archive")

SQL_SCHEMA = """
-- Run this in your Supabase SQL Editor:
CREATE TABLE IF NOT EXISTS bvc_trades (
    trade_id TEXT PRIMARY KEY,
    ticker TEXT NOT NULL,
    trade_date DATE NOT NULL,
    trade_time TIME NOT NULL,
    price NUMERIC(14, 4) NOT NULL,
    quantity NUMERIC(16, 2) NOT NULL,
    total_value NUMERIC(18, 2) NOT NULL,
    operation_type TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_bvc_trades_date_ticker ON bvc_trades(trade_date, ticker);
CREATE INDEX IF NOT EXISTS idx_bvc_trades_ticker ON bvc_trades(ticker);
"""


def is_configured() -> bool:
    """Returns True if Supabase environment variables are provided."""
    return bool(SUPABASE_URL and SUPABASE_KEY)


def insert_trades_table(trades: List[dict], batch_size: int = 500) -> int:
    """
    Inserts a list of trades into Supabase PostgreSQL table via PostgREST API.
    Upserts using on_conflict=trade_id to prevent duplicates.
    """
    if not is_configured():
        logger.warning("Supabase credentials (SUPABASE_URL, SUPABASE_KEY) not set. Skipping DB insert.")
        return 0

    endpoint = f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}?on_conflict=trade_id"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates"
    }

    ctx = ssl.create_default_context()
    inserted_count = 0

    # Batch insert to avoid payload limits
    for i in range(0, len(trades), batch_size):
        chunk = trades[i:i + batch_size]
        body = json.dumps(chunk).encode("utf-8")
        req = urllib.request.Request(endpoint, data=body, headers=headers, method="POST")

        try:
            with urllib.request.urlopen(req, context=ctx, timeout=15.0) as resp:
                if resp.status in (200, 201):
                    inserted_count += len(chunk)
        except Exception as e:
            logger.error(f"Failed to insert batch {i}..{i + len(chunk)} to Supabase: {e}")

    logger.info(f"Successfully upserted {inserted_count}/{len(trades)} trades to Supabase table '{SUPABASE_TABLE}'.")
    return inserted_count


def upload_storage_file(filepath: str, destination_name: Optional[str] = None) -> bool:
    """
    Uploads a local Parquet / CSV file to Supabase Storage.
    """
    if not is_configured():
        logger.warning("Supabase credentials not set. Skipping Storage upload.")
        return False

    if not os.path.exists(filepath):
        logger.error(f"File not found: {filepath}")
        return False

    dest = destination_name or os.path.basename(filepath)
    endpoint = f"{SUPABASE_URL}/storage/v1/object/{SUPABASE_BUCKET}/{dest}"

    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/octet-stream",
        "x-upsert": "true"
    }

    ctx = ssl.create_default_context()
    with open(filepath, "rb") as f:
        file_bytes = f.read()

    req = urllib.request.Request(endpoint, data=file_bytes, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, context=ctx, timeout=30.0) as resp:
            if resp.status in (200, 201):
                logger.info(f"Successfully uploaded {filepath} to Supabase Storage: {SUPABASE_BUCKET}/{dest}")
                return True
    except Exception as e:
        logger.error(f"Failed to upload {filepath} to Supabase Storage: {e}")
        return False

    return False


def upload_trades(trades: List[dict], filepath: Optional[str] = None) -> None:
    """Convenience function to perform both DB upsert and Storage upload."""
    if trades:
        insert_trades_table(trades)
    if filepath and os.path.exists(filepath):
        upload_storage_file(filepath)


if __name__ == "__main__":
    print("=== SUPABASE SQL SCHEMA ===")
    print(SQL_SCHEMA)
