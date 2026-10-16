# BVC Historical Trades Extractor & Cloud Sync

Automated high-performance batch pipeline for extracting, transforming, and syncing tick-by-tick trading data (Time & Sales) from the **BVC (Bolsa de Valores de Colombia)**.

This repository operates completely independently of the live streaming WebSocket scraper. It connects directly to BVC's REST backend (`WebTerminalCoreDB/DatosNegociaciones`), extracting official executed transactions without browser overhead.

---

## Architecture Overview

```
                      ┌──────────────────────────────────────┐
                      │   BVC REST API (WebTerminalCoreDB)   │
                      └──────────────────┬───────────────────┘
                                         │
                        Every Weekday at 7:00 PM COT
                                         │
                                         ▼
                      ┌──────────────────────────────────────┐
                      │    GitHub Actions Runner (Cron)      │
                      │         (15 seconds execution)       │
                      └──────────────────┬───────────────────┘
                                         │
                    ┌────────────────────┴────────────────────┐
                    ▼                                         ▼
      ┌───────────────────────────┐             ┌───────────────────────────┐
      │   Supabase PostgreSQL     │             │    Daily Parquet File     │
      │   (Table: `bvc_trades`)   │             │   (Storage & Artifacts)   │
      └───────────────────────────┘             └───────────────────────────┘
```

---

## Quickstart (Local Usage)

### 1. Requirements
Python 3.10+ and `pyarrow` (for Parquet export):
```bash
pip install -r requirements.txt
```

### 2. Execution Modes

```bash
# Extract today's trading session (or last business day)
python extractor.py --daily

# Extract a specific date
python extractor.py --date 2026-10-15

# Extract only Equities (Renta Variable) or Currencies (Divisas)
python extractor.py --daily --group renta_variable
python extractor.py --daily --group divisas

# Export format: parquet (default), csv, or json
python extractor.py --daily --format csv

# BACKFILL: Extract the entire 90-day retention window (~July 14 to today)
python extractor.py --backfill --format parquet

# Custom date range
python extractor.py --start 2026-09-01 --end 2026-09-30
```

---

## Data Schema (`bvc_trades`)

Each extracted trade contains the complete execution record:

| Field | Type | Description | Example |
|:---|:---:|:---|:---|
| `trade_id` | `TEXT PRIMARY KEY` | Official BVC operation number (Folio) | `"112014539"` |
| `ticker` | `TEXT` | Market instrument code | `"QFECO01"`, `"QFCOR02"` |
| `trade_date` | `DATE` | Execution date (`YYYY-MM-DD`) | `"2026-10-15"` |
| `trade_time` | `TIME` | Execution time (`HH:MM:SS`) | `"17:00:00"` |
| `price` | `NUMERIC(14,4)` | Executed trade price in COP | `2850.0` |
| `quantity` | `NUMERIC(16,2)` | Number of shares transacted | `21746.0` |
| `total_value` | `NUMERIC(18,2)` | Cash value (`price × quantity`) | `62016100.0` |
| `operation_type` | `TEXT` | Operation category flag | `"G"` |

---

## Cloud Deployment (GitHub Actions + Supabase)

### 1. Supabase Database Setup
1. Create a free project at [supabase.com](https://supabase.com).
2. Open the **SQL Editor** in your Supabase dashboard and run:
   ```sql
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
   ```

3. *(Optional Storage)*: Create a public or private bucket named `bvc-trades-archive`.

### 2. Configure GitHub Secrets
In your GitHub repository, navigate to **Settings > Secrets and variables > Actions** and add:

| Secret Name | Value |
|:---|:---|
| `BVC_USER` | `BVCROSARIO4` (or your user) |
| `BVC_PASS` | `BvcUR2023*` (or your password) |
| `SUPABASE_URL` | `https://<your-project>.supabase.co` |
| `SUPABASE_KEY` | Your Supabase `anon` or `service_role` key |

### 3. Automated Schedule
The workflow [`.github/workflows/daily_trades_cron.yml`](.github/workflows/daily_trades_cron.yml) runs automatically:
* **Schedule**: Every weekday at `00:00 UTC` (**7:00 PM Colombia Time / COT**).
* **Execution Time**: ~15 to 20 seconds.
* **Monthly Usage**: ~6 minutes / month (well within GitHub's 2,000 free minutes/month limit).
* **Manual Run**: Can also be triggered anytime via the **Actions** tab in GitHub by clicking **Run workflow**.
