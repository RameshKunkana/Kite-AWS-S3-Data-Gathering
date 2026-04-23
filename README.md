# DriFy Market Data Pipeline

This repository contains the DriFy ingestion service for collecting live market ticks from Zerodha Kite, normalizing them, and delivering them into AWS for downstream storage and analysis.

The current system is designed around a basket-based market feed:
- NIFTY spot
- SENSEX spot
- INDIA VIX
- front-month NIFTY future
- front-month SENSEX future
- NIFTY option strikes around ATM
- SENSEX option strikes around ATM

The service runs as a long-lived Python process, typically on EC2, and publishes normalized events into Amazon Kinesis Data Streams. Amazon Data Firehose reads from Kinesis and writes Parquet files into S3 using date and instrument-based partitions.

## What This Repo Does

At a high level, the application:
- loads runtime settings from `.env`
- connects to Kite REST APIs to build an instrument basket
- optionally captures pre-market reference prices
- supports late starts during market hours using live quote fallback
- opens a Kite WebSocket for the selected basket
- converts raw ticks into a normalized event schema
- pushes those events into Kinesis
- relies on Firehose to batch and land files into S3

The current architecture is intended to support later use cases such as:
- intraday replay
- signal research
- backtesting
- Athena / Glue based querying
- downstream model training or feature generation

## Repository Structure

```text
src/drify_ingestor/
  config.py                 Environment loading and runtime settings
  instrument_selection.py   Basket construction and instrument filtering
  kite_client.py            Kite WebSocket collectors and streamers
  kinesis.py                Kinesis publishing wrapper
  logging_config.py         Logging setup
  main.py                   Application entrypoint and startup orchestration
  models.py                 Tick normalization and event schema

infra/
  firehose_parquet_stack.yaml   AWS stack for Kinesis, Firehose, S3, Glue
```

## End-to-End Flow

### 1. Startup

The app starts from:

```powershell
python -m drify_ingestor.main
```

`main.py` decides how to start based on current IST market time:
- before pre-market: wait and use pre-market capture
- between pre-market start and basket cutoff: capture what is available, then fall back if needed
- after basket cutoff but before market close: skip pre-market capture and build immediately from live quotes
- after market close: exit cleanly

### 2. Basket construction

`instrument_selection.py`:
- loads Kite instruments master
- identifies spot/index instruments
- finds the nearest live future contracts
- finds the nearest live option expiry
- selects strikes in a configurable ATM window

### 3. Tick ingestion

`kite_client.py`:
- collects pre-market reference prices when needed
- starts a live WebSocket stream for the full market basket
- routes each tick into the application handler

### 4. Tick normalization

`models.py` converts raw Kite ticks into a normalized `TickEvent`.

Each event includes:
- market timestamps
- instrument metadata
- option/future context
- core market fields such as LTP, volume, change, OI, and OHLC
- additional raw tick context such as quantities, depth, and a raw tick JSON snapshot
- websocket mode, currently standardized as `full`
- an `instrument_folder` field used by Firehose for S3 partition routing

### 5. AWS delivery

`kinesis.py` publishes each normalized event to Kinesis Data Streams.

Firehose then:
- reads records from Kinesis
- dynamically partitions by instrument folder
- converts JSON to Parquet
- writes files into S3

## AWS Architecture

The current infrastructure template is in:

[`infra/firehose_parquet_stack.yaml`](./infra/firehose_parquet_stack.yaml)

It provisions:
- an S3 bucket for market data
- a Kinesis Data Stream
- a Firehose delivery stream
- a Glue database and table
- IAM role for Firehose
- CloudWatch log group and stream for Firehose delivery logs

### Delivery path

```text
EC2 app -> Kinesis Data Streams -> Firehose -> S3 (Parquet)
```

### S3 layout

Files are written under:

```text
ticks/year=YYYY/month=MM/day=DD/<instrument_folder>/
```

Examples:

```text
ticks/year=2026/month=04/day=21/NIFTY_SPOT/
ticks/year=2026/month=04/day=21/SENSEX_SPOT/
ticks/year=2026/month=04/day=21/INDIA_VIX/
ticks/year=2026/month=04/day=21/NIFTY_24APR_FUT/
ticks/year=2026/month=04/day=21/NIFTY_24APR_22500_CE/
ticks/year=2026/month=04/day=21/SENSEX_24APR_74200_PE/
```

Important:
- Firehose controls the final object filenames
- this repo controls the partition folder path via `instrument_folder`
- multiple Firehose files can exist under the same instrument folder, which is expected

### Glue partitions

The Glue table is aligned to the S3 structure with partition keys:
- `year`
- `month`
- `day`
- `instrument_folder`

### Timezone behavior

Application time logic is based on IST.

Firehose S3 prefixes are also configured for:
- `Asia/Kolkata`

This avoids UTC-vs-IST day partition mismatches.

## Normalized Event Shape

A representative event looks like:

```json
{
  "event_time": "2026-04-15T09:45:10.123000+05:30",
  "ingestion_time": "2026-04-15T09:45:10.456000+05:30",
  "exchange_timestamp": "2026-04-15T09:45:10+05:30",
  "last_trade_time": "2026-04-15T09:45:08+05:30",
  "instrument_token": 123456,
  "tradingsymbol": "NIFTY24APR22500CE",
  "exchange": "NFO",
  "segment": "NFO-OPT",
  "name": "NIFTY",
  "underlying": "NIFTY",
  "basket_role": "option",
  "instrument_folder": "NIFTY_24APR_22500_CE",
  "instrument_type": "CE",
  "option_type": "CE",
  "expiry": "2026-04-24",
  "strike": 22500.0,
  "lot_size": 50,
  "tick_size": 0.05,
  "mode": "full",
  "last_price": 142.35,
  "last_quantity": 50,
  "average_price": 140.11,
  "volume": 10240,
  "buy_quantity": 2300,
  "sell_quantity": 2500,
  "change": 4.2,
  "oi": 182500,
  "oi_day_high": 185000,
  "oi_day_low": 176000,
  "open_price": 138.0,
  "high_price": 145.8,
  "low_price": 136.2,
  "close_price": 136.6,
  "depth_buy": [],
  "depth_sell": [],
  "raw_tick_json": "{\"instrument_token\":123456,\"last_price\":142.35}"
}
```

## Configuration

Settings are loaded from `.env` through `python-dotenv`.

### Required

- `KITE_API_KEY`
- `KITE_ACCESS_TOKEN` or `KITE_ACCESS_TOKEN_FILE`
- `AWS_REGION`
- `KINESIS_STREAM_NAME`

### Common optional settings

- `KINESIS_PARTITION_KEY`
- `LOG_LEVEL`
- `OPTION_STRIKE_WINDOW`
- `NIFTY_STRIKE_STEP`
- `SENSEX_STRIKE_STEP`
- `BASKET_REFERENCE_MODE`
- `KITE_INDEX_MODE`
- `KITE_DERIVATIVE_MODE`
- `KITE_RECONNECT_MAX_TRIES`
- `KITE_RECONNECT_MAX_DELAY`
- `KITE_CONNECT_TIMEOUT`
- `KINESIS_PUBLISH_RETRIES`
- `KINESIS_PUBLISH_BACKOFF_SECONDS`
- `PREMARKET_START_TIME`
- `PREMARKET_END_TIME`
- `BASKET_SELECTION_TIME`
- `MARKET_START_TIME`
- `MARKET_END_TIME`

See:

[` .env.example`](./.env.example)

## Local Development

### Setup

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -e .
Copy-Item .env.example .env
```

Then populate `.env` with real values and run:

```powershell
python -m drify_ingestor.main
```

### Fast local smoke test

If you want to test startup behavior without waiting for the real market window, temporarily use permissive market times in `.env`, for example:

```env
PREMARKET_START_TIME=00:00
PREMARKET_END_TIME=00:01
BASKET_SELECTION_TIME=00:02
MARKET_START_TIME=00:03
MARKET_END_TIME=23:59
```

## EC2 Deployment Notes

Typical production deployment:
- EC2 hosts the Python process
- instance role is used for AWS access
- no long-lived AWS access keys are stored on the machine
- Kite secrets live in `.env` or a token file on the instance

On the EC2 box:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
python -m drify_ingestor.main
```

## Operational Notes

### Late-start handling

If the app starts after pre-market cutoff but before market close, it does not fail. It falls back to current live quotes for basket construction and starts streaming immediately.

### Basket completeness

The app warns when expected option contracts are missing for the configured strike window. This can happen due to live market availability or symbol differences in Kite instruments.

### Kinesis publishing

Publishing is synchronous per tick today. This is simple and reliable for the current phase, but may need batching later if throughput requirements increase materially.

### End-of-day compaction

Raw Firehose output remains under:

```text
ticks/year=YYYY/month=MM/day=DD/<instrument_folder>/
```

To create one compacted parquet file per instrument for a trading day, use:

```powershell
python -m drify_ingestor.compaction --bucket drify-market-data --date 2026-04-22
```

This writes curated files to:

```text
curated/year=YYYY/month=MM/day=DD/<instrument_folder>/<instrument_folder>.parquet
```

You can compact a single instrument only:

```powershell
python -m drify_ingestor.compaction --bucket drify-market-data --date 2026-04-22 --instrument-folder NIFTY_24APR_22500_CE
```

Important:
- raw Firehose files are left untouched
- curated files overwrite the same curated output key for repeatable reruns
- current compaction concatenates source parquet files in key order; global re-sorting is not yet part of the utility

### Firehose / Parquet validation

After infra deployment, always validate:
- records are visible in Kinesis
- Firehose is consuming the stream
- S3 partitions appear as expected
- Parquet conversion succeeds
- instrument folders are named correctly

## Suggested Validation Checklist

Before calling a deployment healthy:

1. Confirm EC2 can reach Kite and AWS APIs.
2. Confirm the app starts without config errors.
3. Confirm the market basket is built successfully.
4. Confirm live ticks are being published to Kinesis.
5. Confirm Firehose writes files into:
   `ticks/year=YYYY/month=MM/day=DD/<instrument_folder>/`
6. Confirm sample files can be queried or inspected downstream.

## Current Limitations / Future Work

- Firehose controls file names; only folder paths are controlled here
- partition registration/query optimization may still need crawler or Athena-side operational setup
- schema evolution should be managed carefully when event fields change
- direct support for curated downstream datasets is not yet implemented
- backtesting-oriented compaction or post-processing layers are not yet part of this repo

## Key Files to Read First

If you are new to this repo, start with:
- [`src/drify_ingestor/main.py`](./src/drify_ingestor/main.py)
- [`src/drify_ingestor/instrument_selection.py`](./src/drify_ingestor/instrument_selection.py)
- [`src/drify_ingestor/models.py`](./src/drify_ingestor/models.py)
- [`infra/firehose_parquet_stack.yaml`](./infra/firehose_parquet_stack.yaml)

These four files explain most of the runtime behavior and AWS integration.
