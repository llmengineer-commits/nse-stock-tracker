# NSE Kenya Stock Tracker

A real-time streaming data pipeline that tracks the Nairobi Securities Exchange (NSE)
— built as the DE Zoomcamp 2026 capstone project by **Ian Nyaga** of Nairobi, Kenya.

The pipeline polls 20 NSE equities via yfinance every 5 minutes during market hours,
streams them through Redpanda and PyFlink, stores everything in ClickHouse, and
surfaces daily summaries in Looker Studio — fully orchestrated by Kestra.

---

## Architecture

```
yfinance (20 NSE tickers)
        │
        │  poll every 5 min (09:00–15:30 EAT, Mon–Fri)
        ▼
  ┌─────────────┐
  │  Redpanda   │  Kafka-compatible broker
  │  nse.raw.   │  topic: nse.raw.ticks
  │  ticks      │
  └──────┬──────┘
         │
         ▼
  ┌─────────────┐
  │  PyFlink    │  two sinks in parallel:
  │             │   1. raw tick → ClickHouse nse.raw_ticks
  │             │   2. 5-min OHLCV window → ClickHouse nse.ohlcv_5min
  └──────┬──────┘
         │
         ▼
  ┌─────────────┐
  │ ClickHouse  │  columnar warehouse (replaces BigQuery)
  │  database:  │  nse.raw_ticks
  │    nse      │  nse.ohlcv_5min
  └──────┬──────┘
         │  (triggered by Kestra at 15:35 EAT)
         ▼
  ┌─────────────┐
  │    dbt      │  staging → marts
  │             │  fct_nse_daily_summary
  │             │  fct_nse_top_movers
  └──────┬──────┘
         │
         ▼
  ┌─────────────┐
  │   Looker    │  live dashboard connecting to ClickHouse HTTP
  │   Studio    │  via the ClickHouse JDBC connector
  └─────────────┘

  ┌─────────────┐
  │   Kestra    │  orchestrates the whole thing:
  │             │  - health checks at 09:00 EAT
  │             │  - triggers dbt at 15:35 EAT
  │             │  - alerts on failure
  └─────────────┘
```

## Tech Stack

| Layer | Tool | Why |
|---|---|---|
| Ingestion | yfinance + kafka-python | Lightweight, no API key needed for NSE data |
| Streaming | Redpanda | Kafka-compatible, single-binary, lower overhead than full Kafka |
| Processing | PyFlink | Required for the zoomcamp; handles windowed aggregation cleanly |
| Warehouse | ClickHouse | Columnar, blazing fast on time-series, Looker Studio compatible |
| Transform | dbt + dbt-clickhouse | Same workflow as the course but pointing at ClickHouse |
| Orchestration | Kestra | Course Module 2 coverage; manages scheduling and failure handling |
| Visualisation | Looker Studio | Connects directly to ClickHouse via HTTP |
| Infrastructure | Docker Compose | Everything runs locally; no cloud spend required |

---

## Prerequisites

- Docker Desktop (Windows/Mac) or Docker Engine (Linux) — at least 6 GB RAM allocated
- Docker Compose v2
- Git

That's it. No cloud accounts, no API keys, no GCP credits needed.

---

## Quickstart

### 1. Clone and configure

```bash
git clone https://github.com/YOUR_USERNAME/nse-stock-tracker.git
cd nse-stock-tracker

# Copy the example env file and fill in passwords
cp .env.example .env
# The defaults in .env.example work fine for local dev
```

### 2. Start all services

```bash
docker compose up -d
```

This starts Redpanda, ClickHouse, Flink, the producer, and Kestra.
Give it about 60 seconds for everything to be healthy — ClickHouse is the
slowest to initialise.

Check that everything is up:

```bash
bash scripts/check_pipeline.sh
```

You should see green checkmarks next to all five services.

### 3. Submit the Flink job

```bash
bash scripts/submit_flink_job.sh
```

This submits `nse_flink_job.py` to the Flink cluster. You can watch it in the
Flink UI at **http://localhost:8081** — look for a job called
"NSE Kenya Stock Tracker" with status RUNNING.

### 4. Verify ticks are flowing

Open the Redpanda Console at **http://localhost:8080**, go to Topics →
`nse.raw.ticks` → Messages. You should see JSON messages appearing every
5 minutes (or immediately if the NSE is currently open).

To see data in ClickHouse directly:

```bash
docker exec nse_clickhouse clickhouse-client \
  --user nse_user --password nse_secret_2026 \
  --database nse \
  --query "SELECT ticker, price, fetched_at FROM raw_ticks LIMIT 10"
```

### 5. Run dbt manually (first time)

Kestra runs dbt automatically after market close, but you can run it
yourself any time:

```bash
docker compose run --rm dbt dbt run
docker compose run --rm dbt dbt test
```

### 6. Connect Looker Studio

1. Go to [lookerstudio.google.com](https://lookerstudio.google.com)
2. Create new report → Add data → ClickHouse (community connector)
3. Host: your machine's public IP or localhost (use ngrok if needed)
4. Port: 8123, Database: nse, Username: nse_user
5. Connect to the `fct_nse_daily_summary` and `fct_nse_top_movers` tables

---

## Service URLs

| Service | URL | Credentials |
|---|---|---|
| Redpanda Console | http://localhost:8080 | none |
| Flink Web UI | http://localhost:8081 | none |
| Kestra UI | http://localhost:8082 | none |
| ClickHouse HTTP | http://localhost:8123 | nse_user / (from .env) |

---

## NSE Tickers Tracked

All 20 tickers are the most liquid counters on the NSE bourse.
Yahoo Finance uses the `.NR` suffix for Nairobi equities.

| Ticker | Company |
|---|---|
| SCOM.NR | Safaricom PLC |
| EQTY.NR | Equity Group Holdings |
| KCB.NR | KCB Group |
| EABL.NR | East African Breweries |
| BAT.NR | BAT Kenya |
| COOP.NR | Co-operative Bank |
| ABSA.NR | Absa Bank Kenya |
| NCBA.NR | NCBA Group |
| SBIC.NR | Stanbic Holdings |
| DTB.NR | Diamond Trust Bank |
| SCBK.NR | Standard Chartered Kenya |
| IMM.NR | I&M Group |
| KEGN.NR | KenGen |
| KPLC.NR | Kenya Power & Lighting |
| SASN.NR | Sasini PLC |
| BAMB.NR | Bamburi Cement |
| CTUM.NR | Centum Investment |
| CABL.NR | East African Cables |
| NMG.NR | Nation Media Group |
| TCL.NR | TotalEnergies Marketing Kenya |

---

## dbt Models

```
models/
├── staging/
│   ├── stg_nse_ticks.sql      ← cleans raw_ticks (deduplicate, cast timestamps)
│   └── stg_nse_ohlcv.sql      ← validates 5-min OHLCV from Flink
└── marts/
    ├── fct_nse_daily_summary.sql  ← one row per ticker per day (O/H/L/C + % change)
    └── fct_nse_top_movers.sql     ← top 5 gainers + top 5 losers per day
```

Run individual models:
```bash
docker compose run --rm dbt dbt run --select stg_nse_ticks
docker compose run --rm dbt dbt run --select fct_nse_daily_summary
docker compose run --rm dbt dbt test
```

---

## Kestra Flows

Two flows live in `kestra/flows/`:

**nse_pipeline.yml** — main scheduled flow
- 09:00 EAT: health checks on Redpanda, ClickHouse, topic existence
- 15:35 EAT: runs dbt staging + marts + tests, logs market summary

**nse_dbt_manual_run.yml** — on-demand dbt trigger
- Trigger from Kestra UI at http://localhost:8082
- Accepts an optional `--select` argument so you can run specific models

---

## Common Issues

**Producer says "NoBrokersAvailable"**
Redpanda isn't ready yet. Wait 30 seconds and check: `docker logs nse_redpanda`

**Flink job exits immediately**
Usually a Python import error. Check: `docker logs nse_flink_taskmanager`
The most common cause is `clickhouse-connect` not installed — rebuild the image:
`docker compose build flink-jm flink-tm`

**ClickHouse returns empty results**
If the NSE is currently closed (outside 09:00–15:30 EAT Mon–Fri), the producer
is sleeping and no new ticks are being written. This is expected behaviour.
To test outside market hours, temporarily remove the `is_market_open()` check
in `producer/nse_producer.py`.

**dbt can't connect to ClickHouse**
The dbt container reads CLICKHOUSE_PASSWORD from your `.env` file. Make sure
`.env` exists (copy from `.env.example`) and the password matches what
ClickHouse was started with.

---

## Project Structure

```
nse-stock-tracker/
├── docker-compose.yml
├── .env.example
├── redpanda-console-config.yml
├── producer/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── nse_producer.py          ← yfinance → Redpanda
├── flink/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── nse_flink_job.py         ← Redpanda → ClickHouse (raw + OHLCV)
├── clickhouse/
│   └── init.sql                 ← schema: raw_ticks, ohlcv_5min, daily_summary
├── dbt/
│   ├── profiles.yml
│   ├── dbt_project.yml
│   ├── packages.yml
│   └── models/
│       ├── staging/
│       │   ├── stg_nse_ticks.sql
│       │   ├── stg_nse_ohlcv.sql
│       │   └── schema.yml
│       └── marts/
│           ├── fct_nse_daily_summary.sql
│           ├── fct_nse_top_movers.sql
│           └── schema.yml
├── kestra/
│   └── flows/
│       ├── nse_pipeline.yml         ← scheduled orchestration
│       └── nse_dbt_manual_run.yml   ← manual dbt trigger
└── scripts/
    ├── submit_flink_job.sh      ← submit Flink job after docker compose up
    └── check_pipeline.sh        ← health check all services
```

---

## Author

**Ian Nyaga**
DataTalks.Club DE Zoomcamp 2026 | Nairobi, Kenya
The Hague Internet Services — ISP operating in Embakasi


