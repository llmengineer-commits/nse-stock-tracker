"""
NSE Kenya Stock Producer
────────────────────────
Polls yfinance every 5 minutes during NSE trading hours and publishes
each stock snapshot to a Redpanda topic (nse.raw.ticks).

NSE trading hours: Mon–Fri, 09:00 – 15:30 EAT (UTC+3)

The tickers below are the 20 most liquid counters on the Nairobi bourse.
Yahoo Finance uses the .NR suffix for NSE Kenya equities.

Ian Nyaga | The Hague Internet Services | DE Zoomcamp 2026
"""

import os
import json
import time
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

import yfinance as yf
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# ── Config ─────────────────────────────────────────────────────────────────

BROKER           = os.getenv("REDPANDA_BROKER", "redpanda:29092")
TOPIC            = "nse.raw.ticks"
POLL_INTERVAL    = int(os.getenv("POLL_INTERVAL_SECONDS", "300"))  # 5 minutes default
NAIROBI          = ZoneInfo("Africa/Nairobi")

# NSE Kenya tickers — ordered roughly by market cap.
# If you want to add more, look up the ticker on finance.yahoo.com
# and append .NR to the symbol shown.
NSE_TICKERS = [
    "SCOM.NR",   # Safaricom PLC          — telecom, biggest by market cap
    "EQTY.NR",   # Equity Group Holdings  — pan-African bank
    "KCB.NR",    # KCB Group              — largest bank by assets
    "EABL.NR",   # East African Breweries — FMCG / Diageo subsidiary
    "BAT.NR",    # BAT Kenya              — tobacco
    "COOP.NR",   # Co-operative Bank      — retail banking
    "ABSA.NR",   # Absa Bank Kenya        — Barclays successor
    "NCBA.NR",   # NCBA Group             — NIC + CBA merger
    "SBIC.NR",   # Stanbic Holdings       — Standard Bank Kenya
    "DTB.NR",    # Diamond Trust Bank     — regional bank
    "SCBK.NR",   # Standard Chartered Kenya
    "IMM.NR",    # I&M Group              — regional banking group
    "KEGN.NR",   # KenGen                 — electricity generation
    "KPLC.NR",   # Kenya Power & Lighting — electricity distribution
    "SASN.NR",   # Sasini PLC             — tea & coffee estates
    "BAMB.NR",   # Bamburi Cement         — largest cement producer
    "CTUM.NR",   # Centum Investment      — diversified investment
    "CABL.NR",   # East African Cables
    "NMG.NR",    # Nation Media Group     — largest media house
    "TCL.NR",    # TotalEnergies Marketing Kenya
]

# ── Logging ─────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger("nse_producer")


# ── Helpers ─────────────────────────────────────────────────────────────────

def is_market_open() -> bool:
    """Return True if NSE is currently open for trading."""
    now = datetime.now(NAIROBI)

    # No trading on weekends
    if now.weekday() >= 5:
        return False

    # Pre-open
    if now.hour < 9:
        return False

    # Post-close: 15:30 EAT
    if now.hour > 15:
        return False
    if now.hour == 15 and now.minute >= 30:
        return False

    return True


def minutes_to_open() -> int:
    """How many minutes until the next market open (rough estimate)."""
    now = datetime.now(NAIROBI)
    # Simple estimate — good enough for a sleep timer
    if now.hour < 9:
        return (9 - now.hour) * 60 - now.minute
    # After close — next day opens at 09:00
    return (24 - now.hour + 9) * 60 - now.minute


def build_producer() -> KafkaProducer:
    """
    Create a KafkaProducer connected to Redpanda.
    Retries for up to 2 minutes — gives Redpanda time to start if the
    producer container comes up before the broker is ready.
    """
    for attempt in range(1, 9):
        try:
            producer = KafkaProducer(
                bootstrap_servers=BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",       # wait for all in-sync replicas to confirm
                retries=5,
                linger_ms=100,    # small batch window — reduces Redpanda round-trips
            )
            log.info(f"Connected to Redpanda at {BROKER}")
            return producer
        except NoBrokersAvailable:
            wait = attempt * 15
            log.warning(f"Redpanda not ready (attempt {attempt}/8). Retrying in {wait}s...")
            time.sleep(wait)

    raise RuntimeError(f"Could not connect to Redpanda at {BROKER} after multiple attempts.")


def fetch_tick(symbol: str) -> dict | None:
    """
    Fetch the latest snapshot for a single NSE ticker via yfinance.
    Returns None if yfinance can't reach the ticker (network blip, delisting, etc.)

    Note: yfinance.fast_info is much lighter than .info — it skips the HTML
    scrape and just hits the Yahoo Finance JSON API. Much faster for polling.
    """
    try:
        ticker = yf.Ticker(symbol)
        fi = ticker.fast_info

        price = fi.last_price
        if price is None or price == 0:
            log.debug(f"{symbol}: price is None or zero — skipping")
            return None

        return {
            "ticker":      symbol,
            "name":        symbol.replace(".NR", ""),
            "price":       round(float(price), 2),
            "volume":      int(fi.three_month_average_volume or 0),
            "market_cap":  float(fi.market_cap or 0),
            "currency":    "KES",
            "fetched_at":  datetime.now(NAIROBI).isoformat(),
            "exchange":    "NSE",
        }

    except Exception as exc:
        log.warning(f"{symbol}: fetch failed — {exc}")
        return None


# ── Main loop ────────────────────────────────────────────────────────────────

def main():
    log.info("NSE Stock Producer starting up...")
    log.info(f"Tracking {len(NSE_TICKERS)} tickers, polling every {POLL_INTERVAL}s")

    producer = build_producer()

    while True:
        if not is_market_open():
            wait_mins = minutes_to_open()
            log.info(f"NSE is closed. Next open in ~{wait_mins} min. Sleeping...")
            # Sleep in chunks so we don't miss the open by more than 1 minute
            time.sleep(min(wait_mins * 60, 300))
            continue

        poll_start = time.time()
        published  = 0
        skipped    = 0

        log.info(f"── Polling {len(NSE_TICKERS)} tickers ──")

        for symbol in NSE_TICKERS:
            tick = fetch_tick(symbol)

            if tick is None:
                skipped += 1
                continue

            producer.send(
                TOPIC,
                key=symbol,
                value=tick,
            )
            published += 1
            log.info(f"  {symbol:<10s}  KES {tick['price']:>10.2f}  cap={tick['market_cap']/1e9:.1f}B")

        producer.flush()
        elapsed = time.time() - poll_start

        log.info(
            f"Batch complete: {published} published, {skipped} skipped "
            f"in {elapsed:.1f}s. Next poll in {POLL_INTERVAL}s."
        )

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
