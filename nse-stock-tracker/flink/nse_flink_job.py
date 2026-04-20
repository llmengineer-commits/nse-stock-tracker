"""
NSE Kenya Stock Tracker — PyFlink Streaming Job
────────────────────────────────────────────────
Reads raw tick messages from Redpanda, does two things simultaneously:

  1. Writes every raw tick straight to ClickHouse (bronze layer)
  2. Aggregates into 5-minute OHLCV windows and writes those too (silver layer)

We use PyFlink's DataStream API rather than Table API so we have full control
over the ClickHouse sink without needing any JDBC JARs.
The ClickHouse HTTP client (clickhouse-connect) handles the writes directly
from Python — cleaner and easier to debug than JDBC-based sinks.

Ian Nyaga | The Hague Internet Services | DE Zoomcamp 2026
"""

import os
import json
import logging
from datetime import datetime, timezone
from typing import Iterator

import clickhouse_connect
from clickhouse_connect.driver.client import Client

from pyflink.common import WatermarkStrategy, Duration, Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment, RuntimeContext
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaOffsetsInitializer,
)
from pyflink.datastream.functions import (
    MapFunction,
    SinkFunction,
    ProcessWindowFunction,
)
from pyflink.datastream.window import TumblingProcessingTimeWindows, Time

# ── Config ─────────────────────────────────────────────────────────────────

BROKER     = os.getenv("REDPANDA_BROKER", "redpanda:29092")
TOPIC      = "nse.raw.ticks"
GROUP_ID   = "flink-nse-consumer-v1"

CH_HOST    = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CH_PORT    = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CH_DB      = os.getenv("CLICKHOUSE_DB", "nse")
CH_USER    = os.getenv("CLICKHOUSE_USER", "nse_user")
CH_PASS    = os.getenv("CLICKHOUSE_PASSWORD", "")

WINDOW_MINUTES = 5   # OHLCV window size

# ── Logging ─────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
log = logging.getLogger("nse_flink")


# ── Data Classes ─────────────────────────────────────────────────────────────

class Tick:
    """A single price snapshot for one NSE ticker."""
    __slots__ = ("ticker", "name", "price", "volume", "market_cap",
                 "currency", "fetched_at", "exchange")

    def __init__(self, d: dict):
        self.ticker     = d.get("ticker", "")
        self.name       = d.get("name", "")
        self.price      = float(d.get("price", 0))
        self.volume     = int(d.get("volume", 0))
        self.market_cap = float(d.get("market_cap", 0))
        self.currency   = d.get("currency", "KES")
        self.fetched_at = d.get("fetched_at", "")
        self.exchange   = d.get("exchange", "NSE")


# ── Map Function ─────────────────────────────────────────────────────────────

class ParseTickJson(MapFunction):
    """Deserialise the raw JSON string from Redpanda into a Tick object."""

    def map(self, raw: str) -> Tick | None:
        try:
            return Tick(json.loads(raw))
        except (json.JSONDecodeError, KeyError) as e:
            log.warning(f"Could not parse message: {e} | raw={raw[:80]}")
            return None


# ── ClickHouse Sinks ─────────────────────────────────────────────────────────

class RawTickSink(SinkFunction):
    """
    Writes each Tick directly to nse.raw_ticks in ClickHouse.
    The client is opened once per TaskManager worker (open()) and reused —
    we don't want to create a new HTTP connection for every single row.
    """

    def __init__(self):
        self._client: Client | None = None

    def open(self, runtime_context: RuntimeContext):
        log.info(f"Opening ClickHouse connection → {CH_HOST}:{CH_PORT}/{CH_DB}")
        self._client = clickhouse_connect.get_client(
            host=CH_HOST, port=CH_PORT,
            database=CH_DB,
            username=CH_USER, password=CH_PASS,
            connect_timeout=10,
        )

    def invoke(self, tick: Tick, context):
        if tick is None:
            return

        try:
            self._client.insert(
                "raw_ticks",
                data=[[
                    tick.ticker,
                    tick.name,
                    tick.price,
                    tick.volume,
                    tick.market_cap,
                    tick.currency,
                    tick.fetched_at,
                    tick.exchange,
                ]],
                column_names=[
                    "ticker", "name", "price", "volume",
                    "market_cap", "currency", "fetched_at", "exchange",
                ],
            )
            log.debug(f"Raw tick written: {tick.ticker} @ {tick.price}")

        except Exception as e:
            log.error(f"Failed to insert raw tick for {tick.ticker}: {e}")


class OhlcvSink(SinkFunction):
    """
    Writes a single OHLCV aggregate row (produced by the window function)
    to nse.ohlcv_5min in ClickHouse.
    """

    def __init__(self):
        self._client: Client | None = None

    def open(self, runtime_context: RuntimeContext):
        self._client = clickhouse_connect.get_client(
            host=CH_HOST, port=CH_PORT,
            database=CH_DB,
            username=CH_USER, password=CH_PASS,
        )

    def invoke(self, row: tuple, context):
        # row = (ticker, window_start, window_end, open, high, low, close, count)
        try:
            self._client.insert(
                "ohlcv_5min",
                data=[list(row)],
                column_names=[
                    "ticker", "window_start", "window_end",
                    "open_price", "high_price", "low_price", "close_price",
                    "tick_count",
                ],
            )
            log.info(
                f"OHLCV written: {row[0]} "
                f"O={row[3]:.2f} H={row[4]:.2f} L={row[5]:.2f} C={row[6]:.2f}"
            )
        except Exception as e:
            log.error(f"Failed to insert OHLCV row: {e}")


# ── Window Function ──────────────────────────────────────────────────────────

class OhlcvAggregator(ProcessWindowFunction):
    """
    Computes OHLCV from a 5-minute window of Tick objects for one ticker.

    Since we're using processing-time tumbling windows (not event-time), we
    don't need watermarks. That's a deliberate trade-off — it keeps the job
    simple and avoids watermark stalling issues, at the cost of a tiny amount
    of latency accuracy. For stock dashboards that refresh every 5 minutes
    anyway, that's perfectly fine.
    """

    def process(
        self,
        key: str,                    # the ticker symbol
        context: ProcessWindowFunction.Context,
        ticks: Iterator[Tick],
    ) -> Iterator[tuple]:
        tick_list = [t for t in ticks if t is not None]

        if not tick_list:
            return

        prices = [t.price for t in tick_list]
        win    = context.window()

        # Convert Flink's millisecond timestamps to Python datetimes
        win_start = datetime.fromtimestamp(win.start / 1000, tz=timezone.utc)
        win_end   = datetime.fromtimestamp(win.end   / 1000, tz=timezone.utc)

        yield (
            key,
            win_start,
            win_end,
            prices[0],      # open  = first price in the window
            max(prices),    # high
            min(prices),    # low
            prices[-1],     # close = last price in the window
            len(prices),    # tick_count
        )


# ── Main Job ──────────────────────────────────────────────────────────────────

def main():
    log.info("Starting NSE Flink job...")

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    # Processing time — no event-time watermarks needed
    env.get_config().set_auto_watermark_interval(0)

    # ── Source: Redpanda (Kafka-compatible) ──────────────────────────────────
    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(BROKER)
        .set_topics(TOPIC)
        .set_group_id(GROUP_ID)
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    raw_stream = env.from_source(
        source=source,
        watermark_strategy=WatermarkStrategy.no_watermarks(),
        source_name="RedpandaTickSource",
    )

    # Parse JSON strings into Tick objects and drop any parse failures
    tick_stream = (
        raw_stream
        .map(ParseTickJson(), output_type=Types.PICKLED_BYTE_ARRAY())
        .filter(lambda t: t is not None)
    )

    # ── Sink 1: every raw tick → ClickHouse nse.raw_ticks ───────────────────
    tick_stream.add_sink(RawTickSink()).name("ClickHouseRawSink")

    # ── Sink 2: 5-min OHLCV windows → ClickHouse nse.ohlcv_5min ─────────────
    (
        tick_stream
        .key_by(lambda t: t.ticker)
        .window(TumblingProcessingTimeWindows.of(Time.minutes(WINDOW_MINUTES)))
        .process(OhlcvAggregator())
        .add_sink(OhlcvSink())
        .name("ClickHouseOhlcvSink")
    )

    log.info(
        f"Job graph built. Consuming from '{TOPIC}' on {BROKER}. "
        f"OHLCV window = {WINDOW_MINUTES} min."
    )

    env.execute("NSE Kenya Stock Tracker")


if __name__ == "__main__":
    main()
