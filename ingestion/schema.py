from __future__ import annotations

import pyarrow as pa

# explicit schema - no inference; field types locked at ingest time
RAW_PRICE_SCHEMA = pa.schema([
    pa.field("symbol", pa.string(), nullable=False),
    pa.field("series", pa.string(), nullable=True),
    pa.field("date", pa.date32(), nullable=False),
    pa.field("open", pa.float64(), nullable=True),
    pa.field("high", pa.float64(), nullable=True),
    pa.field("low", pa.float64(), nullable=True),
    pa.field("prev_close", pa.float64(), nullable=True),
    pa.field("ltp", pa.float64(), nullable=True),
    pa.field("close", pa.float64(), nullable=False),
    pa.field("vwap", pa.float64(), nullable=True),
    pa.field("52w_high", pa.float64(), nullable=True),
    pa.field("52w_low", pa.float64(), nullable=True),
    pa.field("volume", pa.int64(), nullable=True),
    pa.field("value", pa.float64(), nullable=True),
    pa.field("no_of_trades", pa.int64(), nullable=True),
    pa.field("delivery_volume", pa.int64(), nullable=True),
    pa.field("pct_delivery", pa.float64(), nullable=True),
    pa.field("ingested_at", pa.timestamp("us", tz="UTC"), nullable=False),
    pa.field("run_id", pa.string(), nullable=False),
])


def map_raw_record(record: dict, run_id: str, ingested_at) -> dict:
    """Map a single NSE API response record to the raw schema fields."""
    return {
        "symbol": record.get("CH_SYMBOL") or record.get("symbol"),
        "series": record.get("CH_SERIES") or record.get("series"),
        "date": record.get("CH_TIMESTAMP") or record.get("mTIMESTAMP"),
        "open": _to_float(record.get("CH_OPENING_PRICE")),
        "high": _to_float(record.get("CH_TRADE_HIGH_PRICE")),
        "low": _to_float(record.get("CH_TRADE_LOW_PRICE")),
        "prev_close": _to_float(record.get("CH_PREVIOUS_CLS_PRICE")),
        "ltp": _to_float(record.get("CH_LAST_TRADED_PRICE")),
        "close": _to_float(record.get("CH_CLOSING_PRICE")),
        "vwap": _to_float(record.get("VWAP")),
        "52w_high": _to_float(record.get("CH_52WEEK_HIGH_PRICE")),
        "52w_low": _to_float(record.get("CH_52WEEK_LOW_PRICE")),
        "volume": _to_int(record.get("CH_TOT_TRADED_QTY")),
        "value": _to_float(record.get("CH_TOT_TRADED_VAL")),
        "no_of_trades": _to_int(record.get("CH_TOTAL_TRADES")),
        "delivery_volume": _to_int(record.get("COP_DELIV_QTY")),
        "pct_delivery": _to_float(record.get("COP_DELIV_PERC")),
        "ingested_at": ingested_at,
        "run_id": run_id,
    }


def _to_float(val) -> float | None:
    if val is None or val == "" or val == "-":
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _to_int(val) -> int | None:
    if val is None or val == "" or val == "-":
        return None
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return None
