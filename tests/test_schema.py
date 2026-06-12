from datetime import datetime, timezone

import pyarrow as pa
import pytest

from ingestion.schema import map_raw_record, RAW_PRICE_SCHEMA, _to_float, _to_int


SAMPLE_RECORD = {
    "CH_SYMBOL": "INFY",
    "CH_SERIES": "EQ",
    "CH_TIMESTAMP": "2024-01-15",
    "CH_OPENING_PRICE": "1560.00",
    "CH_TRADE_HIGH_PRICE": "1595.40",
    "CH_TRADE_LOW_PRICE": "1555.00",
    "CH_CLOSING_PRICE": "1582.30",
    "CH_PREVIOUS_CLS_PRICE": "1575.00",
    "CH_LAST_TRADED_PRICE": "1582.30",
    "VWAP": "1578.10",
    "CH_52WEEK_HIGH_PRICE": "1700.00",
    "CH_52WEEK_LOW_PRICE": "1300.00",
    "CH_TOT_TRADED_QTY": "12345678",
    "CH_TOT_TRADED_VAL": "19500000000",
    "CH_TOTAL_TRADES": "98765",
    "COP_DELIV_QTY": "5000000",
    "COP_DELIV_PERC": "40.50",
}


def test_map_raw_record_basic():
    now = datetime.now(tz=timezone.utc)
    result = map_raw_record(SAMPLE_RECORD, run_id="test-run-1", ingested_at=now)

    assert result["symbol"] == "INFY"
    assert result["series"] == "EQ"
    assert result["close"] == 1582.30
    assert result["volume"] == 12345678
    assert result["run_id"] == "test-run-1"
    assert result["ingested_at"] == now


def test_map_raw_record_missing_optional_fields():
    now = datetime.now(tz=timezone.utc)
    minimal = {"CH_SYMBOL": "TCS", "CH_TIMESTAMP": "2024-01-15", "CH_CLOSING_PRICE": "3500.00"}
    result = map_raw_record(minimal, run_id="r1", ingested_at=now)

    assert result["symbol"] == "TCS"
    assert result["close"] == 3500.00
    assert result["volume"] is None
    assert result["vwap"] is None


def test_schema_roundtrip():
    now = datetime.now(tz=timezone.utc)
    records = [map_raw_record(SAMPLE_RECORD, run_id="r1", ingested_at=now)]
    table = pa.Table.from_pylist(records, schema=RAW_PRICE_SCHEMA)
    assert table.num_rows == 1
    assert table.schema == RAW_PRICE_SCHEMA


@pytest.mark.parametrize("val,expected", [
    ("1234.56", 1234.56),
    ("-",       None),
    ("",        None),
    (None,      None),
    (0,         0.0),
])
def test_to_float(val, expected):
    assert _to_float(val) == expected


@pytest.mark.parametrize("val,expected", [
    ("1000000", 1000000),
    ("-",       None),
    (None,      None),
    ("1.5e6",   1500000),
])
def test_to_int(val, expected):
    assert _to_int(val) == expected
