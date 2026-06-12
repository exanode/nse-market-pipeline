from unittest.mock import MagicMock, patch

import pytest

from ingestion.fetch import fetch_index_constituents, fetch_price_history, RateLimitError


MOCK_INDEX_RESPONSE = {
    "data": [
        {"symbol": "RELIANCE", "priority": 0},
        {"symbol": "HDFCBANK", "priority": 0},
        {"symbol": "TCS",      "priority": 0},
        {"symbol": "NIFTY 50", "priority": 1},  # index aggregate row - excluded
    ]
}

MOCK_PRICE_RESPONSE = {
    "data": [
        {
            "CH_SYMBOL": "RELIANCE",
            "CH_SERIES": "EQ",
            "CH_TIMESTAMP": "2024-01-15",
            "CH_OPENING_PRICE": "2450.00",
            "CH_TRADE_HIGH_PRICE": "2480.00",
            "CH_TRADE_LOW_PRICE": "2430.00",
            "CH_CLOSING_PRICE": "2465.50",
            "CH_TOT_TRADED_QTY": "5000000",
            "VWAP": "2458.30",
        }
    ]
}


def make_mock_session():
    return MagicMock()


@patch("ingestion.fetch._get_with_retry", return_value=MOCK_INDEX_RESPONSE)
def test_fetch_index_constituents_filters_priority(mock_get):
    session = make_mock_session()
    symbols = fetch_index_constituents(session, "https://www.nseindia.com/api", "NIFTY 50")

    assert "RELIANCE" in symbols
    assert "HDFCBANK" in symbols
    assert "TCS" in symbols
    assert "NIFTY 50" not in symbols  # priority != 0 excluded
    assert len(symbols) == 3


@patch("ingestion.fetch._get_with_retry", return_value=MOCK_PRICE_RESPONSE)
def test_fetch_price_history_returns_records(mock_get):
    session = make_mock_session()
    records = fetch_price_history(
        session=session,
        base_url="https://www.nseindia.com/api",
        symbol="RELIANCE",
        from_date="01-01-2024",
        to_date="15-01-2024",
    )
    assert len(records) == 1
    assert records[0]["CH_SYMBOL"] == "RELIANCE"
    assert records[0]["CH_CLOSING_PRICE"] == "2465.50"


@patch("ingestion.fetch._get_with_retry", return_value={"data": []})
def test_fetch_price_history_empty_response(mock_get):
    session = make_mock_session()
    records = fetch_price_history(
        session=session,
        base_url="https://www.nseindia.com/api",
        symbol="UNKNOWN",
        from_date="01-01-2024",
        to_date="15-01-2024",
    )
    assert records == []


@patch("ingestion.fetch._get_with_retry", side_effect=RateLimitError("429"))
def test_fetch_price_history_raises_on_rate_limit(mock_get):
    session = make_mock_session()
    with pytest.raises(RateLimitError):
        fetch_price_history(
            session=session,
            base_url="https://www.nseindia.com/api",
            symbol="INFY",
            from_date="01-01-2024",
            to_date="15-01-2024",
        )
