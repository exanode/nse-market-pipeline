import logging
import time
from urllib.parse import quote

import requests
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

logger = logging.getLogger(__name__)

# NSE requires browser-like headers and session cookies
_NSE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/",
}


class RateLimitError(Exception):
    pass


def make_session() -> requests.Session:
    """
    Create a requests session with NSE cookies pre-warmed.
    Call once and reuse across all symbol fetches in a run.
    """
    session = requests.Session()
    session.headers.update(_NSE_HEADERS)
    try:
        session.get("https://www.nseindia.com", timeout=10)
        time.sleep(0.5)
    except requests.RequestException as exc:
        logger.warning("nse cookie warm-up failed: %s", exc)
    return session


@retry(
    retry=retry_if_exception_type((RateLimitError, requests.Timeout, requests.ConnectionError)),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=60),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)
def _get_with_retry(session: requests.Session, url: str) -> dict:
    resp = session.get(url, timeout=15)
    if resp.status_code == 429:
        raise RateLimitError(f"rate limited: {url}")
    resp.raise_for_status()
    return resp.json()


def fetch_index_constituents(session: requests.Session, base_url: str, index_name: str) -> list:
    """Return equity symbols for the given NSE index (priority=0 rows only)."""
    url = f"{base_url}/equity-stockIndices?index={quote(index_name)}"
    data = _get_with_retry(session, url)
    symbols = [row["symbol"] for row in data.get("data", []) if row.get("priority") == 0]
    logger.info("fetched %d symbols for index=%s", len(symbols), index_name)
    return symbols


def fetch_price_history(
    session: requests.Session,
    base_url: str,
    symbol: str,
    from_date: str,
    to_date: str,
) -> list:
    """
    Fetch price history for a single symbol.

    NSE's historicalSecurityArchives endpoint returns all records for the
    given date range in one response (no server-side pagination by page number).
    Dates must be in dd-mm-yyyy format.
    """
    url = (
        f"{base_url}/historical/securityArchives"
        f"?from={from_date}&to={to_date}"
        f"&symbol={quote(symbol)}"
        f"&dataType=priceVolumeDeliverable&series=EQ"
    )

    try:
        data = _get_with_retry(session, url)
    except Exception as exc:
        logger.error("fetch failed symbol=%s error=%s", symbol, exc)
        raise

    records = data.get("data", [])
    logger.info("symbol=%s rows=%d", symbol, len(records))
    return records
