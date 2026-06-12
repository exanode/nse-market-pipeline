import argparse
import logging
import time
import uuid
from datetime import datetime, timedelta

from ingestion.checkpoint import is_done, mark_done
from ingestion.config import load_config
from ingestion.fetch import make_session, fetch_index_constituents, fetch_price_history
from ingestion.logger import setup_logging
from ingestion.writer import write_raw_to_s3

logger = logging.getLogger(__name__)


def _date_range_for_run(run_date: str) -> tuple:
    """
    Return (from_date, to_date) in dd-mm-yyyy format for the NSE API.
    Fetches 365 days ending on run_date.
    """
    end = datetime.strptime(run_date, "%Y-%m-%d")
    start = end - timedelta(days=365)
    return start.strftime("%d-%m-%Y"), end.strftime("%d-%m-%Y")


def run_ingest(run_date: str = None, index_name: str = None, dry_run: bool = False) -> dict:
    run_id = str(uuid.uuid4())
    start = time.time()

    config = load_config(run_date=run_date, index_name=index_name)
    nse = config["nse"]
    s3 = config["s3"]
    pipeline = config["pipeline"]

    # default run_date to today if not provided
    if not pipeline.run_date:
        pipeline.run_date = datetime.today().strftime("%Y-%m-%d")

    setup_logging(log_path=pipeline.log_path)
    logger.info(
        "ingest started",
        extra={"run_id": run_id, "run_date": pipeline.run_date, "index": pipeline.index_name},
    )

    from_date, to_date = _date_range_for_run(pipeline.run_date)

    # one session, one cookie warm-up for the full run
    session = make_session()

    symbols = fetch_index_constituents(session, nse.base_url, pipeline.index_name)
    if not symbols:
        logger.error("no symbols returned for index=%s", pipeline.index_name)
        return {"run_id": run_id, "status": "failed", "reason": "empty symbol list"}

    rows_read = 0
    rows_written = 0
    skipped = 0
    failed = []

    for symbol in symbols:
        if is_done(pipeline.checkpoint_path, symbol):
            logger.info("skipping symbol=%s (already done this run)", symbol)
            skipped += 1
            continue

        try:
            records = fetch_price_history(
                session=session,
                base_url=nse.base_url,
                symbol=symbol,
                from_date=from_date,
                to_date=to_date,
            )
            rows_read += len(records)

            if dry_run:
                logger.info("dry_run symbol=%s rows=%d", symbol, len(records))
                continue

            s3_key = write_raw_to_s3(
                records=records,
                symbol=symbol,
                run_date=pipeline.run_date,
                run_id=run_id,
                s3_config=s3,
            )

            if s3_key:
                rows_written += len(records)
                mark_done(pipeline.checkpoint_path, symbol, s3_key)

        except Exception as exc:
            logger.error("symbol=%s failed error=%s", symbol, exc)
            failed.append(symbol)

        time.sleep(nse.request_delay_seconds)

    duration = round(time.time() - start, 2)
    summary = {
        "run_id": run_id,
        "run_date": pipeline.run_date,
        "index": pipeline.index_name,
        "symbols_total": len(symbols),
        "symbols_skipped": skipped,
        "symbols_failed": len(failed),
        "rows_read": rows_read,
        "rows_written": rows_written,
        "duration_seconds": duration,
        "status": "success" if not failed else "partial",
    }
    logger.info("ingest completed", extra=summary)
    return summary


def main():
    parser = argparse.ArgumentParser(description="NSE price ingest to S3")
    parser.add_argument("--run-date", help="Partition date YYYY-MM-DD (default: today)")
    parser.add_argument("--index", default=None, help="NSE index name (default from .env)")
    parser.add_argument("--dry-run", action="store_true", help="Fetch only, skip S3 writes")
    args = parser.parse_args()

    result = run_ingest(
        run_date=args.run_date,
        index_name=args.index,
        dry_run=args.dry_run,
    )

    if result.get("status") == "failed":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
