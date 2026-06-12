import io
import logging
from datetime import datetime, timezone

import boto3
import pyarrow as pa
import pyarrow.parquet as pq

from ingestion.schema import RAW_PRICE_SCHEMA, map_raw_record

logger = logging.getLogger(__name__)


def _s3_client(region: str):
    return boto3.client("s3", region_name=region)


def build_s3_key(prefix: str, symbol: str, run_date: str) -> str:
    """
    Hive-style partitioned key.
    e.g. raw/nse/prices/price_date=2024-01-15/symbol=RELIANCE/data.parquet
    """
    return f"{prefix}/price_date={run_date}/symbol={symbol}/data.parquet"


def write_raw_to_s3(
    records: list[dict],
    symbol: str,
    run_date: str,
    run_id: str,
    s3_config,
) -> str:
    """
    Convert API records to Parquet with explicit schema and upload to S3 raw layer.
    Returns the S3 key.
    """
    if not records:
        logger.warning("no records for symbol=%s run_date=%s - skipping write", symbol, run_date)
        return None

    ingested_at = datetime.now(tz=timezone.utc)
    mapped = [map_raw_record(r, run_id=run_id, ingested_at=ingested_at) for r in records]

    table = pa.Table.from_pylist(mapped, schema=RAW_PRICE_SCHEMA)

    # remove any duplicates for the same date before writing
    # using pandas for dedup since pyarrow doesn't have a native groupby-dedup
    df = table.to_pandas()
    before = len(df)
    df = df.drop_duplicates(subset=["symbol", "date"])
    after = len(df)
    if before != after:
        logger.info("deduped %d -> %d rows for symbol=%s", before, after, symbol)
    table = pa.Table.from_pandas(df, schema=RAW_PRICE_SCHEMA, preserve_index=False)

    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    buf.seek(0)

    key = build_s3_key(s3_config.raw_prefix, symbol, run_date)
    client = _s3_client(s3_config.region)
    client.put_object(Bucket=s3_config.bucket, Key=key, Body=buf.getvalue())

    logger.info(
        "wrote to s3",
        extra={
            "bucket": s3_config.bucket,
            "key": key,
            "rows_written": after,
            "symbol": symbol,
            "run_id": run_id,
        },
    )
    return key


def copy_into_snowflake(sf_config, s3_config, run_date: str) -> None:
    """
    COPY INTO Snowflake raw table from S3 for the given run_date partition.
    Snowflake tracks loaded files internally; re-running is safe (skips already-loaded files).

    Assumes an external stage named RAW_NSE_STAGE is configured in Snowflake with
    S3 credentials or an IAM role. Alternatively, pass credentials inline via env vars.
    """
    import os
    import snowflake.connector

    s3_path = f"s3://{s3_config.bucket}/{s3_config.raw_prefix}/price_date={run_date}/"
    aws_key = os.environ.get("AWS_ACCESS_KEY_ID", "")
    aws_secret = os.environ.get("AWS_SECRET_ACCESS_KEY", "")

    if not aws_key or not aws_secret:
        raise EnvironmentError("AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must be set for COPY INTO")

    copy_sql = (
        f"COPY INTO {sf_config.database}.{sf_config.schema}.raw_nse_prices\n"
        f"FROM '{s3_path}'\n"
        f"CREDENTIALS = (AWS_KEY_ID = '{aws_key}' AWS_SECRET_KEY = '{aws_secret}')\n"
        f"FILE_FORMAT = (TYPE = PARQUET)\n"
        f"MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE\n"
        f"ON_ERROR = ABORT_STATEMENT\n"
        f"PURGE = FALSE"
    )

    conn = snowflake.connector.connect(
        account=sf_config.account,
        user=sf_config.user,
        password=sf_config.password,
        role=sf_config.role,
        warehouse=sf_config.warehouse,
        database=sf_config.database,
        schema=sf_config.schema,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(copy_sql)
            result = cur.fetchall()
            rows_loaded = sum(r[3] for r in result) if result else 0
            logger.info("COPY INTO done run_date=%s files=%d rows_loaded=%d", run_date, len(result), rows_loaded)
    finally:
        conn.close()
