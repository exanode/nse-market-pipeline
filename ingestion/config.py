import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()


@dataclass
class S3Config:
    bucket: str
    region: str
    raw_prefix: str = "raw/nse/prices"
    staging_prefix: str = "staging/nse/prices"
    curated_prefix: str = "curated/nse/prices"


@dataclass
class SnowflakeConfig:
    account: str
    user: str
    password: str
    role: str
    warehouse: str
    database: str
    schema: str


@dataclass
class NSEConfig:
    base_url: str = "https://www.nseindia.com/api"
    index_endpoint: str = "/equity-stockIndices?index={index_name}"
    history_endpoint: str = (
        "/historical/securityArchives"
        "?from={from_date}&to={to_date}&symbol={symbol}"
        "&dataType=priceVolumeDeliverable&series=EQ"
    )
    request_delay_seconds: float = 1.0
    max_retries: int = 5
    backoff_base: float = 2.0


@dataclass
class PipelineConfig:
    run_date: str
    index_name: str
    checkpoint_path: str = "checkpoints/ingest_checkpoint.json"
    log_path: str = "logs/pipeline.log"


def load_config(run_date: str = None, index_name: str = None) -> dict:
    return {
        "s3": S3Config(
            bucket=os.environ["S3_BUCKET"],
            region=os.environ.get("AWS_REGION", "ap-south-1"),
        ),
        "snowflake": SnowflakeConfig(
            account=os.environ["SNOWFLAKE_ACCOUNT"],
            user=os.environ["SNOWFLAKE_USER"],
            password=os.environ["SNOWFLAKE_PASSWORD"],
            role=os.environ.get("SNOWFLAKE_ROLE", "TRANSFORMER"),
            warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
            database=os.environ.get("SNOWFLAKE_DATABASE", "NSE_MARKET"),
            schema=os.environ.get("SNOWFLAKE_SCHEMA", "RAW"),
        ),
        "nse": NSEConfig(),
        "pipeline": PipelineConfig(
            run_date=run_date or os.environ.get("RUN_DATE"),
            index_name=index_name or os.environ.get("INDEX_NAME", "NIFTY 50"),
        ),
    }
