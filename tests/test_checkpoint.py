import json
import os
import tempfile

from ingestion.checkpoint import load_checkpoint, save_checkpoint, mark_done, is_done


def test_load_checkpoint_missing_file():
    result = load_checkpoint("/nonexistent/path/checkpoint.json")
    assert result == {}


def test_save_and_load_roundtrip():
    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "cp", "checkpoint.json")
        state = {"RELIANCE": {"status": "done", "s3_key": "raw/price_date=2024-01-15/symbol=RELIANCE/data.parquet"}}
        save_checkpoint(path, state)
        loaded = load_checkpoint(path)
        assert loaded == state


def test_mark_done_and_is_done():
    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "cp", "checkpoint.json")
        assert not is_done(path, "INFY")
        mark_done(path, "INFY", "raw/price_date=2024-01-15/symbol=INFY/data.parquet")
        assert is_done(path, "INFY")
        assert not is_done(path, "TCS")


def test_mark_done_is_idempotent():
    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "cp", "checkpoint.json")
        mark_done(path, "WIPRO", "s3://bucket/key1.parquet")
        mark_done(path, "WIPRO", "s3://bucket/key2.parquet")
        state = load_checkpoint(path)
        # last write wins, but symbol is still done
        assert state["WIPRO"]["status"] == "done"
        assert state["WIPRO"]["s3_key"] == "s3://bucket/key2.parquet"
