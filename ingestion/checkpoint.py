import json
import os
import logging

logger = logging.getLogger(__name__)


def load_checkpoint(path: str) -> dict:
    """Return checkpoint state or empty dict if file doesn't exist."""
    if not os.path.exists(path):
        return {}
    with open(path, "r") as f:
        state = json.load(f)
    logger.info("checkpoint loaded", extra={"path": path, "symbols_done": len(state)})
    return state


def save_checkpoint(path: str, state: dict) -> None:
    """Persist checkpoint state to disk."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(state, f, indent=2)
    logger.debug("checkpoint saved", extra={"path": path})


def mark_done(path: str, symbol: str, s3_key: str) -> None:
    """Mark a symbol as successfully ingested."""
    state = load_checkpoint(path)
    state[symbol] = {"status": "done", "s3_key": s3_key}
    save_checkpoint(path, state)


def is_done(path: str, symbol: str) -> bool:
    """Return True if this symbol was already ingested in a previous run."""
    state = load_checkpoint(path)
    return state.get(symbol, {}).get("status") == "done"
