"""
pool.py — SOZU pool balance cache and contract balance query.

Imports: config, wallet.
"""
import json
import subprocess
import threading
import time

from .config import (
    _log, CONTRACT_ID, CONTRACT_ADDRESS, RUSK_VERSION, _NODE_STATE_URL,
)

# ── Pool balance cache ─────────────────────────────────────────────────────────
_pool_balance_cache:    float = 0.0
_pool_balance_cache_ts: float = 0.0
_pool_balance_lock            = threading.Lock()
POOL_CACHE_STALE_SECS: int    = 300


def _query_contract_total_dusk() -> float:
    """Query total allocatable DUSK in the SOZU pool contract."""
    url = (f"{_NODE_STATE_URL}/on/contracts:"
           "0100000000000000000000000000000000000000000000000000000000000000"
           "/contract_balance")
    rc = subprocess.run(
        ["curl", "-s", "-X", "POST", url,
         "-H", f"rusk-version: {RUSK_VERSION}",
         "-H", "Content-Type: application/json",
         "-d", json.dumps(CONTRACT_ID())],
        capture_output=True, text=True, timeout=15)
    raw = rc.stdout.strip()
    try:
        return int(json.loads(raw)) / 1_000_000_000
    except Exception:
        pass
    try:
        import re, struct as _struct
        hex_match = re.search(r'[0-9a-fA-F]{16}', raw)
        if hex_match:
            return _struct.unpack("<Q", bytes.fromhex(hex_match.group(0)))[0] / 1_000_000_000
    except Exception:
        pass
    _log(f"[pool] contract balance decode failed: {raw!r}")
    return 0.0


def _query_pool_balance_dusk(staked_dusk: float = 0.0) -> float:
    """Query allocatable DUSK in the SOZU pool. stdout only — not rotation log."""
    available = _query_contract_total_dusk()
    _log(f"[pool] balance: {available:.4f} DUSK")
    return available


def _pool_fetch_real(pw: str = "") -> float:
    """Fetch authoritative pool balance, update cache, return DUSK."""
    global _pool_balance_cache, _pool_balance_cache_ts
    dusk = _query_pool_balance_dusk()
    with _pool_balance_lock:
        _pool_balance_cache    = dusk
        _pool_balance_cache_ts = time.time()
    return dusk


def _pool_delta(delta_dusk: float) -> None:
    """Apply signed delta to pool balance cache (event-driven update)."""
    global _pool_balance_cache
    with _pool_balance_lock:
        _pool_balance_cache = max(0.0, _pool_balance_cache + delta_dusk)


def _fast_alloc_pool() -> float:
    """Return pool balance from cache — no network call."""
    with _pool_balance_lock:
        return _pool_balance_cache
