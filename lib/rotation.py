"""
rotation.py — Rotation state, logging helpers, block/event callbacks, worker stubs.

The actual rotation logic will be built here in subsequent sessions.
All callbacks (_on_block_accepted etc.) are imported by rues.py at runtime.
"""
import threading
import time
from collections import deque
from datetime import datetime

from .config import (
    _log, cfg, _load_rotation_enabled, _save_rotation_enabled,
    _load_rotation_phase, _save_rotation_phase,
)
from .pool import _pool_fetch_real, _pool_balance_cache, _pool_balance_lock
from .assess import _fetch_capacity

# ── Rotation state dict ───────────────────────────────────────────────────────
_rotation_state: dict = {
    "enabled":          _load_rotation_enabled(),
    "state":            "idle",
    "step":             "",
    "last_error":       None,
    "epoch_rotated":    None,
    "last_epoch":       None,
    "log":              deque(maxlen=200),
    # tick-cached window context
    "phase":            None,
    "window":           None,
    "blk_left":         None,
    "epoch":            None,
    "rot_win":          None,
    "snatch_win":       None,
    "rot_master_idx":   None,
    "rot_slave_idx":    None,
    "master_idx":       None,
    # capacity
    "active_current":   None,
    "active_maximum":   None,
    "locked_current":   None,
    "locked_maximum":   None,
    "slash_headroom":   None,
    "max_topup_active": None,
    "pending_terminate": None,
}

if _rotation_state["enabled"]:
    print("[rotation] auto-rotation ENABLED (restored from disk)")
else:
    print("[rotation] auto-rotation disabled")

# ── Tip lock ──────────────────────────────────────────────────────────────────
_tip_lock           = threading.Lock()
_current_tip:   int = 0
_current_blk_left: int = 9999
_current_window: str   = "regular"
_current_epoch:  int   = 0
_prev_window:    str   = "regular"

# ── Rotation log helpers ──────────────────────────────────────────────────────
_cmd_log:      list     = []
_cmd_log_lock           = threading.Lock()


def _rlog(msg: str, level: str = "info") -> None:
    ts    = datetime.now().strftime("%H:%M:%S")
    entry = {"ts": ts, "msg": msg, "level": level}
    _rotation_state["log"].appendleft(entry)
    _log(f"[rotation] [{level}] {msg}")


def _rset(state: str, step: str = "") -> None:
    _rotation_state["state"] = state
    _rotation_state["step"]  = step


def _push_cmd_output(name: str, result: dict) -> None:
    entry = {
        "name":        name,
        "ok":          result.get("ok", False),
        "stdout":      result.get("stdout", ""),
        "stderr":      result.get("stderr", ""),
        "duration_ms": result.get("duration_ms", 0),
        "ts":          datetime.now().strftime("%H:%M:%S"),
    }
    with _cmd_log_lock:
        _cmd_log.append(entry)
        if len(_cmd_log) > 200:
            del _cmd_log[:-200]


# ── Phase helpers ─────────────────────────────────────────────────────────────
PHASE_BOOTSTRAP_WAITING      = "bootstrap_waiting"
PHASE_BOOTSTRAP_ROT_SEEDED   = "bootstrap_rot_seeded"
PHASE_BOOTSTRAP_SLAVE_SEEDED = "bootstrap_slave_seeded"
PHASE_ROTATING               = "rotating"
BOOTSTRAP_PHASES = frozenset([
    PHASE_BOOTSTRAP_WAITING,
    PHASE_BOOTSTRAP_ROT_SEEDED,
    PHASE_BOOTSTRAP_SLAVE_SEEDED,
])


def transitions_to_active(node: dict, current_tip: int, blk_left: int = 0) -> int:
    ta = node.get("ta")
    if ta is not None:
        return ta
    eligblk = node.get("eligibility_block")
    if eligblk is None or current_tip <= 0:
        return 2
    EPOCH_BLOCKS = 2160
    blks_away = eligblk - current_tip
    if blks_away <= 0:
        return 0
    return max(1, -(-blks_away // EPOCH_BLOCKS))


# ── Block-accepted callback ────────────────────────────────────────────────────
def _on_block_accepted(height: int) -> None:
    global _current_tip, _current_blk_left, _current_window, _current_epoch, _prev_window
    EPOCH_BLOCKS = 2160
    epoch    = (height // EPOCH_BLOCKS) + 1
    blk_left = EPOCH_BLOCKS - (height % EPOCH_BLOCKS)
    rot_win    = int(cfg("rotation_window")  or 41)
    snatch_win = int(cfg("snatch_window")    or 11)
    window = ("snatch"   if blk_left <= snatch_win else
              "rotation" if blk_left <= rot_win    else
              "regular")

    with _tip_lock:
        was_zero      = (_current_tip == 0)
        _prev_window  = _current_window
        _current_tip      = height
        _current_blk_left = blk_left
        _current_window   = window
        _current_epoch    = epoch

    _rotation_state["blk_left"]   = blk_left
    _rotation_state["epoch"]      = epoch
    _rotation_state["window"]     = window
    _rotation_state["rot_win"]    = rot_win
    _rotation_state["snatch_win"] = snatch_win

    if was_zero:
        def _initial_fetch():
            pw = _get_password()
            try:
                cap = _fetch_capacity(pw)
                SLASH_RATE = 0.10
                headroom   = max(0.0, cap.get("locked_maximum", 0.0) - cap.get("locked_current", 0.0))
                _rotation_state.update({
                    "locked_current":  round(cap.get("locked_current",  0.0), 4),
                    "locked_maximum":  round(cap.get("locked_maximum",  0.0), 4),
                    "active_current":  round(cap.get("active_current",  0.0), 4),
                    "active_maximum":  round(cap.get("active_maximum",  0.0), 4),
                    "slash_headroom":  round(headroom, 4),
                    "max_topup_active": round(headroom / SLASH_RATE, 4) if SLASH_RATE > 0 else 0.0,
                })
                _log(f"[block0] capacity seeded")
            except Exception as e:
                _log(f"[block0] capacity fetch failed: {e}")
            try:
                _pool_fetch_real()
            except Exception as e:
                _log(f"[block0] pool fetch failed: {e}")
        import threading as _t
        _t.Thread(target=_initial_fetch, daemon=True, name="block0-init").start()


def _get_password():
    try:
        from .wallet import get_password
        return get_password()
    except Exception:
        return ""


# ── Event callbacks ────────────────────────────────────────────────────────────
def _on_deposit_event(decoded: dict) -> None:
    _log("[rotation] deposit event — rotation engine not yet implemented")


def _on_liquidate_event(decoded: dict, is_own: bool) -> None:
    _log(f"[rotation] liquidate event (own={is_own}) — rotation engine not yet implemented")


def _on_reward_recycle_event(decoded: dict) -> None:
    _log("[rotation] reward-recycle event — rotation engine not yet implemented")


# ── Worker stubs ──────────────────────────────────────────────────────────────
def _rotation_job_worker() -> None:
    while True:
        time.sleep(5)


def _rotation_heartbeat() -> None:
    while True:
        time.sleep(300)
        if not _rotation_state.get("enabled"):
            continue
        with _tip_lock:
            blk_left = _current_blk_left
            epoch    = _current_epoch
            window   = _current_window
        with _pool_balance_lock:
            pool = _pool_balance_cache
        _rlog(f"heartbeat: epoch={epoch} blk_left={blk_left} window={window} pool≈{pool:.4f} DUSK")


def _pool_sync_thread() -> None:
    while True:
        time.sleep(300)
        if not _rotation_state.get("enabled"):
            continue
        try:
            real = _pool_fetch_real()
            _log(f"[pool_sync] real balance = {real:.4f} DUSK")
        except Exception as e:
            _log(f"[pool_sync] error: {e}")


def start_rotation_threads():
    import threading as _t
    _t.Thread(target=_rotation_job_worker, daemon=True, name="rotation-worker").start()
    _t.Thread(target=_rotation_heartbeat,  daemon=True, name="rotation-heartbeat").start()
    _t.Thread(target=_pool_sync_thread,    daemon=True, name="pool-sync").start()
