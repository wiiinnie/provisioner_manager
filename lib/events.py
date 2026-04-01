"""
events.py — RUES event action engine.

Called by rues.py on every decoded event. Evaluates rules and triggers
actions such as auto-allocating incoming deposits to provisioners.

Architecture:
  rues.py  →  on_event(topic, decoded, block_height)
           →  _handle_deposit()   — checks thresholds, picks target, calls operator_cmd
           →  _deposit_log        — ring buffer shown in dashboard deposit panel

⚠ TODO (remind Matthias):
  - Target selection is currently: any MATURING non-master provisioner.
    Still missing conditions:
      • Active node top-up logic (slash headroom check)
      • Seeded node preference ordering
      • Split across multiple nodes when amount exceeds per-node capacity
      • Configurable target override (e.g. always prov[2] for deposits)
      • Minimum stake balance enforcement (SEED_DUSK reservation)
"""

import json
import threading
import time
from collections import deque
from datetime import datetime

from .config import _log, cfg, CONTRACT_ID

EPOCH_BLOCKS: int = 2160  # Dusk network constant

# ── Deposit action log ────────────────────────────────────────────────────────
_deposit_log: deque = deque(maxlen=200)
_deposit_log_lock    = threading.Lock()


def _dlog(entry: dict) -> None:
    entry.setdefault("ts", datetime.now().strftime("%H:%M:%S.%f")[:-3])
    with _deposit_log_lock:
        _deposit_log.appendleft(entry)
    _log(f"[events] {entry.get('msg', '')}")


def get_deposit_log() -> list:
    with _deposit_log_lock:
        return list(_deposit_log)


# ── Pending deposit tracking ──────────────────────────────────────────────────
# When a deposit arrives we record it; when activate confirms we compute timing.
_pending_deposits: dict = {}          # tx_hash → {ts, amount_dusk, block_height}
_pending_lock            = threading.Lock()

# Track activate events so we can match them back to deposits
_pending_activations: dict = {}       # provisioner_addr → {deposit_ts, amount_dusk, deposit_block}


def _now_ts() -> str:
    return datetime.now().strftime("%H:%M:%S.%f")[:-3]


# ── Main entry point ──────────────────────────────────────────────────────────

def on_event(topic: str, decoded: dict, block_height: int | None = None) -> None:
    """Called by rues.py for every successfully decoded event."""
    if not isinstance(decoded, dict):
        return
    try:
        if topic in ("deposit", "sozu-stake"):
            _handle_deposit(decoded, block_height)
        elif topic == "reward":
            _handle_reward(decoded, block_height)
        elif topic == "activate":
            _handle_activate(decoded, block_height)
        elif topic == "tx/executed":
            _handle_tx_executed(decoded, block_height)
    except Exception as e:
        _log(f"[events] on_event error ({topic}): {e}")


# ── Deposit handler ───────────────────────────────────────────────────────────

def _handle_reward(decoded: dict, block_height: int | None) -> None:
    """
    A reward contract event arrived.
    operation == "recycle"   → reward from a recycle tx, fee_pool goes back to pool
    operation == "terminate" → reward from provisioner termination, pool gets delegator rewards

    fee_pool is the DUSK available for allocation. Treat exactly like a deposit:
    check threshold and allocate to a maturing provisioner.
    """
    operation   = decoded.get("operation", "unknown")
    fee_pool_lux = int(str(decoded.get("fee_pool") or 0))
    fee_pool_dusk = fee_pool_lux / 1e9

    _dlog({
        "type":      f"reward_{operation}",
        "msg":       f"reward/{operation} fee_pool={fee_pool_dusk:,.4f} DUSK at blk#{block_height}",
        "amount":    fee_pool_dusk,
        "operation": operation,
        "block":     block_height,
        "ok":        True,
    })

    # Re-use deposit logic with the fee_pool amount
    _handle_deposit(
        {"amount": fee_pool_lux, "hash": "", "account": "reward"},
        block_height,
        label=f"reward/{operation}",
    )


def _handle_deposit(decoded: dict, block_height: int | None, label: str = "deposit") -> None:
    """
    A deposit/sozu-stake event arrived. Evaluate thresholds and trigger
    stake-activate if conditions are met.

    Current target selection (⚠ TODO — see module docstring):
      Pick any MATURING provisioner that is NOT the master node.
      Falls back to any non-master provisioner if none are maturing.
    """
    amount_lux  = decoded.get("amount") or decoded.get("value") or 0
    amount_dusk = float(amount_lux) / 1e9 if amount_lux else 0.0
    depositor   = decoded.get("account") or decoded.get("sender") or ""
    tx_hash     = decoded.get("hash") or ""

    # Read thresholds and window from config
    min_dep          = float(cfg("min_deposit_dusk")        or 100.0)
    snatch_min_dep   = float(cfg("snatch_min_deposit_dusk") or 100.0)
    rot_win          = int(cfg("rotation_window")           or 41)
    snatch_win       = int(cfg("snatch_window")             or 11)
    _master = cfg("master_idx")
    master_idx     = int(_master) if _master is not None else -1

    # Determine current epoch window from block height
    window = "regular"
    if block_height:
        blk_left = EPOCH_BLOCKS - (block_height % EPOCH_BLOCKS)
        if blk_left <= snatch_win:
            window = "snatch"
        elif blk_left <= rot_win:
            window = "rotation"

    # Apply threshold
    threshold = snatch_min_dep if window == "snatch" else min_dep
    # TODO: re-enable threshold check once allocation is confirmed working
    # if amount_dusk < threshold:
    if False:
        _dlog({
            "type":    "deposit_skipped",
            "msg":     f"{label} {amount_dusk:,.4f} DUSK < threshold {threshold:,.0f} DUSK ({window}) — skipped",
            "amount":  amount_dusk,
            "window":  window,
            "ok":      False,
        })
        return

    _dlog({
        "type":      "deposit_received",
        "msg":       f"{label} {amount_dusk:,.4f} DUSK in {window} window (≥{threshold:,.0f}) — allocating…",
        "amount":    amount_dusk,
        "window":    window,
        "depositor": depositor,
        "block":     block_height,
        "ok":        True,
    })

    # Record for timing calculation
    with _pending_lock:
        _pending_deposits[tx_hash] = {
            "ts":           _now_ts(),
            "wall_ts":      time.time(),
            "amount_dusk":  amount_dusk,
            "block_height": block_height,
        }

    # Pick target provisioner
    target_idx, target_addr = _pick_target(master_idx)
    if target_idx is None:
        _dlog({
            "type": "deposit_error",
            "msg":  f"no eligible provisioner found for {label} — all active or master-only",
            "amount": amount_dusk,
            "ok":   False,
        })
        return

    _dlog({
        "type": "deposit_allocating",
        "msg":  f"allocating {amount_dusk:,.4f} DUSK → prov[{target_idx}] ({target_addr[:12]}…) [{label}]",
        "amount": amount_dusk,
        "ok":   True,
    })

    # Fire allocation in background thread — operator_cmd is blocking
    threading.Thread(
        target=_do_allocate,
        args=(target_idx, target_addr, amount_dusk, block_height, tx_hash, depositor),
        daemon=True,
    ).start()


def _pick_target(master_idx: int) -> tuple:
    """
    Pick a target provisioner for deposit allocation.
    Priority: MATURING non-master → any non-master with has_stake=False (inactive).

    ⚠ TODO: extend with active top-up logic, seeded preference, multi-node split.
    Returns (idx, address) or (None, None).
    """
    from .assess import _assess_state
    from .config import NODE_INDICES

    try:
        pw = ""
        try:
            from .wallet import get_password
            pw = get_password() or ""
        except Exception:
            pass

        st = _assess_state(0, pw)
        nodes = st.get("by_idx", {})

        # Priority 1: maturing non-master
        for idx in NODE_INDICES:
            if idx == master_idx:
                continue
            node = nodes.get(idx, {})
            if node.get("status") == "maturing":
                addr = node.get("staking_address") or cfg(f"prov_{idx}_address") or ""
                if addr:
                    return idx, addr

        # Priority 2: inactive non-master
        for idx in NODE_INDICES:
            if idx == master_idx:
                continue
            node = nodes.get(idx, {})
            if node.get("status") in ("inactive", "seeded"):
                addr = node.get("staking_address") or cfg(f"prov_{idx}_address") or ""
                if addr:
                    return idx, addr

    except Exception as e:
        _log(f"[events] _pick_target error: {e}")

    return None, None


def _do_allocate(
    idx: int, addr: str, amount_dusk: float,
    deposit_block: int | None, tx_hash: str, depositor: str
) -> None:
    """Run stake-activate and log result."""
    from .wallet import operator_cmd, get_password, WALLET_PATH
    from .config import _NET

    pw         = get_password() or ""
    amount_lux = int(amount_dusk * 1_000_000_000)
    t_start    = time.time()

    # Track for activate confirmation timing
    with _pending_lock:
        _pending_activations[addr] = {
            "deposit_ts":    _now_ts(),
            "wall_ts":       t_start,
            "amount_dusk":   amount_dusk,
            "deposit_block": deposit_block,
            "tx_hash":       tx_hash,
            "prov_idx":      idx,
        }

    r = operator_cmd(
        f"{_NET} pool stake-activate --skip-confirmation "
        f"--amount {amount_lux} "
        f"--provisioner {addr} "
        f"--provisioner-wallet {WALLET_PATH} "
        f"--provisioner-password '{pw}'",
        timeout=120, password=pw,
    )

    elapsed = round(time.time() - t_start, 1)

    if r.get("ok"):
        _dlog({
            "type":     "deposit_allocated",
            "msg":      (f"✓ stake-activate OK — prov[{idx}] "
                         f"{amount_dusk:,.4f} DUSK in {elapsed}s "
                         f"(block ~{deposit_block})"),
            "prov_idx": idx,
            "prov_addr": addr,
            "amount":   amount_dusk,
            "elapsed_s": elapsed,
            "block":    deposit_block,
            "ok":       True,
        })
    else:
        err = r.get("stderr") or r.get("stdout") or "unknown error"
        _dlog({
            "type":     "deposit_failed",
            "msg":      f"✗ stake-activate FAILED prov[{idx}]: {err}",
            "prov_idx": idx,
            "prov_addr": addr,
            "amount":   amount_dusk,
            "elapsed_s": elapsed,
            "error":    err,
            "ok":       False,
        })


# ── Activate confirmation handler ─────────────────────────────────────────────

def _handle_activate(decoded: dict, block_height: int | None) -> None:
    """
    A stake_activate contract event confirmed on-chain.
    Match back to pending allocation and report timing.
    """
    prov_addr = decoded.get("provisioner") or decoded.get("account") or ""
    amount_lux = decoded.get("amount") or decoded.get("value") or 0
    amount_dusk = float(amount_lux) / 1e9

    with _pending_lock:
        pending = _pending_activations.pop(prov_addr, None)

    if pending:
        elapsed = round(time.time() - pending["wall_ts"], 1)
        _dlog({
            "type":       "activate_confirmed",
            "msg":        (f"✓ activate confirmed on-chain — prov ({prov_addr[:12]}…) "
                           f"{amount_dusk:,.4f} DUSK "
                           f"block #{block_height} "
                           f"· {elapsed}s from deposit to activation"),
            "prov_addr":  prov_addr,
            "amount":     amount_dusk,
            "block":      block_height,
            "deposit_block": pending.get("deposit_block"),
            "elapsed_s":  elapsed,
            "ok":         True,
        })


def _handle_tx_executed(decoded: dict, block_height: int | None) -> None:
    """
    tx/executed — check if it's a failed stake-activate we care about.
    """
    inner = decoded.get("inner") or decoded
    call  = inner.get("call") or {}
    err   = decoded.get("err")
    if err and call.get("fn_name") == "stake_activate":
        prov_decoded = call.get("_fn_args_decoded") or {}
        prov_addr    = (prov_decoded.get("keys") or {}).get("account") or ""
        _dlog({
            "type":    "activate_tx_failed",
            "msg":     f"✗ stake_activate tx failed at block #{block_height}: {err}",
            "prov_addr": prov_addr,
            "block":   block_height,
            "error":   str(err),
            "ok":      False,
        })
