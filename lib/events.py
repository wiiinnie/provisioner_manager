"""
events.py — RUES event action engine.

Allocation rules per window:
  Regular / Rotation:
    deposit           → allocate
    reward/recycle    → allocate
    reward/terminate  → allocate ONLY if own provisioner was terminated

  Snatch:
    deposit           → allocate
    reward/recycle    → allocate
    reward/terminate  → always allocate

3-step deposit snatch flow:
  Step 1 (deposit_received):  event arrived, stake-activate fired
  Step 2 (tx_confirmed/failed): tx/executed result — win/loss decided here
  Step 3 (activate_confirmed): activate contract event — stake is live on-chain

⚠ TODO:
  - Target selection incomplete: active top-up, multi-node split, capacity check,
    seed reservation, configurable override
  - Threshold check bypassed (if False) — re-enable when stable
  - Deposit ownership filtering (depositor address) — "we need this later"
  - block_height None for contract events — wire up block cache backfill
"""

import threading
import time
from collections import deque
from datetime import datetime

from .config import _log, cfg

EPOCH_BLOCKS: int = 2160

_deposit_log:      deque = deque(maxlen=200)
_deposit_log_lock         = threading.Lock()

_race_wins:   int = 0
_race_losses: int = 0
_race_lock         = threading.Lock()

_recent_alloc:      dict = {}
_recent_alloc_lock         = threading.Lock()

_pending_activations: dict = {}
_pending_lock               = threading.Lock()


def _dlog(entry: dict) -> None:
    entry.setdefault("ts", datetime.now().strftime("%H:%M:%S.%f")[:-3])
    with _deposit_log_lock:
        _deposit_log.appendleft(entry)
    _log(f"[events] {entry.get('msg', '')}")


def get_deposit_log() -> list:
    with _deposit_log_lock:
        return list(_deposit_log)


def get_race_counters() -> dict:
    with _race_lock:
        return {"wins": _race_wins, "losses": _race_losses}


def _now_ts() -> str:
    return datetime.now().strftime("%H:%M:%S.%f")[:-3]


def _own_addresses() -> set:
    addrs = set()
    for k in ("prov_0_address", "prov_1_address", "prov_2_address"):
        v = cfg(k)
        if v:
            addrs.add(v)
    return addrs


def _current_window(block_height: int | None) -> str:
    rot_win    = int(cfg("rotation_window") or 41)
    snatch_win = int(cfg("snatch_window")   or 11)
    if not block_height:
        return "regular"
    blk_left = EPOCH_BLOCKS - (block_height % EPOCH_BLOCKS)
    if blk_left <= snatch_win:
        return "snatch"
    if blk_left <= rot_win:
        return "rotation"
    return "regular"


def on_event(topic: str, decoded: dict, block_height: int | None = None) -> None:
    if not isinstance(decoded, dict):
        return
    try:
        if topic == "deposit":
            _handle_deposit(decoded, block_height, label="deposit")
        elif topic == "reward":
            _handle_reward(decoded, block_height)
        elif topic == "activate":
            _handle_activate(decoded, block_height)
        elif topic == "tx/executed":
            _handle_tx_executed(decoded, block_height)
    except Exception as e:
        _log(f"[events] on_event({topic}) error: {e}")


def _handle_reward(decoded: dict, block_height: int | None) -> None:
    operation   = decoded.get("operation", "unknown")
    amount_lux  = int(str(decoded.get("amount") or 0))
    window      = _current_window(block_height)

    if operation == "recycle":
        _handle_deposit(
            {"amount": amount_lux, "hash": "", "account": "reward"},
            block_height, label="reward/recycle",
        )
    elif operation == "terminate":
        if window == "snatch":
            _handle_deposit(
                {"amount": amount_lux, "hash": "", "account": "reward"},
                block_height, label="reward/terminate",
            )
        else:
            provisioners = decoded.get("provisioners") or []
            own = _own_addresses()
            own_terminated = [p for p in provisioners if p in own]
            if own_terminated:
                _handle_deposit(
                    {"amount": amount_lux, "hash": "", "account": "reward"},
                    block_height, label="reward/terminate(own)",
                )
            else:
                _log(f"[events] reward/terminate: no own prov in list — skip ({window})")
    else:
        _log(f"[events] reward: unknown operation={operation!r} — skip")


def _handle_deposit(decoded: dict, block_height: int | None, label: str = "deposit") -> None:
    amount_lux  = int(str(decoded.get("amount") or decoded.get("value") or 0))
    amount_dusk = amount_lux / 1e9
    depositor   = decoded.get("account") or decoded.get("sender") or ""

    dedup_key = f"{label}|{amount_dusk:.4f}"
    now = time.time()
    with _recent_alloc_lock:
        if now - _recent_alloc.get(dedup_key, 0) < 30:
            _log(f"[events] dedup skip {label} {amount_dusk:.4f}")
            return
        _recent_alloc[dedup_key] = now
        for k in list(_recent_alloc):
            if now - _recent_alloc[k] > 60:
                del _recent_alloc[k]

    window         = _current_window(block_height)
    min_dep        = float(cfg("min_deposit_dusk")        or 100.0)
    snatch_min_dep = float(cfg("snatch_min_deposit_dusk") or 100.0)
    threshold      = snatch_min_dep if window == "snatch" else min_dep

    # TODO: re-enable: if amount_dusk < threshold:
    if False:
        _dlog({"type":"deposit_skipped","step":0,
               "msg":f"{label} {amount_dusk:,.4f} DUSK < {threshold:,.0f} min ({window}) — skip",
               "amount":amount_dusk,"window":window,"ok":False})
        return

    _master    = cfg("master_idx")
    master_idx = int(_master) if _master is not None else -1
    target_idx, target_addr = _pick_target(master_idx)

    if target_idx is None:
        _dlog({"type":"deposit_error","step":1,
               "msg":f"no eligible provisioner for {label} — all active or master-only",
               "amount":amount_dusk,"window":window,"ok":False})
        return

    # Step 1
    _dlog({
        "type":      "deposit_received",
        "step":      1,
        "msg":       f"{label} {amount_dusk:,.4f} DUSK ({window}) → activating prov[{target_idx}]…",
        "amount":    amount_dusk,
        "window":    window,
        "depositor": depositor,
        "block":     block_height,
        "prov_idx":  target_idx,
        "prov_addr": target_addr,
        "ok":        True,
    })

    threading.Thread(
        target=_do_allocate,
        args=(target_idx, target_addr, amount_dusk, block_height),
        daemon=True,
    ).start()


def _pick_target(master_idx: int) -> tuple:
    """MATURING non-master → inactive/seeded non-master. ⚠ TODO: see module docstring."""
    from .assess import _assess_state
    from .config import NODE_INDICES
    try:
        pw = ""
        try:
            from .wallet import get_password
            pw = get_password() or ""
        except Exception:
            pass
        st    = _assess_state(0, pw)
        nodes = st.get("by_idx", {})
        for idx in NODE_INDICES:
            if idx == master_idx:
                continue
            if nodes.get(idx, {}).get("status") == "maturing":
                addr = nodes[idx].get("staking_address") or cfg(f"prov_{idx}_address") or ""
                if addr:
                    return idx, addr
        for idx in NODE_INDICES:
            if idx == master_idx:
                continue
            if nodes.get(idx, {}).get("status") in ("inactive", "seeded"):
                addr = nodes.get(idx, {}).get("staking_address") or cfg(f"prov_{idx}_address") or ""
                if addr:
                    return idx, addr
    except Exception as e:
        _log(f"[events] _pick_target error: {e}")
    return None, None


def _do_allocate(idx: int, addr: str, amount_dusk: float, deposit_block: int | None) -> None:
    """Fire stake-activate. Step 2 result comes from tx/executed event."""
    from .wallet import operator_cmd, get_password, WALLET_PATH
    from .config import _NET

    pw         = get_password() or ""
    amount_lux = int(amount_dusk * 1_000_000_000)
    t_start    = time.time()

    with _pending_lock:
        _pending_activations[addr] = {
            "wall_ts":       t_start,
            "amount_dusk":   amount_dusk,
            "deposit_block": deposit_block,
            "prov_idx":      idx,
        }

    operator_cmd(
        f"{_NET} pool stake-activate --skip-confirmation "
        f"--amount {amount_lux} "
        f"--provisioner {addr} "
        f"--provisioner-wallet {WALLET_PATH} "
        f"--provisioner-password '{pw}'",
        timeout=120, password=pw,
    )
    # Result logged by _handle_tx_executed when tx/executed event arrives


def _handle_tx_executed(decoded: dict, block_height: int | None) -> None:
    """Step 2: tx/executed for stake_activate — win or loss."""
    global _race_wins, _race_losses
    inner = decoded.get("inner") or decoded
    call  = inner.get("call") or {}
    if call.get("fn_name") != "stake_activate":
        return

    prov_decoded = call.get("_fn_args_decoded") or {}
    prov_addr    = (prov_decoded.get("keys") or {}).get("account") or ""
    amount_lux   = int(str(prov_decoded.get("value") or 0))
    amount_dusk  = amount_lux / 1e9
    err          = decoded.get("err")
    gas_spent    = decoded.get("gas_spent")
    gas_limit    = (inner.get("fee") or {}).get("gas_limit")

    own = _own_addresses()
    if prov_addr and own and prov_addr not in own:
        return

    with _pending_lock:
        pending = _pending_activations.get(prov_addr)
    elapsed = round(time.time() - pending["wall_ts"], 1) if pending else None

    if not err:
        with _race_lock:
            _race_wins += 1
        wins, losses = _race_wins, _race_losses
        _dlog({
            "type":      "tx_confirmed",
            "step":      2,
            "msg":       (f"✓ tx confirmed blk #{block_height}"
                          + (f" · {elapsed}s" if elapsed else "")
                          + f" · gas {gas_spent}/{gas_limit}"
                          + f" · W{wins}/L{losses}"),
            "prov_addr": prov_addr,
            "amount":    amount_dusk,
            "block":     block_height,
            "elapsed_s": elapsed,
            "wins":      wins,
            "losses":    losses,
            "ok":        True,
        })
    else:
        with _race_lock:
            _race_losses += 1
        wins, losses = _race_wins, _race_losses
        _dlog({
            "type":      "tx_failed",
            "step":      2,
            "msg":       (f"✗ tx failed blk #{block_height}: {err}"
                          + (f" · {elapsed}s" if elapsed else "")
                          + f" · W{wins}/L{losses}"),
            "prov_addr": prov_addr,
            "amount":    amount_dusk,
            "block":     block_height,
            "elapsed_s": elapsed,
            "error":     str(err),
            "wins":      wins,
            "losses":    losses,
            "ok":        False,
        })


def _handle_activate(decoded: dict, block_height: int | None) -> None:
    """Step 3: activate contract event — stake is now live on-chain."""
    prov_addr   = decoded.get("provisioner") or decoded.get("account") or ""
    amount_lux  = int(str(decoded.get("amount") or decoded.get("value") or 0))
    amount_dusk = amount_lux / 1e9

    own = _own_addresses()
    if prov_addr and own and prov_addr not in own:
        return

    with _pending_lock:
        pending = _pending_activations.pop(prov_addr, None)
    elapsed = round(time.time() - pending["wall_ts"], 1) if pending else None

    _dlog({
        "type":      "activate_confirmed",
        "step":      3,
        "msg":       (f"✓ stake live on-chain blk #{block_height}"
                      + (f" · {elapsed}s total" if elapsed else "")),
        "prov_addr": prov_addr,
        "amount":    amount_dusk,
        "block":     block_height,
        "elapsed_s": elapsed,
        "ok":        True,
    })
