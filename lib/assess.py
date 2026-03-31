"""
assess.py — Stake-info fetching, capacity fetching, and per-provisioner state classification.

Imports: config, wallet.
"""
import json
import re
import threading
import time

from .config import (
    _log, NETWORK_ID, OPERATOR_ADDRESS, NODE_INDICES, RUSK_VERSION, GRAPHQL_URL,
    cfg, _NET, CONTRACT_ID,
)
from .wallet import operator_cmd, wallet_cmd

EPOCH_BLOCKS    = 2160
TOPUP_SLASH_RATE = 0.10

# ── Stake cache ────────────────────────────────────────────────────────────────
_stake_cache:      dict = {}
_stake_cache_lock        = threading.Lock()

# ── _assess_state cache ────────────────────────────────────────────────────────
_ASSESS_CACHE_SECS: int      = 90
_assess_cache_result: dict   = {}
_assess_cache_ts: float      = 0.0
_assess_cache_lock           = threading.Lock()

_own_provisioner_keys: list  = []
_own_provisioner_keys_lock   = threading.Lock()


def _invalidate_assess_cache() -> None:
    global _assess_cache_ts
    with _assess_cache_lock:
        _assess_cache_ts = 0.0


def _assess_state_cached(tip: int, pw: str, force: bool = False) -> dict:
    global _assess_cache_result, _assess_cache_ts
    with _assess_cache_lock:
        age = time.time() - _assess_cache_ts
        if not force and _assess_cache_result and age < _ASSESS_CACHE_SECS:
            _log(f"[assess] cache hit (age={age:.0f}s)")
            return dict(_assess_cache_result)
    result = _assess_state(tip, pw)
    with _assess_cache_lock:
        _assess_cache_result = dict(result)
        _assess_cache_ts     = time.time()
    return result


def _ensure_own_keys(pw: str) -> None:
    """Derive and cache BLS hex keys for our provisioners (used for event matching)."""
    global _own_provisioner_keys
    with _own_provisioner_keys_lock:
        if _own_provisioner_keys:
            return
    keys = []
    for idx in NODE_INDICES:
        sk = cfg(f"prov_{idx}_sk") or ""
        if not sk:
            from config import _get_sk
            sk = _get_sk(idx)
        if sk:
            try:
                r = operator_cmd(
                    f"calculate-payload-stake-activate --provisioner-sk {sk} "
                    f"--amount 1000000000 --network-id {NETWORK_ID()}",
                    timeout=15, password=pw)
                out = r.get("stdout", "") + r.get("stderr", "")
                import re as _re
                hexes = _re.findall(r'[0-9a-f]{32,}', out.lower())
                if hexes:
                    keys.append(max(hexes, key=len))
            except Exception:
                pass
    with _own_provisioner_keys_lock:
        _own_provisioner_keys = keys


def _assess_state(tip: int, pw: str) -> dict:
    """
    Fetch stake-info for all provisioners via operator wallet (--format json).

    Provisioner states (protocol-accurate):
      inactive  — no stake (stake_dusk < 1000)
      seeded    — eligibility_epoch >= current_epoch + 2  (ta=2)
      maturing  — eligibility_epoch == current_epoch + 1  (ta=1)
      active    — eligibility_epoch <= current_epoch       (ta=0)
    """
    _ensure_own_keys(pw)
    op = OPERATOR_ADDRESS()
    nodes_by_idx = {}

    r = operator_cmd(f"{_NET} stake-info --operator {op} --format json",
                     timeout=30, password=pw)
    raw = (r.get("stdout") or r.get("stderr") or "").strip()
    si_list = []
    si_epoch = None
    si_remaining_blocks = None
    if raw.startswith("{") or raw.startswith("["):
        try:
            obj = json.loads(raw)
            if isinstance(obj, list):
                si_list = obj
            elif isinstance(obj, dict):
                si_list             = obj.get("provisioners") or [obj]
                si_epoch            = obj.get("epoch")
                si_remaining_blocks = obj.get("remaining_blocks")
        except Exception:
            pass

    current_epoch = si_epoch

    for idx in NODE_INDICES:
        addr      = cfg(f"prov_{idx}_address") or ""
        entry     = next((e for e in si_list if e.get("account") == addr), None)

        stake_lux  = entry.get("stake")             if entry else None
        locked_lux = entry.get("locked")            if entry else None
        reward_lux = entry.get("reward")            if entry else None
        eligblk    = entry.get("eligibility_block") if entry else None
        eligepoch  = entry.get("eligibility_epoch") if entry else None
        si_status  = (entry.get("status") or "Inactive").lower() if entry else "inactive"

        stake_dusk  = stake_lux  / 1e9 if stake_lux  is not None else 0.0
        locked_dusk = locked_lux / 1e9 if locked_lux is not None else 0.0
        reward_dusk = reward_lux / 1e9 if reward_lux is not None else 0.0

        MIN_STAKE_DUSK = 1000.0

        if stake_dusk < MIN_STAKE_DUSK:
            status, ta = "inactive", None
        elif eligepoch is not None and current_epoch is not None:
            diff = eligepoch - current_epoch
            if diff <= 0:   status, ta = "active",   0
            elif diff == 1: status, ta = "maturing",  1
            else:           status, ta = "seeded",    2
        else:
            if si_status == "active":
                status, ta = "active", 0
            elif si_status in ("maturing", "seeded"):
                if eligblk is not None and tip > 0:
                    blks_away = eligblk - tip
                    ta_calc = max(0, -(-blks_away // EPOCH_BLOCKS))
                    if ta_calc <= 0:   status, ta = "active",  0
                    elif ta_calc == 1: status, ta = "maturing", 1
                    else:              status, ta = "seeded",   2
                else:
                    status = si_status
                    ta     = 1 if si_status == "maturing" else 2
            else:
                status, ta = "inactive", None

        node = {
            "idx":               idx,
            "address":           addr,
            "status":            status,
            "ta":                ta,
            "stake_dusk":        stake_dusk,
            "locked_dusk":       locked_dusk,
            "reward_dusk":       reward_dusk,
            "eligibility_block": eligblk,
            "eligibility_epoch": eligepoch,
            "amount_dusk":       stake_dusk,
            "slashed_dusk":      locked_dusk,
            "rewards_dusk":      reward_dusk,
            "has_stake":         stake_dusk >= MIN_STAKE_DUSK,
        }
        nodes_by_idx[idx] = node
        with _stake_cache_lock:
            _stake_cache[idx] = {
                **node, "ok": r["ok"], "cached": False,
                "si_stake": stake_lux, "si_locked": locked_lux,
                "si_reward": reward_lux, "si_status": si_status,
                "si_eligblk": eligblk, "si_eligepoch": eligepoch,
            }

    active   = [n for n in nodes_by_idx.values() if n["status"] == "active"]
    maturing = [n for n in nodes_by_idx.values() if n["status"] in ("maturing", "seeded")]
    inactive = [n for n in nodes_by_idx.values() if n["status"] == "inactive"]
    label    = f"A:{len(active)} M:{len(maturing)} I:{len(inactive)}"
    return {
        "active": active, "maturing": maturing, "inactive": inactive,
        "by_idx": nodes_by_idx, "label": label,
        "epoch": si_epoch, "remaining_blocks": si_remaining_blocks,
    }


def _fetch_capacity(pw: str) -> dict:
    """Fetch operator capacity from substrate. Returns DUSK values."""
    op  = OPERATOR_ADDRESS()
    r   = operator_cmd(f"{_NET} substrate capacity --operator {op} --format json",
                       timeout=30, password=pw)
    raw = (r.get("stdout") or r.get("stderr") or "").strip()
    cap = {}
    if raw.startswith("{"):
        try:
            obj = json.loads(raw)
            cap["active_current"] = int(obj.get("current_eligibility", obj.get("active_current", 0))) / 1e9
            cap["active_maximum"] = int(obj.get("max_eligibility",     obj.get("active_maximum", 0))) / 1e9
            cap["locked_current"] = int(obj.get("current_stake",       obj.get("locked_current", 0))) / 1e9
            cap["locked_maximum"] = int(obj.get("max_stake",           obj.get("locked_maximum", 0))) / 1e9
        except Exception:
            pass
    return {"active_current": 0.0, "active_maximum": 0.0,
            "locked_current": 0.0, "locked_maximum": 0.0, **cap}


def _max_topup_active(cap: dict) -> float:
    headroom = max(0.0, cap.get("locked_maximum", 0.0) - cap.get("locked_current", 0.0))
    return round(headroom / TOPUP_SLASH_RATE, 4) if TOPUP_SLASH_RATE > 0 else 0.0


def _stake_headroom(cap: dict) -> float:
    return max(0.0, cap.get("locked_maximum", 0.0) - cap.get("locked_current", 0.0))


# ── parse_stake_info (legacy text parser) ─────────────────────────────────────
def parse_stake_info(output: str, current_tip: int = 0) -> dict:
    result = {
        "has_stake": False, "amount_dusk": 0.0, "slashed_dusk": 0.0,
        "transitions": None, "active_block": None, "active_epoch": None,
        "status": "inactive", "status_label": "inactive (0 trans)",
        "staking_address": "", "rewards_dusk": 0.0, "raw": output,
    }
    if not output:
        return result
    output = re.sub(r'\x1b\[[\d;?]*[A-Za-z]', '', output)
    output = re.sub(r'\x1b[()][A-Z0-9]', '', output)

    m = re.search(r"Staking address:\s*([A-Za-z0-9]{40,})", output)
    if m:
        result["staking_address"] = m.group(1).strip()

    if "A stake does not exist" in output:
        return result

    m = re.search(r"Eligible stake:\s*([\d,]+(?:\.\d+)?)\s*DUSK", output)
    if m:
        result["has_stake"]   = True
        result["amount_dusk"] = float(m.group(1).replace(",", ""))
    if not result["has_stake"]:
        return result

    m2 = re.search(r"Reclaimable slashed stake:\s*([\d,]+(?:\.\d+)?)\s*DUSK", output)
    if m2:
        result["slashed_dusk"] = float(m2.group(1).replace(",", ""))

    m_rew = re.search(r"Accumulated rewards:\s*([\d,]+(?:\.\d+)?)\s*DUSK", output)
    if m_rew:
        result["rewards_dusk"] = float(m_rew.group(1).replace(",", ""))

    m_block = re.search(r"Stake active from block\s*#?(\d+).*?Epoch\s+(\d+)", output, re.IGNORECASE)
    if m_block:
        result["active_block"] = int(m_block.group(1))
        result["active_epoch"] = int(m_block.group(2))

    if result["active_epoch"] is not None and current_tip > 0:
        stake_epoch   = result["active_epoch"] - 2
        current_epoch = current_tip // EPOCH_BLOCKS
        result["transitions"] = max(0, current_epoch - stake_epoch)
    else:
        m3 = re.search(r"(?:epoch\s+transitions?|counter)[\s:]+(\d+)", output, re.IGNORECASE)
        if m3:
            result["transitions"] = int(m3.group(1))

    t = result["transitions"]
    if result["active_block"] is not None and current_tip > 0:
        if current_tip >= result["active_block"]:
            result["status"] = "active"
        else:
            stake_epoch   = result["active_epoch"] - 2
            current_epoch = current_tip // EPOCH_BLOCKS
            seen = max(0, current_epoch - stake_epoch)
            result["transitions"] = seen
            result["status"] = "seeded" if seen == 0 else "maturing"
    elif t is not None:
        result["status"] = "seeded" if t == 0 else ("maturing" if t == 1 else "active")
    else:
        result["status"] = "seeded"

    t, s = result["transitions"], result["status"]
    if s == "active":    result["status_label"] = f"active ({t if t is not None else 2}+ trans)"
    elif s == "maturing": result["status_label"] = f"maturing ({t if t is not None else 1} trans)"
    elif s == "seeded":  result["status_label"] = f"seeded ({t if t is not None else 0} trans)"
    else:                result["status_label"] = f"inactive ({t if t is not None else 0} trans)"
    return result
