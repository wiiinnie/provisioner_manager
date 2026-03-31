"""
routes/info.py — Info tab endpoints.

Buttons: stake info · wallet balance · profiles · pool balance · exchange rate ·
         track transaction · query operator rewards · withdraw rewards · recycle
"""
import json
import subprocess
import threading
from flask import Blueprint, jsonify, request

from ..config import (
    _NET, NETWORK_ID, OPERATOR_ADDRESS, CONTRACT_ID, RUSK_VERSION,
    GAS_LIMIT, GRAPHQL_URL, _NODE_STATE_URL, NODE_INDICES,
)
from ..wallet import operator_cmd, wallet_cmd, run_cmd, get_password, WALLET_BIN, OPERATOR_WALLET
from ..assess import _ensure_own_keys, parse_stake_info, EPOCH_BLOCKS
from ..assess import _stake_cache, _stake_cache_lock

bp = Blueprint("info", __name__)


# ── Stake info ────────────────────────────────────────────────────────────────

@bp.route("/api/stake_info/<int:idx>", methods=["GET", "POST"])
def stake_info(idx: int):
    """Per-index stake-info via provisioner wallet."""
    result = wallet_cmd(f"--profile-idx {idx} stake-info", timeout=20, password=get_password())
    return jsonify(result)


@bp.route("/api/stake_info_all", methods=["GET", "POST"])
def stake_info_all():
    """All-provisioner stake-info via operator wallet (JSON format)."""
    pw = get_password()
    op = OPERATOR_ADDRESS()
    if not op:
        return jsonify({"ok": False, "stderr": "operator_address not configured"}), 400
    if pw:
        threading.Thread(target=_ensure_own_keys, args=(pw,), daemon=True).start()
    r = operator_cmd(f"{_NET} stake-info --operator {op} --format json",
                     timeout=30, password=pw)
    stdout = r.get("stdout", "") or ""
    stderr = r.get("stderr", "") or ""
    provisioners = None
    raw = stdout.strip() or stderr.strip()
    if raw.startswith("{") or raw.startswith("["):
        try:
            obj = json.loads(raw)
            provisioners = obj if isinstance(obj, list) else obj.get("provisioners") or [obj]
        except Exception:
            pass
    return jsonify({"ok": r["ok"], "stdout": stdout, "stderr": stderr,
                    "provisioners": provisioners, "duration_ms": r.get("duration_ms", 0)})


# ── Wallet balance ────────────────────────────────────────────────────────────

@bp.route("/api/balance/operator", methods=["GET", "POST"])
def balance_operator():
    pw = (request.get_json(silent=True) or {}).get("password") or get_password()
    return jsonify(operator_cmd("balance", timeout=20, password=pw))


@bp.route("/api/balance/provisioner/<int:idx>", methods=["GET", "POST"])
def balance_provisioner(idx: int):
    pw = (request.get_json(silent=True) or {}).get("password") or get_password()
    return jsonify(wallet_cmd(f"--profile-idx {idx} balance", timeout=20, password=pw))


@bp.route("/api/balance", methods=["GET", "POST"])
def balance():
    pw = (request.get_json(silent=True) or {}).get("password") or get_password()
    return jsonify(operator_cmd("balance", timeout=20, password=pw))


@bp.route("/api/balance/<int:idx>", methods=["GET", "POST"])
def balance_idx(idx: int):
    pw = (request.get_json(silent=True) or {}).get("password") or get_password()
    return jsonify(wallet_cmd(f"--profile-idx {idx} balance", timeout=20, password=pw))


@bp.route("/api/profiles", methods=["GET", "POST"])
def profiles():
    return jsonify(wallet_cmd("profiles", timeout=15, password=get_password()))


# ── Pool balance ──────────────────────────────────────────────────────────────

@bp.route("/api/sozu/contract_balance", methods=["GET", "POST"])
def sozu_contract_balance():
    """Query SOZU pool LUX balance from transfer contract."""
    url = (f"{_NODE_STATE_URL}/on/contracts:"
           "0100000000000000000000000000000000000000000000000000000000000000"
           "/contract_balance")
    r = subprocess.run(
        ["curl", "-s", "-X", "POST", url,
         "-H", f"rusk-version: {RUSK_VERSION}",
         "-H", "Content-Type: application/json",
         "-d", json.dumps(CONTRACT_ID)],
        capture_output=True, text=True, timeout=15,
    )
    raw = r.stdout.strip()
    try:
        lux = int(json.loads(raw))
        return jsonify({"ok": True, "lux": lux, "dusk": lux / 1_000_000_000, "raw": raw})
    except Exception:
        try:
            hex_clean = raw.replace("0x", "")
            if len(hex_clean) == 16:
                lux = int(bytes.fromhex(hex_clean)[::-1].hex(), 16)
                return jsonify({"ok": True, "lux": lux, "dusk": lux / 1_000_000_000, "raw": raw})
        except Exception:
            pass
    return jsonify({"ok": False, "raw": raw, "stderr": "could not decode response"})


@bp.route("/api/provisioner/check_stake")
def provisioner_check_stake():
    return sozu_contract_balance()


@bp.route("/api/sozu/balance_of", methods=["POST"])
def sozu_balance_of():
    """Query SOZU pool token balance for an account."""
    data    = request.get_json() or {}
    account = data.get("account", "")
    if not account:
        return jsonify({"ok": False, "stderr": "missing account"}), 400

    from ..rues import _rues_node_host
    base = _rues_node_host()
    DRIVER    = f"{base}/on/driver:{CONTRACT_ID}"
    CONTRACTS = f"{base}/on/contracts:{CONTRACT_ID}"

    def curl(url, body, ct="application/x-www-form-urlencoded"):
        r = subprocess.run(
            ["curl", "-s", "-X", "POST", url,
             "-H", f"rusk-version: {RUSK_VERSION}",
             "-H", f"Content-Type: {ct}", "-d", body],
            capture_output=True, text=True, timeout=15)
        return r.stdout.strip()

    try:
        encoded  = curl(f"{DRIVER}/encode_input_fn:balance_of", json.dumps(account), "application/json")
        if not encoded or "error" in encoded.lower():
            return jsonify({"ok": False, "stderr": f"encode failed: {encoded}"})
        body_hex = encoded if encoded.startswith("0x") else f"0x{encoded}"
        raw      = curl(f"{CONTRACTS}/balance_of", body_hex)
        raw_hex  = raw if raw.startswith("0x") else f"0x{raw}"
        decoded  = curl(f"{DRIVER}/decode_output_fn:balance_of", raw_hex)
        lux = int(json.loads(decoded))
        return jsonify({"ok": True, "lux": lux, "dusk": lux / 1_000_000_000})
    except Exception as exc:
        return jsonify({"ok": False, "stderr": str(exc)})


# ── Exchange rate ─────────────────────────────────────────────────────────────

@bp.route("/api/sozu/exchange_rate", methods=["GET", "POST"])
def sozu_exchange_rate():
    from ..rues import _rues_node_host
    base = _rues_node_host()
    DRIVER    = f"{base}/on/driver:{CONTRACT_ID}"
    CONTRACTS = f"{base}/on/contracts:{CONTRACT_ID}"

    def curl(url, body, ct="application/x-www-form-urlencoded"):
        r = subprocess.run(
            ["curl", "-s", "-X", "POST", url,
             "-H", f"rusk-version: {RUSK_VERSION}",
             "-H", f"Content-Type: {ct}", "-d", body],
            capture_output=True, text=True, timeout=15)
        return r.stdout.strip()

    try:
        encoded = curl(f"{DRIVER}/encode_input_fn:exchange_rate", "null", "application/json")
        body_hex = encoded if encoded.startswith("0x") else f"0x{encoded}"
        raw      = curl(f"{CONTRACTS}/exchange_rate", body_hex)
        raw_hex  = raw if raw.startswith("0x") else f"0x{raw}"
        decoded  = curl(f"{DRIVER}/decode_output_fn:exchange_rate", raw_hex)
        rate = json.loads(decoded)
        num, den = int(rate["numerator"]), int(rate["denominator"])
        ratio = num / den if den else 0
        return jsonify({"ok": True, "numerator": num, "denominator": den,
                        "rate": round(ratio, 8),
                        "meaning": f"1 SOZU token = {ratio:.6f} DUSK"})
    except Exception as exc:
        return jsonify({"ok": False, "stderr": str(exc)})


# ── Track transaction ─────────────────────────────────────────────────────────

@bp.route("/api/track", methods=["POST", "GET"])
def track_transaction():
    if request.method == "POST":
        tx_hash = (request.get_json() or {}).get("hash", "").strip()
    else:
        tx_hash = request.args.get("hash", "").strip()
    if not tx_hash:
        return jsonify({"ok": False, "stderr": "hash required"}), 400
    cmd = f"{WALLET_BIN} -w {OPERATOR_WALLET} track --transaction {tx_hash} --format json"
    r   = run_cmd(cmd, timeout=120)
    stdout, stderr = r.get("stdout", "") or "", r.get("stderr", "") or ""
    parsed = None
    raw = stdout.strip() or stderr.strip()
    if raw.startswith("{"):
        try:
            parsed = json.loads(raw)
        except Exception:
            pass
    return jsonify({"ok": r["ok"], "hash": tx_hash, "stdout": stdout,
                    "stderr": stderr, "parsed": parsed, "duration_ms": r.get("duration_ms", 0)})


# ── Operator rewards ──────────────────────────────────────────────────────────

@bp.route("/api/provisioner/query_operator_rewards", methods=["POST"])
def query_operator_rewards():
    pw      = (request.get_json() or {}).get("password", "")
    r       = operator_cmd(f"{_NET} pool balance-of --format json", timeout=30, password=pw)
    raw_out = (r.get("stdout") or r.get("stderr") or "").strip()
    balance_lux, balance_dusk, account = 0, 0.0, ""
    try:
        obj          = json.loads(raw_out)
        balance_lux  = int(obj.get("balance", 0))
        balance_dusk = balance_lux / 1_000_000_000
        account      = obj.get("account", "")
    except Exception:
        pass
    return jsonify({
        "ok": r["ok"], "balance_lux": balance_lux,
        "balance_dusk": round(balance_dusk, 9), "account": account,
        "stdout": f"Operator rewards: {balance_dusk:.6f} DUSK ({balance_lux} LUX)",
        "stderr": r.get("stderr", "") if not r["ok"] else "",
        "duration_ms": r.get("duration_ms", 0), "raw": raw_out,
    })


@bp.route("/api/provisioner/withdraw_rewards", methods=["POST"])
def provisioner_withdraw_rewards():
    pw  = (request.get_json() or {}).get("password", "")
    r1  = operator_cmd(f"{_NET} pool balance-of --format json", timeout=30, password=pw)
    raw = (r1.get("stdout") or r1.get("stderr") or "").strip()
    try:
        obj          = json.loads(raw)
        balance_lux  = int(obj.get("balance", 0))
        balance_dusk = balance_lux / 1_000_000_000
    except Exception as e:
        return jsonify({"ok": False, "step": "query_balance",
                        "stderr": f"Could not parse balance-of: {e}\nRaw: {raw}"})
    if not r1["ok"]:
        return jsonify({"ok": False, "step": "query_balance", "stderr": r1.get("stderr", raw)})
    if balance_lux == 0:
        return jsonify({"ok": True, "step": "nothing_to_withdraw", "balance_lux": 0,
                        "balance_dusk": 0.0,
                        "stdout": "Operator rewards balance is 0 — nothing to withdraw"})
    r2 = operator_cmd(f"{_NET} pool sozu-unstake --amount {balance_lux} --format json",
                      timeout=60, password=pw)
    stdout_msg = (f"Balance: {balance_dusk:.6f} DUSK ({balance_lux} LUX)\n"
                  f"Withdrawing: {balance_lux} LUX\n"
                  + (r2.get("stdout", "") or r2.get("stderr", "")))
    return jsonify({
        "ok": r2["ok"], "step": "sozu_unstake",
        "balance_lux": balance_lux, "balance_dusk": round(balance_dusk, 9),
        "withdraw_lux": balance_lux, "withdrawn_dusk": balance_dusk,
        "stdout": stdout_msg.strip(), "stderr": r2.get("stderr", ""),
        "duration_ms": r2.get("duration_ms", 0),
    })


# ── Recycle ───────────────────────────────────────────────────────────────────

@bp.route("/api/sozu/recycle", methods=["POST"])
def sozu_recycle():
    pw = (request.get_json() or {}).get("password", "")
    r  = operator_cmd(f"{_NET} pool recycle --format json",
                      timeout=60, password=pw, gas_limit=GAS_LIMIT())
    return jsonify({"ok": r["ok"], "stdout": r.get("stdout", ""),
                    "stderr": r.get("stderr", ""), "duration_ms": r.get("duration_ms", 0)})


# ── Network tip ───────────────────────────────────────────────────────────────

@bp.route("/api/network/tip", methods=["GET", "POST"])
def network_tip():
    import urllib.request as ur
    query = '{ block(height: -1) { header { height } } }'
    req   = ur.Request(GRAPHQL_URL, data=query.encode(),
                       headers={"rusk-version": RUSK_VERSION,
                                "Content-Type": "application/graphql"},
                       method="POST")
    try:
        with ur.urlopen(req, timeout=8) as resp:
            payload = json.loads(resp.read())
        block  = payload.get("block") or payload.get("data", {}).get("block", {})
        height = int(block["header"]["height"])
        return jsonify({"ok": True, "height": height})
    except Exception as exc:
        return jsonify({"ok": False, "stderr": str(exc)})


# ── Provisioner live (stake info + epoch) ─────────────────────────────────────

@bp.route("/api/provisioner/live", methods=["GET", "POST"])
def provisioner_live():
    """Live stake info for all provisioners + epoch countdown."""
    import urllib.request as _ur
    tip = None
    try:
        q   = '{ block(height: -1) { header { height } } }'
        req = _ur.Request(GRAPHQL_URL, data=q.encode(),
                          headers={"rusk-version": RUSK_VERSION,
                                   "Content-Type": "application/graphql"},
                          method="POST")
        with _ur.urlopen(req, timeout=6) as resp:
            p = json.loads(resp.read())
        b   = p.get("block") or p.get("data", {}).get("block", {})
        tip = int(b["header"]["height"])
    except Exception:
        tip = None

    epoch                   = (tip // EPOCH_BLOCKS + 1)        if tip is not None else None
    blocks_in_epoch         = (tip % EPOCH_BLOCKS)             if tip is not None else None
    blocks_until_transition = (EPOCH_BLOCKS - blocks_in_epoch) if blocks_in_epoch is not None else None

    pw    = (request.get_json(silent=True) or {}).get("password", "")
    nodes = {}
    for idx in NODE_INDICES:
        r    = wallet_cmd(f"--profile-idx {idx} stake-info", timeout=20, password=pw)
        info = parse_stake_info(r.get("stdout", "") + r.get("stderr", ""), current_tip=tip or 0)
        with _stake_cache_lock:
            authoritative = (info["has_stake"]
                             or "A stake does not exist" in (r.get("stdout", "") + r.get("stderr", "")))
            if authoritative:
                _stake_cache[idx] = {**info, "ok": r["ok"], "cached": False}
            elif idx in _stake_cache:
                info = dict(_stake_cache[idx])
                info["cached"] = True
        nodes[str(idx)] = {**info, "ok": r["ok"]}

    return jsonify({
        "tip": tip, "epoch": epoch,
        "blocks_in_epoch": blocks_in_epoch,
        "blocks_until_transition": blocks_until_transition,
        "epoch_blocks": EPOCH_BLOCKS,
        "nodes": nodes,
    })


# ── Stake / unstake / withdraw (provisioner wallet) ──────────────────────────

@bp.route("/api/stake", methods=["POST"])
def stake():
    data   = request.get_json() or {}
    idx, amount = data.get("idx"), data.get("amount")
    if idx is None or amount is None:
        return jsonify({"ok": False, "stderr": "missing idx or amount"}), 400
    return jsonify(wallet_cmd(f"--profile-idx {idx} stake --amt {amount}",
                              timeout=60, password=data.get("password", "")))


@bp.route("/api/unstake", methods=["POST"])
def unstake():
    data = request.get_json() or {}
    idx  = data.get("idx")
    if idx is None:
        return jsonify({"ok": False, "stderr": "missing idx"}), 400
    return jsonify(wallet_cmd(f"--profile-idx {idx} unstake",
                              timeout=60, password=data.get("password", "")))


@bp.route("/api/withdraw_reward", methods=["POST"])
def withdraw_reward():
    data = request.get_json() or {}
    idx  = data.get("idx")
    if idx is None:
        return jsonify({"ok": False, "stderr": "missing idx"}), 400
    return jsonify(wallet_cmd(f"--profile-idx {idx} withdraw-reward",
                              timeout=60, password=data.get("password", "")))
