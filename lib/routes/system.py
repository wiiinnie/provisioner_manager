"""
routes/system.py — System, config, nodes, rotation status, RUES debug endpoints.
"""
import json
import os
import time
from datetime import datetime
from flask import Blueprint, jsonify, request, send_file

from ..config import (
    _log, NETWORK, NETWORK_ID, CONTRACT_ID, _NODE_STATE_URL, RUSK_VERSION,
    WALLET_BIN, WALLET_PATH, NODE_INDICES, PORT,
    cfg, _cfg, _CONFIG_DEFAULTS, _load_config, _save_config,
    _load_sks, _save_sks,
)
from ..wallet import (
    run_cmd, wallet_cmd, get_password, _cache_wallet_pw,
    _cmd_log, _cmd_log_lock, WALLET_BIN, OPERATOR_WALLET,
)

bp = Blueprint("system", __name__)

SERVER_VERSION = "1.0.5"


# ── Health ────────────────────────────────────────────────────────────────────

@bp.route("/api/ping")
def ping():
    return jsonify({"ok": True, "ts": datetime.now().isoformat()})


@bp.route("/")
def index():
    # server.py lives at repo root alongside provisioner_dashboard*.html
    # __file__ is lib/routes/system.py → go up two levels to reach repo root
    here   = os.path.dirname(os.path.abspath(__file__))   # lib/routes/
    root   = os.path.dirname(os.path.dirname(here))        # repo root
    import glob
    candidates = sorted(glob.glob(os.path.join(root, "provisioner_dashboard*.html")))
    if not candidates:
        return ("Dashboard HTML not found.", 404)
    return send_file(candidates[-1])


@bp.route("/api/status")
def status():
    binary_ok = run_cmd(f"which {WALLET_BIN}")["ok"]
    return jsonify({
        "binary_found":  binary_ok,
        "wallet_dir":    WALLET_PATH,
        "wallet_dir_ok": os.path.isdir(WALLET_PATH),
        "network":       NETWORK,
        "nodes":         NODE_INDICES,
        "ts":            datetime.now().isoformat(),
    })


@bp.route("/api/version")
def api_version():
    return jsonify({
        "version":  SERVER_VERSION,
        "network":  NETWORK,
        "node_url": _NODE_STATE_URL,
        "pool":     CONTRACT_ID,
    })


@bp.route("/api/debug/wallet", methods=["POST"])
def debug_wallet():
    data    = request.get_json() or {}
    pw      = data.get("password", "")
    subcmd  = data.get("subcmd", "profiles")
    timeout = data.get("timeout", 20)
    if subcmd == "help":
        result = run_cmd(f"{WALLET_BIN} --help", timeout=10)
    elif subcmd == "help_network":
        result = run_cmd(f"{WALLET_BIN} -w {WALLET_PATH} --help", timeout=10)
    else:
        result = wallet_cmd(subcmd, timeout=timeout, password=pw)
    return jsonify(result)


# ── Config ────────────────────────────────────────────────────────────────────

@bp.route("/api/config", methods=["GET"])
def get_config():
    cfg("network_id")   # triggers reload if stale
    safe = {k: v for k, v in _cfg.items() if not k.endswith("_sk")}
    sks  = _load_sks()
    safe["prov_0_sk_set"] = bool(sks.get("prov_0_sk"))
    safe["prov_1_sk_set"] = bool(sks.get("prov_1_sk"))
    safe["prov_2_sk_set"] = bool(sks.get("prov_2_sk"))
    return jsonify(safe)


@bp.route("/api/config", methods=["POST"])
def set_config():
    data    = request.get_json() or {}
    current = dict(_cfg) if _cfg else dict(_CONFIG_DEFAULTS)

    int_keys   = ("network_id", "rotation_window", "snatch_window", "backfill_blocks",
                  "master_idx", "gas_limit",
                  "node_0_ws_port", "node_1_ws_port", "node_2_ws_port")
    float_keys = ("min_deposit_dusk", "snatch_min_deposit_dusk")
    str_keys   = ("contract_address", "operator_address",
                  "prov_0_address", "prov_1_address", "prov_2_address",
                  "node_0_log", "node_1_log", "node_2_log")

    for k in int_keys:
        if k in data:
            current[k] = max(100000, int(data[k])) if k == "gas_limit" else int(data[k])
    for k in float_keys:
        if k in data:
            current[k] = float(data[k])
    for k in str_keys:
        if k in data:
            current[k] = str(data[k]).strip()

    if "master_stake_pct" in data:
        val = float(data["master_stake_pct"])
        current["master_stake_pct"] = round(val / 100 if val > 1 else val, 6)

    _save_config(current)

    sks        = _load_sks()
    sk_updated = False
    for key in ("prov_0_sk", "prov_1_sk", "prov_2_sk"):
        if key in data and data[key]:
            sks[key] = str(data[key]).strip()
            sk_updated = True
    if sk_updated:
        _save_sks(sks)

    safe      = {k: v for k, v in current.items() if not k.endswith("_sk")}
    sks_now   = _load_sks()
    for i in range(3):
        safe[f"prov_{i}_sk_set"] = bool(sks_now.get(f"prov_{i}_sk"))
    return jsonify({"ok": True, "config": safe})


@bp.route("/api/config/reset", methods=["POST"])
def reset_config():
    _save_config(dict(_CONFIG_DEFAULTS))
    return jsonify({"ok": True, "config": _cfg})


# ── Nodes sync / heights ──────────────────────────────────────────────────────

@bp.route("/api/nodes/sync", methods=["GET"])
def nodes_sync():
    import urllib.request as _ur
    from ..rues import _local_node_heights, _local_node_heights_lock
    from ..assess import _read_node_height_log  # see note below

    network_tip = None
    from ..config import GRAPHQL_URL
    try:
        q   = '{ block(height: -1) { header { height } } }'
        req = _ur.Request(GRAPHQL_URL, data=q.encode(),
                          headers={"rusk-version": RUSK_VERSION,
                                   "Content-Type": "application/graphql"},
                          method="POST")
        with _ur.urlopen(req, timeout=6) as r:
            p   = json.loads(r.read())
        b = p.get("block") or p.get("data", {}).get("block", {})
        network_tip = int(b["header"]["height"])
    except Exception:
        pass

    with _local_node_heights_lock:
        cached = dict(_local_node_heights)

    results = {}
    for idx in NODE_INDICES:
        height = cached.get(idx)
        if height is not None:
            results[str(idx)] = {"height": height, "ok": True, "error": None, "source": "ws"}
        else:
            log_path = cfg(f"node_{idx}_log") or ""
            r = _read_node_height_from_log(log_path)
            r["source"] = "log"
            results[str(idx)] = r

    return jsonify({"ok": True, "network_tip": network_tip, "nodes": results})


@bp.route("/api/nodes/heights", methods=["GET"])
def nodes_heights():
    """Lightweight: local node heights only — pure memory read."""
    from ..rues import _local_node_heights, _local_node_heights_lock
    with _local_node_heights_lock:
        cached = dict(_local_node_heights)
    return jsonify({str(k): v for k, v in cached.items()})


def _read_node_height_from_log(log_path: str, tail_lines: int = 500) -> dict:
    """Grep block height from rusk log file (fallback when WS not connected)."""
    if not log_path or not os.path.exists(log_path):
        return {"height": None, "ok": False, "error": "log not found"}
    try:
        r = run_cmd(f"tail -n {tail_lines} {log_path} | grep -oP 'height.*?\\K[0-9]+' | tail -1",
                    timeout=5)
        raw = r.get("stdout", "").strip()
        return {"height": int(raw), "ok": True, "error": None} if raw else \
               {"height": None, "ok": False, "error": "no height in log"}
    except Exception as e:
        return {"height": None, "ok": False, "error": str(e)}


# ── Rotation status ───────────────────────────────────────────────────────────

@bp.route("/api/rotation/status", methods=["GET", "POST"])
def rotation_status():
    from ..rotation import _rotation_state
    from ..pool import _pool_balance_cache, _pool_balance_cache_ts, _pool_balance_lock
    from ..rues import _local_node_heights, _local_node_heights_lock

    st = {k: (list(v) if hasattr(v, "__iter__") and not isinstance(v, str) else v)
          for k, v in _rotation_state.items()}
    with _pool_balance_lock:
        st["pool_balance_cache"] = round(_pool_balance_cache, 4)
        st["pool_cache_age_s"]   = round(time.time() - _pool_balance_cache_ts, 1) \
                                   if _pool_balance_cache_ts else None
    pt = _rotation_state.get("pending_terminate")
    st["pending_terminate"] = ({"addr": pt["addr"], "idx": pt["idx"],
                                "attempts": pt.get("attempts", 1)} if pt else None)
    with _local_node_heights_lock:
        st["local_node_heights"] = dict(_local_node_heights)
    return jsonify(st)


@bp.route("/api/rotation/enable", methods=["POST"])
def rotation_enable():
    from ..rotation import _rotation_state
    pw = (request.get_json() or {}).get("password", "") or get_password()
    if pw:
        _cache_wallet_pw(pw)
    _rotation_state["enabled"] = True
    from ..config import _save_rotation_enabled
    _save_rotation_enabled(True)
    from ..rotation import _rlog
    _rlog("auto-rotation enabled", "ok")
    return jsonify({"ok": True, "enabled": True, "errors": [], "warnings": [
        "Rotation engine is being rebuilt — enable saved but no automation active yet"
    ]})


@bp.route("/api/rotation/disable", methods=["POST"])
def rotation_disable():
    from ..rotation import _rotation_state, _rset, _rlog
    _rotation_state["enabled"] = False
    _rset("idle")
    from ..config import _save_rotation_enabled
    _save_rotation_enabled(False)
    _rlog("rotation DISABLED")
    return jsonify({"ok": True, "enabled": False})


@bp.route("/api/rotation/cmd_log", methods=["GET"])
def rotation_cmd_log():
    with _cmd_log_lock:
        entries = list(_cmd_log)
    after_idx   = int(request.args.get("after", -1))
    new_entries = entries[after_idx + 1:] if after_idx >= 0 else entries
    return jsonify({"entries": new_entries, "total": len(entries)})


# ── RUES debug + control ──────────────────────────────────────────────────────

@bp.route("/api/debug/rues", methods=["GET", "POST"])
def debug_rues():
    from ..rues import _rues_debug_log, _rues_debug_lock, _RUES_RUNNING, _RUES_SESSION_ID, _RUES_SUB_RESULTS
    with _rues_debug_lock:
        log = list(reversed(_rues_debug_log[-500:]))
    return jsonify({
        "running":       _RUES_RUNNING,
        "session_id":    _RUES_SESSION_ID,
        "subscriptions": _RUES_SUB_RESULTS,
        "log":           log,
    })


@bp.route("/api/rues/connect", methods=["POST"])
def rues_connect():
    import rues as _rues
    data = request.get_json() or {}
    if "node_idx" in data:
        idx  = int(data["node_idx"])
        port = int(cfg(f"node_{idx}_ws_port") or [8080, 8282, 8383][idx])
        url  = f"ws://localhost:{port}/on"
    elif "url" in data:
        url = data["url"].strip()
    else:
        return jsonify({"ok": False, "error": "provide url or node_idx"}), 400
    _rues._RUES_WS_URL = url
    _rues._RUES_RUNNING = False
    time.sleep(0.3)
    _rues._start_rues()
    _log(f"[rues] reconnecting to {url}")
    return jsonify({"ok": True, "url": url})


@bp.route("/api/rues/subscribe", methods=["POST"])
def rues_subscribe_topic():
    import urllib.request as _ur, urllib.error
    import rues as _rues
    data   = request.get_json() or {}
    topic  = data.get("topic", "")
    action = data.get("action", "subscribe")
    if not topic or topic not in _rues._RUES_TOPICS:
        return jsonify({"ok": False, "error": f"unknown topic: {topic}"}), 400
    if not _rues._RUES_SESSION_ID:
        return jsonify({"ok": False, "error": "no active RUES session"}), 503
    url    = _rues._rues_topic_url(topic)
    method = "GET" if action == "subscribe" else "DELETE"
    try:
        req = _ur.Request(url, method=method,
                          headers={"Rusk-Session-Id": _rues._RUES_SESSION_ID,
                                   "rusk-version": RUSK_VERSION})
        with _ur.urlopen(req, timeout=8):
            pass
        _rues._RUES_SUB_RESULTS[topic] = "ok" if action == "subscribe" else "unsubscribed"
        return jsonify({"ok": True, "topic": topic, "action": action})
    except urllib.error.HTTPError as e:
        if e.code == 424 and action == "subscribe":
            _rues._RUES_SUB_RESULTS[topic] = "ok (auto)"
            return jsonify({"ok": True, "topic": topic, "action": action,
                            "note": "424 — node auto-subscribed"})
        _rues._RUES_SUB_RESULTS[topic] = f"FAILED: HTTP {e.code}"
        return jsonify({"ok": False, "error": str(e)}), 500
    except Exception as e:
        _rues._RUES_SUB_RESULTS[topic] = f"FAILED: {e}"
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/debug/own_keys", methods=["GET", "POST"])
def debug_own_keys():
    from ..assess import _own_provisioner_keys
    return jsonify({"keys": list(_own_provisioner_keys)})


# ── fn_args decoder ───────────────────────────────────────────────────────────

_SOZU_ADDR_FNS = frozenset([
    "liquidate", "terminate", "stake_deactivate", "remove_provisioner",
])
_SOZU_AMOUNT_FNS = frozenset([
    "sozu_stake", "sozu_unstake", "sozu_airdrop",
])
_SOZU_STAKE_ACTIVATE_FNS = frozenset(["stake_activate"])
_SOZU_DECODE_FNS = _SOZU_ADDR_FNS | _SOZU_AMOUNT_FNS | _SOZU_STAKE_ACTIVATE_FNS


def _decode_fn_args(fn_name: str, fn_args_b64: str) -> dict:
    if fn_name not in _SOZU_DECODE_FNS or not fn_args_b64:
        return {}
    import subprocess as _sp, base64 as _b64
    from ..rues import _rues_node_host
    try:
        raw_bytes = _b64.b64decode(fn_args_b64)
        hex_val   = "0x" + raw_bytes.hex()
        node_host = _rues_node_host()
        url = f"{node_host}/on/driver:{CONTRACT_ID}/decode_input_fn:{fn_name}"
        r = _sp.run(
            ["curl", "-s", "-X", "POST", url,
             "-H", f"rusk-version: {RUSK_VERSION}",
             "-d", hex_val],
            capture_output=True, text=True, timeout=8)
        result = r.stdout.strip()
        if not result:
            return {}
        if fn_name in _SOZU_STAKE_ACTIVATE_FNS:
            try:
                obj       = json.loads(result)
                bls_key   = (obj.get("keys") or {}).get("account", "")
                value_str = str(obj.get("value", "0"))
                lux       = int(value_str) if value_str.isdigit() else 0
                out = {}
                if bls_key: out["provisioner"]  = bls_key
                if lux:
                    out["amount_lux"]  = lux
                    out["amount_dusk"] = round(lux / 1_000_000_000, 9)
                return out
            except Exception:
                return {}
        if fn_name in _SOZU_AMOUNT_FNS:
            lux = int(result.strip('"'))
            return {"amount_lux": lux, "amount_dusk": round(lux / 1_000_000_000, 9)}
        clean = result.strip('"')
        return {"provisioner": clean} if len(clean) > 20 else {}
    except Exception as e:
        _log(f"[decode_fn_args] failed fn={fn_name}: {e}")
        return {}


@bp.route("/api/decode_fn_args", methods=["POST"])
def api_decode_fn_args():
    data    = request.get_json() or {}
    fn_name = data.get("fn_name", "").strip()
    fn_args = data.get("fn_args", "").strip()
    if not fn_name or not fn_args:
        return jsonify({"ok": False, "error": "fn_name and fn_args required"}), 400
    result = _decode_fn_args(fn_name, fn_args)
    if not result:
        return jsonify({"ok": False,
                        "error": f"decode_input_fn:{fn_name} returned empty"})
    return jsonify({"ok": True, "fn_name": fn_name, "result": result})
