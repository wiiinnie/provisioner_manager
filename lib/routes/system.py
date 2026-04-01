"""
routes/system.py — System, config, nodes, version endpoints.
No rotation. No RUES.
"""
import json
import os
import time
from datetime import datetime
from flask import Blueprint, jsonify, request, send_file

from ..config import (
    _log, NETWORK, CONTRACT_ID, _NODE_STATE_URL, RUSK_VERSION,
    WALLET_BIN, WALLET_PATH, NODE_INDICES, PORT,
    cfg, _cfg, _CONFIG_DEFAULTS, _load_config, _save_config,
    _load_sks, _save_sks,
)
from ..wallet import run_cmd, wallet_cmd, get_password, _cache_wallet_pw, OPERATOR_WALLET

bp = Blueprint("system", __name__)

SERVER_VERSION = "1.0.6"


@bp.route("/api/ping")
def ping():
    return jsonify({"ok": True, "ts": datetime.now().isoformat()})


@bp.route("/")
def index():
    here = os.path.dirname(os.path.abspath(__file__))
    root = os.path.dirname(os.path.dirname(here))
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


@bp.route("/api/config", methods=["GET"])
def get_config():
    cfg("network_id")
    safe = {k: v for k, v in _cfg.items() if not k.endswith("_sk")}
    sks  = _load_sks()
    for i in range(3):
        safe[f"prov_{i}_sk_set"] = bool(sks.get(f"prov_{i}_sk"))
    return jsonify(safe)


@bp.route("/api/config", methods=["POST"])
def set_config():
    data    = request.get_json() or {}
    current = dict(_cfg) if _cfg else dict(_CONFIG_DEFAULTS)
    int_keys   = ("network_id","rotation_window","snatch_window","backfill_blocks",
                  "master_idx","gas_limit","node_0_ws_port","node_1_ws_port","node_2_ws_port")
    float_keys = ("min_deposit_dusk","snatch_min_deposit_dusk")
    str_keys   = ("contract_address","operator_address",
                  "prov_0_address","prov_1_address","prov_2_address",
                  "node_0_log","node_1_log","node_2_log")
    for k in int_keys:
        if k in data:
            current[k] = max(100000,int(data[k])) if k=="gas_limit" else int(data[k])
    for k in float_keys:
        if k in data: current[k] = float(data[k])
    for k in str_keys:
        if k in data: current[k] = str(data[k]).strip()
    if "master_stake_pct" in data:
        val = float(data["master_stake_pct"])
        current["master_stake_pct"] = round(val/100 if val>1 else val, 6)
    _save_config(current)
    sks = _load_sks(); sk_updated = False
    for key in ("prov_0_sk","prov_1_sk","prov_2_sk"):
        if key in data and data[key]: sks[key]=str(data[key]).strip(); sk_updated=True
    if sk_updated: _save_sks(sks)
    safe = {k:v for k,v in current.items() if not k.endswith("_sk")}
    sks_now = _load_sks()
    for i in range(3): safe[f"prov_{i}_sk_set"] = bool(sks_now.get(f"prov_{i}_sk"))
    return jsonify({"ok":True,"config":safe})


@bp.route("/api/config/reset", methods=["POST"])
def reset_config():
    _save_config(dict(_CONFIG_DEFAULTS))
    return jsonify({"ok":True,"config":_cfg})


@bp.route("/api/nodes/heights", methods=["GET"])
def nodes_heights():
    from ..nodes import get_heights
    return jsonify(get_heights())


@bp.route("/api/nodes/sync", methods=["GET"])
def nodes_sync():
    import urllib.request as _ur
    from ..nodes import get_heights
    from ..config import GRAPHQL_URL
    network_tip = None
    try:
        q   = "{ block(height: -1) { header { height } } }"
        req = _ur.Request(GRAPHQL_URL, data=q.encode(),
                          headers={"rusk-version": RUSK_VERSION,
                                   "Content-Type": "application/graphql"},
                          method="POST")
        with _ur.urlopen(req, timeout=6) as r:
            p = json.loads(r.read())
        b = p.get("block") or p.get("data",{}).get("block",{})
        network_tip = int(b["header"]["height"])
    except Exception:
        pass
    cached = get_heights()
    results = {}
    for idx in NODE_INDICES:
        h = cached.get(idx)
        results[str(idx)] = {"height":h,"ok":h is not None,"source":"ws" if h else "none"}
    return jsonify({"ok":True,"network_tip":network_tip,"nodes":results})


_SOZU_ADDR_FNS           = frozenset(["liquidate","terminate","stake_deactivate","remove_provisioner"])
_SOZU_AMOUNT_FNS         = frozenset(["sozu_stake","sozu_unstake","sozu_airdrop"])
_SOZU_STAKE_ACTIVATE_FNS = frozenset(["stake_activate"])
_SOZU_DECODE_FNS         = _SOZU_ADDR_FNS | _SOZU_AMOUNT_FNS | _SOZU_STAKE_ACTIVATE_FNS


def _decode_fn_args(fn_name:str, fn_args_b64:str) -> dict:
    if fn_name not in _SOZU_DECODE_FNS or not fn_args_b64: return {}
    import subprocess as _sp, base64 as _b64
    try:
        raw_bytes = _b64.b64decode(fn_args_b64)
        hex_val   = "0x" + raw_bytes.hex()
        url = f"{_NODE_STATE_URL}/on/driver:{CONTRACT_ID}/decode_input_fn:{fn_name}"
        r = _sp.run(["curl","-s","-X","POST",url,"-H",f"rusk-version: {RUSK_VERSION}","-d",hex_val],
                    capture_output=True,text=True,timeout=8)
        result = r.stdout.strip()
        if not result: return {}
        if fn_name in _SOZU_STAKE_ACTIVATE_FNS:
            obj=json.loads(result); bls=(obj.get("keys") or {}).get("account","")
            lux=int(str(obj.get("value","0"))); out={}
            if bls: out["provisioner"]=bls
            if lux: out["amount_lux"]=lux; out["amount_dusk"]=round(lux/1e9,9)
            return out
        if fn_name in _SOZU_AMOUNT_FNS:
            lux=int(result.strip('"'))
            return {"amount_lux":lux,"amount_dusk":round(lux/1e9,9)}
        clean=result.strip('"')
        return {"provisioner":clean} if len(clean)>20 else {}
    except Exception as e:
        _log(f"[decode_fn_args] {fn_name}: {e}"); return {}


@bp.route("/api/decode_fn_args", methods=["POST"])
def api_decode_fn_args():
    data=request.get_json() or {}
    fn_name=data.get("fn_name","").strip(); fn_args=data.get("fn_args","").strip()
    if not fn_name or not fn_args:
        return jsonify({"ok":False,"error":"fn_name and fn_args required"}),400
    result=_decode_fn_args(fn_name,fn_args)
    if not result:
        return jsonify({"ok":False,"error":f"decode_input_fn:{fn_name} returned empty"})
    return jsonify({"ok":True,"fn_name":fn_name,"result":result})


@bp.route("/api/debug/own_keys", methods=["GET","POST"])
def debug_own_keys():
    from ..assess import _own_provisioner_keys
    return jsonify({"keys":list(_own_provisioner_keys)})


# ── RUES event stream ─────────────────────────────────────────────────────────

@bp.route("/api/rues/status", methods=["GET"])
def rues_status():
    from ..rues import get_status
    return jsonify(get_status())


@bp.route("/api/rues/subscribe", methods=["POST"])
def rues_subscribe():
    from ..rues import subscribe_topic
    data   = request.get_json() or {}
    topic  = data.get("topic", "").strip()
    action = data.get("action", "subscribe")
    if action not in ("subscribe", "unsubscribe"):
        return jsonify({"ok": False, "error": "action must be subscribe or unsubscribe"}), 400
    return jsonify(subscribe_topic(topic, action))


# ── Deposit event log ─────────────────────────────────────────────────────────

@bp.route("/api/events/deposit_log", methods=["GET"])
def deposit_log():
    from ..events import get_deposit_log
    return jsonify(get_deposit_log())


@bp.route("/api/events/deposit_log/clear", methods=["POST"])
def deposit_log_clear():
    from ..events import _deposit_log, _deposit_log_lock
    with _deposit_log_lock:
        _deposit_log.clear()
    return jsonify({"ok": True})
