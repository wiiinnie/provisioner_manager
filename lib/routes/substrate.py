"""
routes/substrate.py — Substrate query endpoints.

Buttons: my capacity · registered operators · combined operator stake · stake & operator fees
"""
import json
from flask import Blueprint, jsonify

from ..config import _NET, OPERATOR_ADDRESS
from ..wallet import operator_cmd, wallet_cmd, get_password

bp = Blueprint("substrate", __name__)


@bp.route("/api/substrate/capacity", methods=["GET", "POST"])
def substrate_capacity():
    """Operator capacity from substrate (active/locked stake bounds)."""
    pw = get_password()
    op = OPERATOR_ADDRESS()
    if not op:
        return jsonify({"ok": False, "stderr": "operator_address not configured"}), 400

    r = operator_cmd(f"{_NET} substrate capacity --operator {op} --format json",
                     timeout=30, password=pw)
    stdout = r.get("stdout", "") or ""
    stderr = r.get("stderr", "") or ""
    capacity = None
    raw = stdout.strip() or stderr.strip()
    if raw.startswith("{"):
        try:
            capacity = json.loads(raw)
        except Exception:
            pass
    return jsonify({"ok": r["ok"], "stdout": stdout, "stderr": stderr,
                    "capacity": capacity, "duration_ms": r.get("duration_ms", 0)})


@bp.route("/api/substrate/operators", methods=["GET", "POST"])
def substrate_operators():
    """List all registered operators from substrate."""
    r = operator_cmd(f"{_NET} substrate operators --format json",
                     timeout=30, password=get_password())
    stdout = r.get("stdout", "") or ""
    stderr = r.get("stderr", "") or ""
    parsed = None
    raw = stdout.strip() or stderr.strip()
    if raw.startswith("{") or raw.startswith("["):
        try:
            parsed = json.loads(raw)
        except Exception:
            pass
    return jsonify({"ok": r["ok"], "stdout": stdout, "stderr": stderr,
                    "parsed": parsed, "duration_ms": r.get("duration_ms", 0)})


@bp.route("/api/substrate/fee", methods=["GET", "POST"])
def substrate_fee():
    """Query operator fee from substrate."""
    op = OPERATOR_ADDRESS()
    if not op:
        return jsonify({"ok": False, "stderr": "operator_address not configured"}), 400
    r = operator_cmd(f"{_NET} substrate fee --operator {op}",
                     timeout=30, password=get_password())
    return jsonify({"ok": r["ok"],
                    "stdout": r.get("stdout", "") or "",
                    "stderr": r.get("stderr", "") or "",
                    "duration_ms": r.get("duration_ms", 0)})


@bp.route("/api/substrate/active_stake", methods=["GET", "POST"])
def substrate_active_stake():
    """Query combined operator active stake."""
    r = wallet_cmd(f"{_NET} substrate active-stake --format json",
                   timeout=30, password=get_password())
    stdout = r.get("stdout", "") or ""
    stderr = r.get("stderr", "") or ""
    parsed = None
    raw = stdout.strip() or stderr.strip()
    if raw.startswith("{") or raw.startswith("["):
        try:
            parsed = json.loads(raw)
        except Exception:
            pass
    return jsonify({"ok": r["ok"], "stdout": stdout, "stderr": stderr,
                    "parsed": parsed, "duration_ms": r.get("duration_ms", 0)})
