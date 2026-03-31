"""
wallet.py — Shell command execution, wallet/operator cmd wrappers, password cache.

Imports: config only.
"""
import os
import re
import signal
import subprocess
import threading
import time
from datetime import datetime

from .config import (
    _log, WALLET_BIN, WALLET_PATH, OPERATOR_WALLET,
    _NET, RUSK_VERSION, GAS_LIMIT,
)

# ── Locks ─────────────────────────────────────────────────────────────────────
# _wallet_lock:   general wallet commands (balance, profiles, stake-info)
# _rotation_lock: time-critical pool commands (stake-activate, liquidate, deactivate)
#                 kept separate so dashboard reads never block fast-alloc
_wallet_lock   = threading.Lock()
_rotation_lock = threading.Lock()

# ── Password cache ─────────────────────────────────────────────────────────────
_cached_wallet_pw: str = ""

def get_password() -> str:
    """
    Return the cached wallet password.
    Inside a Flask request context also reads from JSON body / header and caches.
    """
    global _cached_wallet_pw
    try:
        from flask import has_request_context, request
        if has_request_context():
            data = request.get_json(silent=True) or {}
            pw = data.get("password", "") or request.headers.get("X-Wallet-Password", "")
            if pw:
                _cached_wallet_pw = pw
            return _cached_wallet_pw
    except Exception:
        pass
    return _cached_wallet_pw

def _cache_wallet_pw(pw: str) -> None:
    global _cached_wallet_pw
    if pw:
        _cached_wallet_pw = pw

# ── ANSI stripping ─────────────────────────────────────────────────────────────
def _strip_ansi(s: str) -> str:
    return re.sub("\x1b[^a-zA-Z]*[a-zA-Z]|\r", "", s)

# ── run_cmd ───────────────────────────────────────────────────────────────────
def run_cmd(cmd: str, timeout: int = 30) -> dict:
    """Run a shell command, return {ok, stdout, stderr, returncode, duration_ms}.
    Uses start_new_session=True so on timeout the whole process group is killed."""
    t0 = time.time()
    proc = subprocess.Popen(
        cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True, start_new_session=True,
    )
    try:
        stdout, stderr = proc.communicate(timeout=timeout)
        return {
            "ok":          proc.returncode == 0,
            "stdout":      _strip_ansi(stdout).strip(),
            "stderr":      _strip_ansi(stderr).strip(),
            "returncode":  proc.returncode,
            "duration_ms": int((time.time() - t0) * 1000),
            "cmd":         cmd,
            "ts":          datetime.now().isoformat(),
        }
    except subprocess.TimeoutExpired:
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        except Exception:
            proc.kill()
        proc.communicate()
        return {"ok": False, "stdout": "", "returncode": -1,
                "stderr": f"timeout after {timeout}s",
                "duration_ms": timeout * 1000,
                "cmd": cmd, "ts": datetime.now().isoformat()}
    except Exception as exc:
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        except Exception:
            proc.kill()
        return {"ok": False, "stdout": "", "returncode": -1,
                "stderr": str(exc), "duration_ms": 0,
                "cmd": cmd, "ts": datetime.now().isoformat()}

# ── wallet_cmd ────────────────────────────────────────────────────────────────
def wallet_cmd(subcmd: str, timeout: int = 30, password: str = "") -> dict:
    """Run a command using the PROVISIONER wallet (~/sozu_provisioner)."""
    if password:
        safe_pw = password.replace("'", "'\\''")
        inner   = f"{WALLET_BIN} -w {WALLET_PATH} --password '{safe_pw}' {subcmd}"
        log_cmd = f"{WALLET_BIN} -w {WALLET_PATH} --password '***' {subcmd}"
    else:
        inner   = f"{WALLET_BIN} -w {WALLET_PATH} {subcmd}"
        log_cmd = inner
    with _wallet_lock:
        result = run_cmd(inner, timeout=timeout)
    result["cmd"] = log_cmd
    return result

# ── operator_cmd ──────────────────────────────────────────────────────────────
_own_tx_hashes: set       = set()
_own_tx_hashes_lock       = threading.Lock()
_cmd_log: list            = []
_cmd_log_lock             = threading.Lock()

def _extract_tx_hash(output: str) -> str:
    m = re.search(r"Hash:\s*([0-9a-fA-F]{32,})", output)
    return m.group(1).strip() if m else ""

def _push_cmd_output(name: str, result: dict) -> None:
    """Store background command result for the dashboard Command Output panel."""
    from datetime import datetime as _dt
    entry = {
        "name":        name,
        "ok":          result.get("ok", False),
        "stdout":      result.get("stdout", ""),
        "stderr":      result.get("stderr", ""),
        "duration_ms": result.get("duration_ms", 0),
        "ts":          _dt.now().strftime("%H:%M:%S"),
    }
    with _cmd_log_lock:
        _cmd_log.append(entry)
        if len(_cmd_log) > 200:
            del _cmd_log[:-200]

def operator_cmd(subcmd: str, timeout: int = 30, password: str = "",
                 gas_limit: int = 0) -> dict:
    """Run a command using the OPERATOR wallet (~/sozu_operator).

    Lock selection:
      pool / stake-info / substrate → _rotation_lock  (time-critical)
      everything else               → _wallet_lock
    """
    gl_flag = f" --gas-limit {gas_limit}" if gas_limit > 0 else ""
    if password:
        safe_pw = password.replace("'", "'\\''")
        cmd     = f"{WALLET_BIN} --password '{safe_pw}'{gl_flag} -w {OPERATOR_WALLET} {subcmd}"
        log_cmd = f"{WALLET_BIN} --password '***'{gl_flag} -w {OPERATOR_WALLET} {subcmd}"
    else:
        cmd     = f"{WALLET_BIN}{gl_flag} -w {OPERATOR_WALLET} {subcmd}"
        log_cmd = cmd

    _stripped = subcmd.strip()
    if _stripped.startswith("-n "):
        parts = _stripped.split(None, 2)
        _stripped = parts[2] if len(parts) > 2 else _stripped
    _use_rot_lock = any(_stripped.startswith(p) for p in ("pool ", "stake-info", "substrate "))
    _lock = _rotation_lock if _use_rot_lock else _wallet_lock

    with _lock:
        result = run_cmd(cmd, timeout=timeout)
    result["cmd"] = log_cmd

    _tx_hash = _extract_tx_hash(result.get("stdout", "") + result.get("stderr", ""))
    if _tx_hash:
        with _own_tx_hashes_lock:
            _own_tx_hashes.add(_tx_hash.lower())
            if len(_own_tx_hashes) > 500:
                _own_tx_hashes.discard(next(iter(_own_tx_hashes)))

    import threading as _thr
    if _thr.current_thread().name == "rotation":
        _push_cmd_output(subcmd.split()[0], result)
    return result

def extract_payload(output: str) -> str:
    """Extract bare hex payload from wallet calculate-payload output."""
    candidates = re.findall(r"[0-9a-fA-F]{40,}", output)
    if candidates:
        return max(candidates, key=len)
    for line in reversed(output.strip().splitlines()):
        line = line.strip()
        if not line:
            continue
        if ":" in line:
            line = line.split(":", 1)[1].strip()
        line = line.strip('"').strip()
        if re.match(r"^[0-9a-fA-F]{40,}$", line):
            return line
    return ""
