"""
config.py — Network constants, sozu-wallet config reader, dashboard config, SK storage.

All other modules import from here.  No imports from sibling modules.
"""
import json
import os
import re
import threading
import logging
from datetime import datetime

# ── Logging ───────────────────────────────────────────────────────────────────
_log_lock = threading.Lock()

def _log(msg: str) -> None:
    ts = datetime.now().strftime("%H:%M:%S")
    with _log_lock:
        print(f"{ts}  {msg}", flush=True)

class _OptionsFilter(logging.Filter):
    def filter(self, record):
        return "OPTIONS" not in record.getMessage()

class _CompactFormatter(logging.Formatter):
    _DATE_PAT = re.compile(r' - - \[[\d/A-Za-z: ]+\]')
    def format(self, record):
        msg = super().format(record)
        return self._DATE_PAT.sub("", msg)

def configure_werkzeug_logger():
    wz = logging.getLogger("werkzeug")
    wz.handlers.clear()
    h = logging.StreamHandler()
    h.setFormatter(_CompactFormatter(fmt="%(asctime)s  %(message)s", datefmt="%H:%M:%S"))
    h.addFilter(_OptionsFilter())
    wz.addHandler(h)
    wz.propagate = False

# ── Network / wallet constants ─────────────────────────────────────────────────
WALLET_BIN      = "sozu-wallet"
WALLET_PATH     = os.path.expanduser("~/sozu_provisioner")
OPERATOR_WALLET = os.path.expanduser("~/sozu_operator")
NETWORK         = "testnet"   # change to "mainnet" to go live
_NET            = f"-n {NETWORK}"
RUSK_VERSION    = "1.5"
NODE_INDICES    = [0, 1, 2]
PORT            = 7373

# ── sozu-wallet config.toml reader ────────────────────────────────────────────
_SOZU_WALLET_CONFIG_TOML = os.path.expanduser("~/.config/sozu-wallet/config.toml")

_NETWORK_DEFAULTS = {
    "testnet": {
        "state":     "https://testnet.nodes.dusk.network",
        "pool":      "72883945ac1aa032a88543aacc9e358d1dfef07717094c05296ce675f23078f2",
        "hub":       "bae85f8c24730a5a19fbe3d3bd58248ac8c302b62fe414a8c640d8c0ed286b9e",
        "relayer":   "51ced4fad52fc590def2736969c9e3e30013275a996c53714b81d8a08774aa37",
        "substrate": "0077ecbf88aa20d6d0a6afa20bd26300a2b562fdbac368bf1e3c1325e8555941",
    },
    "mainnet": {
        "state":     "https://nodes.dusk.network",
        "pool":      "6fdfdc713a18fc6ca2ad20eb2b4a3305a935ef47d6a872d9a4df8bc9fd9d169e",
        "hub":       "b32c917e76abc6fcf2edbee0fa70231d8e19c405b18421794a11badfc66d2f26",
        "relayer":   "1cc415d05b1cfbf2583bf2e8a0e39b2c768d263ef92d6a21a4787f76c6afa924",
        "substrate": "bc6f50f7404d098cdd1117b15dddab6f6f3dad01c5ce3c5ce9b68a8b60bc4c1d",
    },
    "devnet": {
        "state":     "https://devnet.nodes.dusk.network",
        "pool":      "", "hub":  "", "relayer":  "", "substrate": "",
    },
    "local": {
        "state":     "http://127.0.0.1:8080",
        "pool":      "", "hub":  "", "relayer":  "", "substrate": "",
    },
}

def _read_sozu_wallet_config(network: str = NETWORK) -> dict:
    defaults = dict(_NETWORK_DEFAULTS.get(network, _NETWORK_DEFAULTS["testnet"]))
    try:
        with open(_SOZU_WALLET_CONFIG_TOML) as f:
            content = f.read()
        section_re = re.compile(r'^\[network\.([^\]]+)\]', re.MULTILINE)
        kv_re      = re.compile(r'^(\w+)\s*=\s*"([^"]*)"', re.MULTILINE)
        sections = {}
        for m in section_re.finditer(content):
            sec_name  = m.group(1)
            sec_start = m.end()
            next_sec  = section_re.search(content, sec_start)
            sec_end   = next_sec.start() if next_sec else len(content)
            body      = content[sec_start:sec_end]
            sections[sec_name] = {k: v for k, v in kv_re.findall(body)}
        net_section       = sections.get(network, {})
        contracts_section = sections.get(f"{network}.contracts", {})
        if net_section.get("state"):
            defaults["state"] = net_section["state"]
        for key in ("pool", "hub", "relayer", "substrate"):
            if contracts_section.get(key):
                defaults[key] = contracts_section[key]
        _log(f"[config] loaded sozu-wallet config for network={network}: "
             f"state={defaults['state']} pool={defaults['pool'][:12]}…")
    except FileNotFoundError:
        _log(f"[config] {_SOZU_WALLET_CONFIG_TOML} not found — using built-in defaults")
    except Exception as e:
        _log(f"[config] failed to parse sozu-wallet config: {e} — using built-in defaults")
    return defaults

_SOZU_NET_CFG   = _read_sozu_wallet_config(NETWORK)
_NODE_STATE_URL = _SOZU_NET_CFG["state"]
GRAPHQL_URL     = f"{_NODE_STATE_URL}/on/graphql/query"
CONTRACT_ID     = _SOZU_NET_CFG["pool"]

# ── Dashboard config ───────────────────────────────────────────────────────────
_CONFIG_PATH       = os.path.expanduser("~/.sozu_dashboard_config.json")
_SK_PATH           = os.path.expanduser("~/.sozu_keys")
_ROTATION_LOG_PATH = os.path.expanduser("~/.sozu_rotation.log")
_ROTATION_STATE_PATH = os.path.expanduser("~/.sozu_rotation_enabled")

_CONFIG_DEFAULTS = {
    "network_id":              2,
    "contract_address":        CONTRACT_ID,
    "operator_address":        "",
    "prov_0_address":          "",
    "prov_1_address":          "",
    "prov_2_address":          "",
    "node_0_log":              "/var/log/rusk-1.log",
    "node_1_log":              "/var/log/rusk-2.log",
    "node_2_log":              "/var/log/rusk-3.log",
    "node_0_ws_port":          8080,
    "node_1_ws_port":          8282,
    "node_2_ws_port":          8383,
    "rotation_window":         41,
    "snatch_window":           11,
    "backfill_blocks":         200,
    "master_idx":              -1,
    "master_stake_pct":        0.75,
    "min_deposit_dusk":        100.0,
    "snatch_min_deposit_dusk": 100.0,
    "gas_limit":               2000000,
}

_cfg: dict       = {}
_cfg_mtime: float = 0.0

def _load_config() -> dict:
    global _cfg, _cfg_mtime
    stored = {}
    if os.path.exists(_CONFIG_PATH):
        try:
            with open(_CONFIG_PATH) as f:
                stored = json.load(f)
            _cfg_mtime = os.path.getmtime(_CONFIG_PATH)
        except Exception:
            stored = {}
    merged = {**_CONFIG_DEFAULTS, **stored}
    # Fixup stale defaults from old versions
    if merged.get("backfill_blocks", 200) == 50:   merged["backfill_blocks"] = 200
    if merged.get("rotation_window") == 100:        merged["rotation_window"] = 41
    if merged.get("snatch_window")   == 50:         merged["snatch_window"]   = 11
    _cfg = merged
    return _cfg

def _save_config(data: dict) -> None:
    global _cfg, _cfg_mtime
    _cfg = {**_CONFIG_DEFAULTS, **data}
    with open(_CONFIG_PATH, "w") as f:
        json.dump(_cfg, f, indent=2)
    _cfg_mtime = os.path.getmtime(_CONFIG_PATH)

def cfg(key: str):
    global _cfg_mtime
    if not _cfg:
        _load_config()
        return _cfg.get(key, _CONFIG_DEFAULTS.get(key))
    try:
        mtime = os.path.getmtime(_CONFIG_PATH) if os.path.exists(_CONFIG_PATH) else 0.0
        if mtime > _cfg_mtime:
            _load_config()
    except Exception:
        pass
    return _cfg.get(key, _CONFIG_DEFAULTS.get(key))

_load_config()

def CONTRACT_ADDRESS(): return cfg("contract_address") or CONTRACT_ID
def OPERATOR_ADDRESS(): return cfg("operator_address")
def NETWORK_ID():       return cfg("network_id") or 2
def GAS_LIMIT():        return int(cfg("gas_limit") or 2000000)

# ── SK storage ─────────────────────────────────────────────────────────────────
def _load_sks() -> dict:
    if os.path.exists(_SK_PATH):
        try:
            with open(_SK_PATH) as f:
                return json.load(f)
        except Exception:
            pass
    return {}

def _save_sks(sks: dict) -> None:
    with open(_SK_PATH, "w") as f:
        json.dump(sks, f, indent=2)
    os.chmod(_SK_PATH, 0o600)

def _get_sk(idx: int) -> str:
    return _load_sks().get(f"prov_{idx}_sk", "")

# ── Rotation phase persistence ─────────────────────────────────────────────────
_ROTATION_PHASE_PATH = os.path.expanduser("~/.sozu_rotation_phase")

def _load_rotation_phase() -> str:
    try:
        with open(_ROTATION_PHASE_PATH) as f:
            return json.load(f) or "bootstrap_waiting"
    except Exception:
        return "bootstrap_waiting"

def _save_rotation_phase(phase: str) -> None:
    try:
        with open(_ROTATION_PHASE_PATH, "w") as f:
            json.dump(phase, f)
    except Exception as e:
        _log(f"WARNING: could not persist rotation phase: {e}")

def _load_rotation_enabled() -> bool:
    try:
        with open(_ROTATION_STATE_PATH) as f:
            return json.load(f) is True
    except Exception:
        return False

def _save_rotation_enabled(enabled: bool) -> None:
    try:
        with open(_ROTATION_STATE_PATH, "w") as f:
            json.dump(enabled, f)
    except Exception as e:
        _log(f"[rotation] WARNING: could not persist enabled state: {e}")
