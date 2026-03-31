"""
rues.py — RUES WebSocket thread, event log, decode, and local node WS monitors.

Owns:
  _event_log, _event_log_lock       — shared with routes/actions.py via _get_event_log()
  _RUES_WS_URL, _RUES_SESSION_ID    — runtime-mutable
  _rues_node_host()                 — base HTTP URL for the active node
  _local_node_heights               — per-node block heights from local WS

Imports: config, wallet (for operator_cmd in decode), rotation (callbacks).
"""
import json
import threading
import time
from datetime import datetime

from .config import (
    _log, CONTRACT_ID, RUSK_VERSION, NODE_INDICES, cfg,
)

# ── Event log ─────────────────────────────────────────────────────────────────
_event_log:      list = []
_event_log_lock        = threading.Lock()
_event_seen:     set  = set()
_rues_seen:      set  = set()

_CONFIRM_SECS           = 120
_POLLER_STALE_THRESHOLD = 1

# ── RUES connection state ─────────────────────────────────────────────────────
_RUES_RUNNING     = False
_RUES_SESSION_ID: str = ""
_RUES_WS_URL      = "ws://127.0.0.1:8080/on"   # default: local node 0
_RUES_BLOCK_CACHE: dict       = {}
_RUES_BLOCK_CACHE_LOCK        = threading.Lock()

_RUES_TOPICS = [
    "activate", "deactivate", "liquidate", "terminate",
    "reward-terminate", "reward-recycle", "unstake", "deposit",
    "donate", "reward",
    "block_accepted",
    "tx/included", "tx/removed", "tx/executed",
]
_RUES_DEFAULT_TOPICS = list(_RUES_TOPICS)
_RUES_SUB_RESULTS: dict = {t: "unsubscribed" for t in _RUES_TOPICS}

# ── Local node heights ────────────────────────────────────────────────────────
_local_node_heights: dict      = {0: None, 1: None, 2: None}
_local_node_heights_lock       = threading.Lock()

# ── RUES debug log ────────────────────────────────────────────────────────────
_rues_debug_log: list   = []
_rues_debug_lock        = threading.Lock()

# ── TX sender cache (for _check_external_op) ─────────────────────────────────
_rues_tx_sender_cache: dict      = {}
_rues_tx_sender_cache_lock       = threading.Lock()


def _rues_node_host() -> str:
    """HTTP base URL derived from the currently active _RUES_WS_URL."""
    return (_RUES_WS_URL
            .replace("wss://", "https://")
            .replace("ws://",  "http://")
            .rstrip("/on").rstrip("/"))


def _rues_topic_url(topic: str) -> str:
    base = _rues_node_host()
    if topic == "block_accepted":
        return f"{base}/on/blocks/accepted"
    if topic.startswith("tx/"):
        return f"{base}/on/transactions/{topic[3:]}"
    return f"{base}/on/contracts:{CONTRACT_ID}/{topic}"


def _rues_dbg(msg: str, raw_hex: str = "", ping: bool = False, tx_count: int = -1):
    ts   = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    ts_s = ts[:8]
    entry = {"ts": ts, "msg": msg, "raw": raw_hex, "ping": ping, "tx_count": tx_count, "dups": 0}
    with _rues_debug_lock:
        if _rues_debug_log:
            prev = _rues_debug_log[-1]
            if (prev["msg"][:120] == msg[:120] and prev["ts"][:8] == ts_s and not ping):
                prev["dups"] = prev.get("dups", 0) + 1
                return
        _rues_debug_log.append(entry)
        if len(_rues_debug_log) > 2000:
            del _rues_debug_log[:-2000]


def _wait_for_event(topic_substr: str, addr_prefix: str,
                    log_cursor: int, timeout: int = _CONFIRM_SECS) -> tuple:
    """Poll _event_log for a matching event. Returns (status, block_height)."""
    deadline     = time.time() + timeout
    events_seen  = 0
    baseline_len = log_cursor

    while time.time() < deadline:
        with _event_log_lock:
            entries   = list(_event_log[log_cursor:])
            total_now = len(_event_log)
        events_seen  += total_now - baseline_len
        baseline_len  = total_now

        for ev in entries:
            if topic_substr not in ev.get("topic", ""):
                continue
            haystack = json.dumps(ev.get("decoded", "")) + ev.get("data", "")
            if addr_prefix in haystack:
                return "confirmed", ev.get("block_height", 0)
        time.sleep(3)

    if events_seen < _POLLER_STALE_THRESHOLD:
        return "stale", 0
    return "timeout", 0


def _log_event(entry: dict) -> None:
    dedup_key = (entry.get("height"), entry.get("_source", ""), entry.get("topic", ""))
    is_rues   = bool(entry.get("_rues"))
    with _event_log_lock:
        if dedup_key in _event_seen:
            if is_rues and dedup_key not in _rues_seen:
                for existing in reversed(_event_log):
                    ek = (existing.get("height"), existing.get("_source", ""), existing.get("topic", ""))
                    if ek == dedup_key:
                        existing["_rues"] = True
                        break
                _rues_seen.add(dedup_key)
            return
        _event_seen.add(dedup_key)
        if is_rues:
            _rues_seen.add(dedup_key)
        _event_log.append(entry)
        if len(_event_log) > 2000:
            del _event_log[:-2000]


def _rues_parse_message(raw: bytes) -> tuple:
    brace_idx = raw.find(b'{')
    if brace_idx == -1:
        return {}, raw
    json_bytes = raw[brace_idx:]
    depth = 0
    for i, b in enumerate(json_bytes):
        if b == ord('{'):  depth += 1
        elif b == ord('}'): depth -= 1
        if depth == 0:
            try:
                hdr = json.loads(json_bytes[:i+1].decode("utf-8", errors="replace"))
                if isinstance(hdr, dict) and "Content-Location" in hdr:
                    return hdr, json_bytes[i+1:]
            except Exception:
                pass
            break
    return {}, raw


def _rues_decode(topic: str, data_bytes: bytes, is_block_event: bool = False):
    if is_block_event or topic.startswith("tx/"):
        try:
            return json.loads(data_bytes.decode("utf-8", errors="replace").strip())
        except Exception as e:
            return {"_raw_len": len(data_bytes), "_parse_err": str(e)}
    import subprocess as _sp
    hex_val   = "0x" + data_bytes.hex()
    node_host = _rues_node_host()
    url       = f"{node_host}/on/driver:{CONTRACT_ID}/decode_event:{topic}"
    try:
        r = _sp.run(["curl", "-s", "-X", "POST", url,
                     "-H", f"rusk-version: {RUSK_VERSION}", "-d", hex_val],
                    capture_output=True, text=True, timeout=12)
        stdout = r.stdout.strip()
        if not stdout:
            return {"_decode_error": "empty response", "_payload_len": len(data_bytes)}
        return json.loads(stdout)
    except Exception as e:
        return {"_decode_error": str(e), "_payload_len": len(data_bytes)}


def _rues_subscribe(session_id: str):
    import urllib.request as _ur, urllib.error as _uerr
    for topic in _RUES_DEFAULT_TOPICS:
        url = _rues_topic_url(topic)
        try:
            req = _ur.Request(url, method="GET",
                              headers={"Rusk-Session-Id": session_id,
                                       "rusk-version": RUSK_VERSION})
            with _ur.urlopen(req, timeout=8) as r:
                resp_body = r.read()
            _RUES_SUB_RESULTS[topic] = f"ok ({resp_body[:40]})"
            _rues_dbg(f"subscribed {topic}")
        except _uerr.HTTPError as e:
            if e.code == 424:
                _RUES_SUB_RESULTS[topic] = "ok (auto)"
                _rues_dbg(f"subscribed {topic} (424 → auto-subscribed by node)")
            else:
                _RUES_SUB_RESULTS[topic] = f"FAILED: HTTP {e.code}"
                _rues_dbg(f"subscribe {topic} FAILED: HTTP {e.code}")
        except Exception as e:
            _RUES_SUB_RESULTS[topic] = f"FAILED: {e}"
            _rues_dbg(f"subscribe {topic} FAILED: {e}")


def _rues_thread():
    global _RUES_RUNNING, _RUES_SESSION_ID
    try:
        import websocket as _ws
    except ImportError as ie:
        _rues_dbg(f"FATAL: websocket-client not installed: {ie}")
        _RUES_RUNNING = False
        return

    backoff = 2
    _rues_dbg("thread started")

    while _RUES_RUNNING:
        session_id = ""
        try:
            _rues_dbg(f"connecting to {_RUES_WS_URL}")
            ws = _ws.create_connection(_RUES_WS_URL, timeout=30, ping_interval=0)
            raw_sid = ws.recv()
            if isinstance(raw_sid, bytes):
                raw_sid = raw_sid.decode("utf-8", errors="replace")
            session_id = raw_sid.strip().strip('"')
            _RUES_SESSION_ID = session_id
            _rues_dbg(f"connected session={session_id}")
            _log(f"[rues] connected — session={session_id[:16]}…")

            time.sleep(0.3)   # let remote node index the session before subscribing
            _rues_subscribe(session_id)

            # Seed pool balance cache
            try:
            from .pool import _pool_fetch_real
                _pool_fetch_real()
            except Exception:
                pass

            last_ping = time.time()
            backoff   = 2

            while _RUES_RUNNING:
                if time.time() - last_ping > 30:
                    try:
                        ws.ping()
                        last_ping = time.time()
                        _rues_dbg("ping sent", "", ping=True)
                    except Exception:
                        break

                ws.settimeout(5)
                try:
                    raw = ws.recv()
                except _ws.WebSocketTimeoutException:
                    continue
                except Exception:
                    break

                if not raw:
                    continue

                raw_bytes     = raw if isinstance(raw, bytes) else raw.encode("utf-8")
                hdr, payload  = _rues_parse_message(raw_bytes)
                if not hdr:
                    continue

                location      = hdr.get("Content-Location", "")
                block_origin  = hdr.get("Rusk-Origin", "")

                is_block_event = "/blocks:" in location
                topic = ""
                if is_block_event:
                    topic = "block_accepted"
                else:
                    loc_parts = location.split("/")
                    for part in reversed(loc_parts):
                        if part and ":" not in part:
                            topic = part
                            break
                    if not topic:
                        for part in reversed(loc_parts):
                            if part.startswith("contracts:") or part.startswith("transactions"):
                                continue
                            topic = part
                            break

                if not topic:
                    continue

                decoded = _rues_decode(topic, payload, is_block_event=is_block_event)

                # Compact block display
                decoded_display = decoded
                _btx = -1
                if is_block_event and isinstance(decoded, dict):
                    hdr_raw = decoded.get("header") or {}
                    txs     = decoded.get("transactions") or []
                    _btx    = len(txs)
                    height_val = int(hdr_raw.get("height", 0) or 0)
                    block_hash = hdr_raw.get("hash", "")
                    if height_val and block_hash:
                        with _RUES_BLOCK_CACHE_LOCK:
                            _RUES_BLOCK_CACHE[block_hash] = height_val
                            if len(_RUES_BLOCK_CACHE) > 500:
                                del _RUES_BLOCK_CACHE[next(iter(_RUES_BLOCK_CACHE))]
                    decoded_display = {
                        "height": height_val, "hash": hdr_raw.get("hash", ""),
                        "tx_count": _btx,
                    }

                op   = decoded.get("operation", "") if isinstance(decoded, dict) else ""
                display_topic = f"{topic}-{op}" if op else topic

                with _RUES_BLOCK_CACHE_LOCK:
                    height = _RUES_BLOCK_CACHE.get(block_origin, 0)

                entry = {
                    "ts":      datetime.now().strftime("%H:%M:%S"),
                    "height":  height,
                    "topic":   display_topic,
                    "decoded": decoded_display,
                    "data":    payload.hex(),
                    "_source": CONTRACT_ID,
                    "_rues":   True,
                    "_origin": block_origin,
                }
                _log_event(entry)

                empty_tag = " [empty]" if (topic == "block_accepted" and _btx == 0) else ""
                tx_info   = f" txs={_btx}" if topic == "block_accepted" else ""
                _rues_dbg(f"decoded {display_topic}{empty_tag}{tx_info}", tx_count=_btx)

                # ── Rotation callbacks ────────────────────────────────────────
                try:
            from .rotation import (
                        _on_block_accepted, _on_deposit_event,
                        _on_liquidate_event, _on_reward_recycle_event,
                    )
                    from .config import cfg as _cfg_fn
                    if topic == "block_accepted" and isinstance(decoded_display, dict):
                        h = decoded_display.get("height", 0)
                        if h:
                            _on_block_accepted(h)
                    elif topic == "deposit" and isinstance(decoded_display, dict):
                        _on_deposit_event(decoded_display)
                    elif topic == "liquidate" and isinstance(decoded_display, dict):
                        our_provs = [_cfg_fn(f"prov_{i}_address") for i in range(3)]
                        is_own    = decoded_display.get("provisioner", "") in our_provs
                        _on_liquidate_event(decoded_display, is_own)
                    elif topic == "reward-recycle" and isinstance(decoded_display, dict):
                        _on_reward_recycle_event(decoded_display)
                except Exception as _cb_err:
                    _log(f"[rues] rotation callback error: {_cb_err}")

            try:
                ws.close()
            except Exception:
                pass

        except Exception as e:
            _log(f"[rues] connection error: {e}")
            _RUES_SESSION_ID = ""

        for t in _RUES_TOPICS:
            _RUES_SUB_RESULTS[t] = "unsubscribed"
        _rues_dbg(f"reconnect in {backoff}s")
        time.sleep(backoff)
        backoff = min(backoff * 2, 60)


def _local_node_ws_thread(node_idx: int):
    """Connect to local rusk WS for node_idx, update _local_node_heights on block_accepted."""
    try:
        import websocket as _ws
    except ImportError:
        return

    backoff = 2
    while True:
        port = int(cfg(f"node_{node_idx}_ws_port") or [8080, 8282, 8383][node_idx])
        url  = f"ws://localhost:{port}/on"
        try:
            ws = _ws.create_connection(url, timeout=10, ping_interval=0)
            raw_sid = ws.recv()
            if isinstance(raw_sid, bytes):
                raw_sid = raw_sid.decode("utf-8", errors="replace")
            session_id = raw_sid.strip().strip('"')
            _log(f"[node{node_idx}] connected session={session_id[:16]}…")

            import urllib.request as _ur2, urllib.error as _uerr2
            sub_url = f"http://localhost:{port}/on/blocks/accepted"
            try:
                req = _ur2.Request(sub_url, method="GET",
                                   headers={"Rusk-Session-Id": session_id,
                                            "rusk-version": RUSK_VERSION})
                with _ur2.urlopen(req, timeout=6) as r:
                    r.read()
                _log(f"[node{node_idx}] subscribed to blocks/accepted")
            except _uerr2.HTTPError as he:
                if he.code == 424:
                    _log(f"[node{node_idx}] 424 → auto-subscribed")
                else:
                    _log(f"[node{node_idx}] subscribe failed: HTTP {he.code}")
                    ws.close()
                    time.sleep(backoff)
                    backoff = min(backoff * 2, 60)
                    continue
            except Exception as se:
                _log(f"[node{node_idx}] subscribe failed: {se}")
                ws.close()
                time.sleep(backoff)
                backoff = min(backoff * 2, 60)
                continue

            last_ping = time.time()
            backoff   = 2

            while True:
                if time.time() - last_ping > 30:
                    try:
                        ws.ping()
                        last_ping = time.time()
                    except Exception:
                        break

                ws.settimeout(5)
                try:
                    raw = ws.recv()
                except _ws.WebSocketTimeoutException:
                    continue
                except Exception:
                    break

                if not raw:
                    continue

                raw_bytes = raw if isinstance(raw, bytes) else raw.encode("utf-8")
                hdr, payload = _rues_parse_message(raw_bytes)
                if "/blocks:" not in hdr.get("Content-Location", ""):
                    continue

                try:
                    decoded   = json.loads(payload.decode("utf-8", errors="replace").strip())
                    hdr_inner = decoded.get("header") or {}
                    height    = int(hdr_inner.get("height", 0) or 0)
                    if height:
                        with _local_node_heights_lock:
                            _local_node_heights[node_idx] = height
                        # Push to rotation_state for dashboard
                        try:
            from .rotation import _rotation_state
                            _rotation_state[f"node{node_idx}_height"] = height
                        except Exception:
                            pass
                except Exception:
                    pass

            try:
                ws.close()
            except Exception:
                pass

        except Exception as e:
            _log(f"[node{node_idx}] connection error: {e}")

        time.sleep(backoff)
        backoff = min(backoff * 2, 60)


def _start_local_node_monitors():
    for idx in NODE_INDICES:
        threading.Thread(target=_local_node_ws_thread, args=(idx,),
                         daemon=True, name=f"node{idx}_ws").start()
    _log("[node_ws] local node height monitors started")


def _start_rues():
    global _RUES_RUNNING
    if not _RUES_RUNNING:
        _RUES_RUNNING = True
        threading.Thread(target=_rues_thread, daemon=True, name="rues").start()
        _log("[rues] subscriber started")
