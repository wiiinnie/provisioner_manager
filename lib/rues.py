"""
rues.py — RUES WebSocket event stream.

Protocol:
  1. Connect to wss://<node>/on  -> receive session ID string
  2. Subscribe: HTTP GET /on/<path>  with Rusk-Session-Id header
  3. Binary frame: [u32 LE header_len][JSON header][raw payload]
     Header contains Content-Location identifying the topic.
"""
import json
import struct
import threading
import time
from collections import deque
from datetime import datetime

from .config import _log, RUSK_VERSION

_ws_url:      str  = ""
_session_id:  str  = ""
_connected:   bool = False
_running:     bool = False
_sub_results: dict = {}
_sub_lock          = threading.Lock()
_state_lock        = threading.Lock()

_event_log: deque = deque(maxlen=500)
_log_lock          = threading.Lock()

# Raw frame log — stores every WS message regardless of parse success
_raw_log: deque = deque(maxlen=200)
_raw_log_lock     = threading.Lock()

# All subscribable topics: key -> URL path template (CONTRACT_ID substituted at runtime)
# Paths match Dusk RUES API exactly as documented.
TOPIC_PATHS = {
    "block_accepted":   "/on/blocks/accepted",
    "activate":         "/on/contracts:{cid}/activate",
    "deactivate":       "/on/contracts:{cid}/deactivate",
    "deposit":          "/on/contracts:{cid}/deposit",
    "donate":           "/on/contracts:{cid}/donate",
    "liquidate":        "/on/contracts:{cid}/liquidate",
    "reward":           "/on/contracts:{cid}/reward",
    "unstake":          "/on/contracts:{cid}/unstake",
    "tx/included":      "/on/transactions/included",
    "tx/executed":      "/on/transactions/executed",
}

# No virtual filters needed — reward events are filtered by operation in the UI if desired

DEFAULT_SUBSCRIBE = list(TOPIC_PATHS.keys())


def _path(key: str) -> str:
    from .config import CONTRACT_ID
    return TOPIC_PATHS[key].replace("{cid}", CONTRACT_ID)


# Reverse map: URL path suffix -> display key
# e.g. "stake_activate" -> "activate"
_LOCATION_TO_KEY = {
    _path_val.split("/")[-1].replace("{cid}", ""): key
    for key, _path_val in TOPIC_PATHS.items()
    if not _path_val.startswith("/on/blocks") and not _path_val.startswith("/on/transactions")
}
# Add tx and block mappings explicitly
_LOCATION_TO_KEY.update({
    "accepted": "block_accepted",
    "included": "tx/included",
    "executed": "tx/executed",
})


def _topic_from_location(location: str) -> str:
    """Derive display key from Content-Location."""
    loc = location.rstrip("/")
    segment = loc.split("/")[-1]
    # Strip contract entity prefix if present (e.g. "contracts:HASH" -> use segment after)
    return _LOCATION_TO_KEY.get(segment, segment)


def _parse_frame(raw: bytes):
    """Parse RUES binary frame.

    Two formats observed in the wild:
    A) [u32 LE header_len][JSON header bytes][payload bytes]  — documented format
    B) [some binary prefix][JSON header starting with {][payload bytes]  — some nodes

    Try A first, fall back to B (scan for first '{').
    """
    # Try format A: u32 LE header length
    if len(raw) >= 4:
        header_len = struct.unpack_from("<I", raw, 0)[0]
        if 0 < header_len < len(raw) - 4:
            try:
                header = json.loads(raw[4:4 + header_len])
                if isinstance(header, dict) and "Content-Location" in header:
                    return header, raw[4 + header_len:]
            except Exception:
                pass

    # Fall back to format B: scan for JSON header starting with '{'
    brace_idx = raw.find(b'{')
    if brace_idx == -1:
        return None
    json_bytes = raw[brace_idx:]
    depth = 0
    for i, b in enumerate(json_bytes):
        if b == ord('{'): depth += 1
        elif b == ord('}'): depth -= 1
        if depth == 0:
            try:
                header = json.loads(json_bytes[:i + 1])
                if isinstance(header, dict) and "Content-Location" in header:
                    return header, json_bytes[i + 1:]
            except Exception:
                pass
            break
    return None


def _http(sid: str, path: str, method: str = "GET") -> bool:
    import urllib.request as _ur, urllib.error as _ue
    from .config import _NODE_STATE_URL
    url = _NODE_STATE_URL.rstrip("/") + path
    try:
        req = _ur.Request(url, method=method,
                          headers={"Rusk-Session-Id": sid,
                                   "rusk-version": RUSK_VERSION})
        with _ur.urlopen(req, timeout=10) as r:
            r.read()
        return True
    except _ue.HTTPError as e:
        if e.code == 424:
            return True
        _log(f"[rues] HTTP {method} {path} => {e.code}")
        return False
    except Exception as e:
        _log(f"[rues] HTTP {method} {path} error: {e}")
        return False


def _decode_fn_args_inplace(parsed: dict) -> None:
    """Walk a decoded tx dict, find fn_args fields and decode them via the driver endpoint.
    Modifies parsed in-place, adding a '_fn_args_decoded' key alongside fn_args.
    Handles both tx/included (call at top level) and tx/executed (call under 'inner').
    """
    import base64 as _b64, subprocess as _sp
    from .config import _NODE_STATE_URL, CONTRACT_ID

    # Find the call dict — either top-level or under 'inner'
    call = parsed.get("call") or (parsed.get("inner") or {}).get("call")
    if not call:
        return

    fn_name = call.get("fn_name", "")
    fn_args = call.get("fn_args", "")
    if not fn_name or not fn_args:
        return

    try:
        raw_bytes = _b64.b64decode(fn_args)
        hex_val   = "0x" + raw_bytes.hex()
        url = f"{_NODE_STATE_URL}/on/driver:{CONTRACT_ID}/decode_input_fn:{fn_name}"
        r = _sp.run(
            ["curl", "-s", "-X", "POST", url,
             "-H", f"rusk-version: {RUSK_VERSION}",
             "-H", "Content-Type: text/plain",
             "-d", hex_val],
            capture_output=True, text=True, timeout=8)
        result = r.stdout.strip()
        if result and not result.startswith("<") and len(result) > 2:
            try:
                call["_fn_args_decoded"] = json.loads(result)
            except Exception:
                call["_fn_args_decoded"] = result[:500]
    except Exception as e:
        _log(f"[rues] fn_args decode {fn_name}: {e}")


def _decode_payload(location: str, payload: bytes) -> dict:
    loc = location.strip()
    is_block = "blocks" in loc  # handles blocks:HASH/accepted and /on/blocks/...
    is_tx    = "transactions" in loc  # handles transactions:HASH/executed and /on/transactions/...

    if is_block or is_tx:
        # Try JSON first (local nodes send JSON, testnet sends binary/protobuf)
        text = payload.decode("utf-8", errors="replace").strip()
        if text.startswith("{") or text.startswith("["):
            try:
                parsed = json.loads(text)
                # Decode fn_args in place if present (base64 -> hex -> driver decode)
                _decode_fn_args_inplace(parsed)
                return parsed
            except Exception:
                pass

        # Binary payload — try driver decode endpoint
        # Use decode_raw_transaction for tx events, decode_block for blocks
        topic_name = loc.rstrip("/").split("/")[-1]  # "accepted", "executed", "included"
        try:
            import subprocess as _sp
            from .config import _NODE_STATE_URL, CONTRACT_ID
            hex_val = "0x" + payload.hex()
            # Try the generic driver decode
            driver_topic = "decode_raw_transaction" if is_tx else "decode_block"
            url = f"{_NODE_STATE_URL}/on/driver:{CONTRACT_ID}/decode_event:{driver_topic}"
            r = _sp.run(
                ["curl", "-s", "-X", "POST", url,
                 "-H", f"rusk-version: {RUSK_VERSION}",
                 "-d", hex_val],
                capture_output=True, text=True, timeout=8)
            result = r.stdout.strip()
            _log(f"[rues] driver decode {driver_topic}: {result[:80]}")
            if result and not result.startswith("<") and not result.startswith("error"):
                try:
                    return json.loads(result)
                except Exception:
                    return {"_decoded_text": result[:500]}
        except Exception as e:
            _log(f"[rues] driver decode error: {e}")

        # Fall back: return hex preview so UI can show something
        return {
            "_raw_bytes": len(payload),
            "_hex_preview": payload[:32].hex(),
        }

    # contracts:HASH/stake_activate  or  /on/contracts:HASH/...
    if "contracts:" in loc:
        topic_name = loc.rstrip("/").split("/")[-1]
        try:
            import subprocess as _sp
            from .config import _NODE_STATE_URL, CONTRACT_ID
            hex_val = "0x" + payload.hex()
            url     = f"{_NODE_STATE_URL}/on/driver:{CONTRACT_ID}/decode_event:{topic_name}"
            r = _sp.run(
                ["curl", "-s", "-X", "POST", url,
                 "-H", f"rusk-version: {RUSK_VERSION}",
                 "-d", hex_val],
                capture_output=True, text=True, timeout=8)
            result = r.stdout.strip()
            if result and not result.startswith("<"):
                return json.loads(result)
        except Exception as e:
            _log(f"[rues] decode_event {topic_name}: {e}")
        return {"_raw_bytes": len(payload), "_hex": payload[:16].hex()}

    return {"_raw_bytes": len(payload)}


def _append_log(topic: str, header: dict, decoded: dict, payload: bytes) -> None:
    ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    height = None
    if topic == "block_accepted":
        h = decoded.get("header") or {}
        height = (int(h.get("height", 0) or 0)
                  or int(decoded.get("height", 0) or 0))
        # Content-Location: blocks:HASH/accepted — no height there
        # Try Rusk-Origin header if set
        if not height:
            origin = header.get("Rusk-Origin", "")
            import re as _re
            m = _re.search(r":(\d+)/", header.get("Content-Location", ""))
            if m:
                height = int(m.group(1))
        if height:
            _log(f"[rues] block height={height}")
    entry = {
        "ts":          ts,
        "topic":       topic,
        "height":      height,
        "decoded":     decoded,
        "payload_len": len(payload),
    }
    with _log_lock:
        _event_log.appendleft(entry)
    if height:
        try:
            from .nodes import _on_remote_block
            _on_remote_block(height)
        except Exception:
            pass
    # Fire event action engine
    try:
        from .events import on_event
        on_event(topic, decoded, height)
    except Exception as _ev_err:
        _log(f"[events] on_event({topic}) error: {_ev_err}")



def _rues_thread() -> None:
    global _session_id, _connected, _running, _ws_url
    try:
        import websocket as _ws
    except ImportError:
        _log("[rues] websocket-client not installed")
        return

    from .config import _NODE_STATE_URL
    ws_url = (_NODE_STATE_URL
              .replace("https://", "wss://")
              .replace("http://",  "ws://")
              .rstrip("/") + "/on")

    with _state_lock:
        _ws_url = ws_url

    backoff = 2
    _log(f"[rues] connecting to {ws_url}")

    while _running:
        ws = None
        try:
            ws = _ws.create_connection(ws_url, timeout=30)

            # First message = session ID
            raw_sid = ws.recv()
            sid = (raw_sid.decode() if isinstance(raw_sid, bytes) else raw_sid).strip().strip('"')
            with _state_lock:
                _session_id = sid
            _log(f"[rues] connected session={sid}")

            time.sleep(0.3)

            # Subscribe
            with _sub_lock:
                _sub_results.clear()
            for key in DEFAULT_SUBSCRIBE:
                path_str = _path(key)
                ok = _http(sid, path_str, "GET")
                with _sub_lock:
                    _sub_results[key] = "ok" if ok else "failed"
                _log(f"[rues] subscribe '{key}' {path_str} => {'OK' if ok else 'FAILED'}")

            _log("[rues] subscribed, listening for frames...")
            with _state_lock:
                _connected = True
            backoff     = 2
            frame_count = 0

            # Dedicated ping thread
            _ping_stop = threading.Event()
            def _ping_loop(wsc=ws, stop=_ping_stop):
                while not stop.wait(20):
                    try:
                        wsc.ping()
                    except Exception:
                        break
            threading.Thread(target=_ping_loop, daemon=True).start()

            while _running:
                ws.settimeout(30)
                try:
                    raw = ws.recv()
                except _ws.WebSocketTimeoutException:
                    _log(f"[rues] recv timeout ({frame_count} frames so far)")
                    continue
                except Exception as exc:
                    _log(f"[rues] recv error: {exc}")
                    break

                if not raw:
                    continue

                raw_bytes = raw if isinstance(raw, bytes) else raw.encode()
                frame_count += 1
                if frame_count <= 5:
                    _log(f"[rues] frame #{frame_count}: {len(raw_bytes)}B hex={raw_bytes[:16].hex()}")

                ts_raw = datetime.now().strftime("%H:%M:%S.%f")[:-3]
                raw_entry = {
                    "ts": ts_raw, "len": len(raw_bytes),
                    "hex": raw_bytes[:64].hex(),
                    "text": raw_bytes[:300].decode("utf-8", errors="replace"),
                    "parsed": False,
                    "payload_text": "",
                }
                with _raw_log_lock:
                    _raw_log.appendleft(raw_entry)

                result = _parse_frame(raw_bytes)
                if result is None:
                    continue

                header, payload = result
                raw_entry["parsed"]       = True
                raw_entry["location"]     = header.get("Content-Location", "")
                raw_entry["payload_text"] = payload.decode("utf-8", errors="replace")

                location = header.get("Content-Location", "")
                if not location:
                    continue

                topic   = _topic_from_location(location)
                decoded = _decode_payload(location, payload)
                # Update raw_entry payload_text with the fully decoded dict
                # (includes _fn_args_decoded if decode_input_fn succeeded)
                if isinstance(decoded, dict) and "_raw_bytes" not in decoded:
                    try:
                        raw_entry["payload_text"] = json.dumps(decoded, indent=2)
                    except Exception:
                        pass
                _append_log(topic, header, decoded, payload)

            _ping_stop.set()

        except Exception as exc:
            _log(f"[rues] error: {exc}")
        finally:
            try:
                if ws:
                    ws.close()
            except Exception:
                pass

        with _state_lock:
            _connected  = False
            _session_id = ""

        if _running:
            _log(f"[rues] reconnect in {backoff}s")
            time.sleep(backoff)
            backoff = min(backoff * 2, 60)

def start() -> None:
    global _running
    if _running:
        return
    _running = True
    threading.Thread(target=_rues_thread, daemon=True, name="rues").start()


def get_status() -> dict:
    with _state_lock:
        sid  = _session_id
        conn = _connected
        url  = _ws_url
    with _sub_lock:
        subs = dict(_sub_results)
    with _log_lock:
        log = list(_event_log)
    with _raw_log_lock:
        raw = list(_raw_log)
    return {
        "connected":      conn,
        "session_id":     sid,
        "ws_url":         url,
        "subscriptions":  subs,
        "all_topics":     list(TOPIC_PATHS.keys()),
        "log":            log,
        "raw_log":        raw,
    }


def subscribe_topic(key: str, action: str = "subscribe") -> dict:
    with _state_lock:
        sid = _session_id
    if not sid:
        return {"ok": False, "error": "no active session"}
    if key not in TOPIC_PATHS:
        return {"ok": False, "error": f"unknown topic: {key}"}
    method = "DELETE" if action == "unsubscribe" else "GET"
    ok     = _http(sid, _path(key), method)
    with _sub_lock:
        _sub_results[key] = ("ok" if ok else "failed") if action == "subscribe" else "unsubscribed"
    return {"ok": ok, "topic": key, "action": action}
