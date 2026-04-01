"""
nodes.py — Minimal local node block-height monitor.

One thread per provisioner node connects to its local rusk WebSocket,
subscribes to blocks/accepted, and updates _node_heights on each block.

No RUES event routing. No rotation callbacks. Just heights.
"""
import json
import threading
import time

from .config import _log, RUSK_VERSION, NODE_INDICES, cfg

_node_heights: dict = {0: None, 1: None, 2: None}
_node_heights_lock  = threading.Lock()


def get_heights() -> dict:
    with _node_heights_lock:
        return dict(_node_heights)


def _on_remote_block(height: int) -> None:
    """Called by rues.py when a block_accepted event carries a height.
    Stored separately so the dashboard can display remote tip alongside local heights."""
    global _remote_height
    _remote_height = height


_remote_height: int = 0


def get_remote_height() -> int:
    return _remote_height


def _ws_thread(node_idx: int) -> None:
    try:
        import websocket as _ws
    except ImportError:
        _log(f"[node{node_idx}] websocket-client not installed — height unavailable")
        return

    backoff = 2
    while True:
        port = int(cfg(f"node_{node_idx}_ws_port") or [8080, 8282, 8383][node_idx])
        url  = f"ws://localhost:{port}/on"
        try:
            ws = _ws.create_connection(url, timeout=10, ping_interval=0)

            # Receive session ID
            raw = ws.recv()
            session_id = (raw.decode("utf-8") if isinstance(raw, bytes) else raw).strip().strip('"')
            _log(f"[node{node_idx}] connected session={session_id[:16]}…")

            # Subscribe to blocks/accepted  (424 = auto-subscribed by local node, fine)
            import urllib.request as _ur, urllib.error as _uerr
            sub_url = f"http://localhost:{port}/on/blocks/accepted"
            try:
                req = _ur.Request(sub_url, method="GET",
                                  headers={"Rusk-Session-Id": session_id,
                                           "rusk-version": RUSK_VERSION})
                with _ur.urlopen(req, timeout=6) as r:
                    r.read()
            except _uerr.HTTPError as he:
                if he.code != 424:
                    _log(f"[node{node_idx}] subscribe failed HTTP {he.code}")
                    ws.close(); time.sleep(backoff); backoff = min(backoff*2, 60); continue
            except Exception as se:
                _log(f"[node{node_idx}] subscribe failed: {se}")
                ws.close(); time.sleep(backoff); backoff = min(backoff*2, 60); continue

            backoff   = 2
            last_ping = time.time()
            _log(f"[node{node_idx}] subscribed to blocks/accepted")

            while True:
                # Keepalive ping every 30s
                if time.time() - last_ping > 30:
                    try:
                        ws.ping(); last_ping = time.time()
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

                # Parse the RUES binary frame: find JSON header then payload
                raw_bytes = raw if isinstance(raw, bytes) else raw.encode()
                brace = raw_bytes.find(b'{')
                if brace == -1:
                    continue
                json_bytes = raw_bytes[brace:]
                depth, hdr_end = 0, -1
                for i, b in enumerate(json_bytes):
                    if b == ord('{'): depth += 1
                    elif b == ord('}'): depth -= 1
                    if depth == 0:
                        hdr_end = i; break
                if hdr_end == -1:
                    continue
                try:
                    hdr     = json.loads(json_bytes[:hdr_end+1])
                    payload = json_bytes[hdr_end+1:]
                except Exception:
                    continue

                if "/blocks:" not in hdr.get("Content-Location", ""):
                    continue

                try:
                    decoded = json.loads(payload.decode("utf-8", errors="replace").strip())
                    # Try both nested and flat payload formats
                    h = decoded.get("header") or {}
                    height = (int(h.get("height", 0) or 0)
                              or int(decoded.get("height", 0) or 0))
                    if height:
                        with _node_heights_lock:
                            _node_heights[node_idx] = height
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


def start_node_monitors() -> None:
    """Start one height-monitor thread per provisioner node."""
    for idx in NODE_INDICES:
        threading.Thread(target=_ws_thread, args=(idx,),
                         daemon=True, name=f"node{idx}_ws").start()
    _log("[nodes] local height monitors started")
