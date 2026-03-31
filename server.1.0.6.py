"""
server.py — Entry point.

Creates Flask app, registers blueprints, starts RUES + local node monitors +
rotation background threads.

systemd / gunicorn:
    gunicorn --workers 1 --bind 0.0.0.0:7373 server:app
Direct:
    python3 server.py
"""
import functools
import os
import secrets

from flask import Flask, Response, request
from flask_cors import CORS

from lib.config import (
    _log, configure_werkzeug_logger,
    NETWORK, CONTRACT_ID, WALLET_PATH, PORT, _NODE_STATE_URL,
    _load_config, cfg,
)

# ── Flask app ─────────────────────────────────────────────────────────────────
app = Flask(__name__)
CORS(app)
configure_werkzeug_logger()

# ── Optional HTTP Basic Auth ──────────────────────────────────────────────────
_AUTH_USER    = os.environ.get("SOZU_DASHBOARD_USER", "")
_AUTH_PASS    = os.environ.get("SOZU_DASHBOARD_PASS", "")
_AUTH_ENABLED = bool(_AUTH_USER and _AUTH_PASS)


def _check_auth(username: str, password: str) -> bool:
    ok_user = secrets.compare_digest(username.encode(), _AUTH_USER.encode())
    ok_pass = secrets.compare_digest(password.encode(), _AUTH_PASS.encode())
    return ok_user and ok_pass


@app.before_request
def _global_auth():
    if not _AUTH_ENABLED or request.method == "OPTIONS":
        return
    auth = request.authorization
    if not auth or not _check_auth(auth.username, auth.password):
        return Response("Authentication required.", 401,
                        {"WWW-Authenticate": 'Basic realm="SOZU Dashboard"'})


# ── Register blueprints ───────────────────────────────────────────────────────
from lib.routes.system    import bp as system_bp
from lib.routes.info      import bp as info_bp
from lib.routes.actions   import bp as actions_bp
from lib.routes.substrate import bp as substrate_bp

app.register_blueprint(system_bp)
app.register_blueprint(info_bp)
app.register_blueprint(actions_bp)
app.register_blueprint(substrate_bp)

# ── Startup: load config, start RUES + threads ────────────────────────────────
_rues_started = False


@app.before_request
def _ensure_started():
    global _rues_started
    if not _rues_started:
        _rues_started = True
        _load_config()
        from lib.assess import _ensure_own_keys
        from lib.wallet import get_password
        pw = get_password()
        if pw:
            import threading
            threading.Thread(target=_ensure_own_keys, args=(pw,), daemon=True).start()
        from lib.rues import _start_rues, _start_local_node_monitors
        _start_rues()
        _start_local_node_monitors()
        from lib.rotation import start_rotation_threads
        start_rotation_threads()


# ── Main ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    _load_config()
    print(f"  Provisioner Manager API  →  http://localhost:{PORT}")
    print(f"  Wallet: {WALLET_PATH}  |  Network: {NETWORK}")
    print(f"  Contract: {CONTRACT_ID[:20]}…")
    print(f"  Config: ~/.sozu_dashboard_config.json")
    if _AUTH_ENABLED:
        print(f"  Auth: enabled  (user: {_AUTH_USER})")
    else:
        print(f"  Auth: disabled  (set SOZU_DASHBOARD_USER / SOZU_DASHBOARD_PASS to enable)")
    print()

    from lib.rues import _start_rues, _start_local_node_monitors
    _start_rues()
    _start_local_node_monitors()
    from lib.rotation import start_rotation_threads
    start_rotation_threads()

    app.run(host="0.0.0.0", port=PORT, threaded=True, debug=False)
