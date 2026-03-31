#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# SOZU Dashboard — HTTPS direct access setup
# Run as root on your VPS.
#
# Two modes:
#   ./setup_https.sh self-signed          # IP-only, self-signed cert
#   ./setup_https.sh domain sozu.you.com  # domain + Let's Encrypt cert
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

PORT=7373
SERVER_DIR="$(cd "$(dirname "$0")" && pwd)"
CERT_DIR="/root/sozu_certs"
SYSTEMD_UNIT="/etc/systemd/system/sozu-dashboard.service"

RED='\033[0;31m'; GRN='\033[0;32m'; YLW='\033[1;33m'; CYN='\033[0;36m'; NC='\033[0m'
info()  { echo -e "${CYN}[info]${NC}  $*"; }
ok()    { echo -e "${GRN}[ok]${NC}    $*"; }
warn()  { echo -e "${YLW}[warn]${NC}  $*"; }
die()   { echo -e "${RED}[error]${NC} $*" >&2; exit 1; }

# ── Validate args ─────────────────────────────────────────────────────────────
MODE="${1:-}"
DOMAIN=""
[[ "$MODE" == "self-signed" || "$MODE" == "domain" ]] || {
    echo "Usage:"
    echo "  $0 self-signed"
    echo "  $0 domain your.domain.com"
    exit 1
}
if [[ "$MODE" == "domain" ]]; then
    DOMAIN="${2:-}"
    [[ -n "$DOMAIN" ]] || die "domain mode requires a domain name as second argument"
fi

# ── Check auth env vars ───────────────────────────────────────────────────────
[[ -n "${SOZU_DASHBOARD_USER:-}" ]] || die "Set SOZU_DASHBOARD_USER before running this script"
[[ -n "${SOZU_DASHBOARD_PASS:-}" ]] || die "Set SOZU_DASHBOARD_PASS before running this script"

# ── Locate server file and ensure it's importable as provisioner_server ───────
echo ""
echo "Available server files in $SERVER_DIR:"
ls "$SERVER_DIR"/server*.py 2>/dev/null | xargs -I{} basename {} || true
echo ""
read -rp "Enter server filename (e.g. server.0.5.32.py): " SERVER_FILENAME
SERVER_PY="$SERVER_DIR/$SERVER_FILENAME"
[[ -f "$SERVER_PY" ]] || die "File not found: $SERVER_PY"

SYMLINK="$SERVER_DIR/provisioner_server.py"
ln -sf "$SERVER_PY" "$SYMLINK"
ok "Symlinked $SERVER_FILENAME → provisioner_server.py"


info "Installing gunicorn…"
pip install gunicorn --break-system-packages -q
ok "gunicorn installed"

# ── Firewall ──────────────────────────────────────────────────────────────────
info "Opening port $PORT in ufw…"
ufw allow "$PORT/tcp" comment "SOZU Dashboard HTTPS" || warn "ufw not available, skip"
ok "Port $PORT open"

# ── Certificate setup ─────────────────────────────────────────────────────────
mkdir -p "$CERT_DIR"

if [[ "$MODE" == "self-signed" ]]; then
    info "Generating self-signed TLS certificate…"
    VPS_IP="$(curl -s https://ifconfig.me || hostname -I | awk '{print $1}')"
    openssl req -x509 -newkey rsa:4096 -sha256 -days 3650 \
        -nodes \
        -keyout "$CERT_DIR/key.pem" \
        -out    "$CERT_DIR/cert.pem" \
        -subj   "/CN=$VPS_IP/O=SOZU Dashboard/C=DE" \
        -addext "subjectAltName=IP:$VPS_IP" \
        2>/dev/null
    chmod 600 "$CERT_DIR/key.pem"
    ok "Self-signed cert generated for $VPS_IP  →  $CERT_DIR/"
    echo ""
    warn "Browser will show a security warning on first visit."
    warn "Click 'Advanced' → 'Proceed' (Chrome) or 'Accept the Risk' (Firefox)."
    warn "After accepting, the connection is fully encrypted."
    CERTFILE="$CERT_DIR/cert.pem"
    KEYFILE="$CERT_DIR/key.pem"
    ACCESS_URL="https://$VPS_IP:$PORT"

elif [[ "$MODE" == "domain" ]]; then
    info "Setting up Let's Encrypt certificate for $DOMAIN…"
    # Install certbot
    apt-get install -y certbot 2>/dev/null || pip install certbot --break-system-packages -q
    # Obtain cert in standalone mode (temporarily binds port 80)
    # Port 80 must be open for ACME challenge
    ufw allow 80/tcp temporarily 2>/dev/null || true
    certbot certonly --standalone \
        --non-interactive \
        --agree-tos \
        --register-unsafely-without-email \
        -d "$DOMAIN"
    CERTFILE="/etc/letsencrypt/live/$DOMAIN/fullchain.pem"
    KEYFILE="/etc/letsencrypt/live/$DOMAIN/privkey.pem"
    ok "Let's Encrypt certificate obtained for $DOMAIN"
    ACCESS_URL="https://$DOMAIN:$PORT"

    # Auto-renewal hook: reload gunicorn after cert renewal
    cat > /etc/letsencrypt/renewal-hooks/deploy/sozu-dashboard.sh << 'HOOK'
#!/bin/bash
systemctl reload sozu-dashboard 2>/dev/null || true
HOOK
    chmod +x /etc/letsencrypt/renewal-hooks/deploy/sozu-dashboard.sh
    ok "Auto-renewal hook installed"
fi

# ── Systemd service ───────────────────────────────────────────────────────────
info "Creating systemd service…"
cat > "$SYSTEMD_UNIT" << EOF
[Unit]
Description=SOZU Provisioner Dashboard
After=network.target
Wants=network.target

[Service]
Type=simple
User=root
WorkingDirectory=$SERVER_DIR
Environment="SOZU_DASHBOARD_USER=${SOZU_DASHBOARD_USER}"
Environment="SOZU_DASHBOARD_PASS=${SOZU_DASHBOARD_PASS}"
ExecStart=$(which gunicorn) \\
    --workers 1 \\
    --threads 8 \\
    --worker-class gthread \\
    --bind 0.0.0.0:${PORT} \\
    --timeout 120 \\
    --certfile ${CERTFILE} \\
    --keyfile  ${KEYFILE} \\
    --access-logfile - \\
    --error-logfile  - \\
    provisioner_server:app
Restart=always
RestartSec=5
KillMode=mixed
TimeoutStopSec=10

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable  sozu-dashboard
systemctl restart sozu-dashboard
sleep 2
systemctl is-active --quiet sozu-dashboard && ok "Service running" || {
    warn "Service failed to start, check: journalctl -u sozu-dashboard -n 40"
    exit 1
}

# ── Done ──────────────────────────────────────────────────────────────────────
echo ""
echo -e "${GRN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${GRN}  SOZU Dashboard is live${NC}"
echo -e "${GRN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""
echo -e "  URL:      ${CYN}${ACCESS_URL}${NC}"
echo -e "  User:     ${SOZU_DASHBOARD_USER}"
echo -e "  Password: (as set in SOZU_DASHBOARD_PASS)"
echo ""
echo "  Service management:"
echo "    systemctl status  sozu-dashboard"
echo "    systemctl restart sozu-dashboard"
echo "    journalctl -u sozu-dashboard -f"
echo ""
