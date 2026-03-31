#!/usr/bin/env bash
set -e

#############################################
# Multi-Instance Dusk Node Installer
# Modified version to support multiple node instances on the same server
#############################################

# Default values
NETWORK="mainnet"
INSTANCE=1
FEATURE=""
ARCH=$(uname -m)
KADCAST_BASE_PORT=9000

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Help message
show_help() {
    cat << EOF
Multi-Instance Dusk Node Installer

Usage: $0 [OPTIONS]

OPTIONS:
    --instance N           Instance number (default: 1)
                          Creates installation in /opt/dusk{N}
                          Uses Kadcast port 9000+N (e.g., 9001, 9002, 9003...)
    
    --network NETWORK      Network to install (default: mainnet)
                          Options: mainnet, testnet, devnet
    
    --feature FEATURE      Optional feature flag
                          Options: archive, prover
    
    -h, --help            Show this help message

EXAMPLES:
    # Install first instance on testnet
    sudo bash $0 --instance 1 --network testnet
    
    # Install second instance on mainnet
    sudo bash $0 --instance 2 --network mainnet
    
    # Install third instance with archive feature
    sudo bash $0 --instance 3 --feature archive

NOTES:
    - Each instance gets a unique installation directory: /opt/dusk{N}
    - Kadcast ports: Instance 1 uses 9001, Instance 2 uses 9002, etc.
    - Standard ports (9000/8080) remain free for regular installations
    - HTTP is disabled by default (listen = false)
    - Service name will be: rusk-{N}
    - Log files: /var/log/rusk-{N}.log

EOF
    exit 0
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --instance)
            INSTANCE="$2"
            shift 2
            ;;
        --network)
            NETWORK="$2"
            shift 2
            ;;
        --feature)
            FEATURE="$2"
            shift 2
            ;;
        -h|--help)
            show_help
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            show_help
            ;;
    esac
done

# Validate instance number
if ! [[ "$INSTANCE" =~ ^[0-9]+$ ]] || [ "$INSTANCE" -lt 1 ]; then
    echo -e "${RED}Error: Instance must be a positive integer${NC}"
    exit 1
fi

# Validate network
if [[ ! "$NETWORK" =~ ^(mainnet|testnet|devnet)$ ]]; then
    echo -e "${RED}Error: Network must be mainnet, testnet, or devnet${NC}"
    exit 1
fi

# Calculate ports (starts from 9001 for instance 1, keeping 9000/8080 free for standard installation)
KADCAST_PORT=$((KADCAST_BASE_PORT + INSTANCE))
HTTP_PORT=$((8080 + INSTANCE))

# Set paths based on instance
DUSK_ROOT="/opt/dusk${INSTANCE}"
SERVICE_NAME="rusk-${INSTANCE}"
LOG_FILE="/var/log/rusk-${INSTANCE}.log"
LOG_FILE_RECOVERY="/var/log/rusk-${INSTANCE}-recovery.log"

# Check if running as root
if [ "$EUID" -ne 0 ]; then 
    echo -e "${RED}Error: This script must be run as root (use sudo)${NC}"
    exit 1
fi

# Get current user (the one who invoked sudo)
CURRENT_USER="${SUDO_USER:-$USER}"

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}Multi-Instance Dusk Node Installer${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo -e "Instance Number:    ${YELLOW}${INSTANCE}${NC}"
echo -e "Network:            ${YELLOW}${NETWORK}${NC}"
echo -e "Installation Path:  ${YELLOW}${DUSK_ROOT}${NC}"
echo -e "Kadcast Port:       ${YELLOW}${KADCAST_PORT}/udp${NC}"
echo -e "HTTP Status:        ${YELLOW}Disabled${NC}"
echo -e "Service Name:       ${YELLOW}${SERVICE_NAME}${NC}"
echo -e "Log File:           ${YELLOW}${LOG_FILE}${NC}"
if [ -n "$FEATURE" ]; then
    echo -e "Feature:            ${YELLOW}${FEATURE}${NC}"
fi
echo ""
echo -e "${YELLOW}NOTE: HTTP is disabled by default to prevent port conflicts${NC}"
echo -e "${YELLOW}      You can enable it later by editing ${DUSK_ROOT}/conf/rusk.toml${NC}"
echo ""
read -p "Continue with installation? (y/N) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Installation cancelled."
    exit 0
fi

# Check if instance already exists
if [ -d "$DUSK_ROOT" ]; then
    echo -e "${YELLOW}Warning: Instance ${INSTANCE} already exists at ${DUSK_ROOT}${NC}"
    read -p "Do you want to upgrade/reinstall? (y/N) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Installation cancelled."
        exit 0
    fi
    echo "Stopping existing service..."
    systemctl stop ${SERVICE_NAME} 2>/dev/null || true
fi

# Create directory structure
echo -e "${GREEN}Creating directory structure...${NC}"
mkdir -p ${DUSK_ROOT}/{bin,conf,services,installer,rusk}
mkdir -p ${DUSK_ROOT}/installer/os

# Detect OS and architecture
echo -e "${GREEN}Detecting system...${NC}"
if [ -f /etc/os-release ]; then
    . /etc/os-release
    distro=$(echo "$ID" | tr '[:upper:]' '[:lower:]')
else
    echo -e "${RED}Error: Unable to detect OS. /etc/os-release not found.${NC}"
    exit 1
fi

# Normalize distro ID for compatible derivatives
case "$distro" in
    linuxmint*) distro="ubuntu" ;;
esac

echo "Detected OS: $ID"
echo "Normalized OS: $distro"
echo "Architecture: $ARCH"

# Map architecture
case "$ARCH" in
    x86_64)
        ARCH="x64"
        ;;
    aarch64|arm64)
        ARCH="arm64"
        ;;
    *)
        echo -e "${RED}Error: Unsupported architecture: $ARCH${NC}"
        exit 1
        ;;
esac

# Check OpenSSL version
echo -e "${GREEN}Checking OpenSSL version...${NC}"
if ! command -v openssl >/dev/null 2>&1 || [ "$(openssl version | awk '{print $2}' | cut -d. -f1)" -lt 3 ]; then
    echo -e "${RED}Error: OpenSSL 3 or higher is required${NC}"
    echo "Please upgrade your OS or install a newer version of OpenSSL"
    exit 1
fi

# Download installer package for OS-specific scripts
INSTALLER_URL="https://github.com/dusk-network/node-installer/tarball/main"
echo -e "${GREEN}Downloading installer package...${NC}"
curl -sL "$INSTALLER_URL" -o ${DUSK_ROOT}/installer/installer.tar.gz || {
    echo -e "${RED}Failed to download installer package${NC}"
    exit 1
}

tar xf ${DUSK_ROOT}/installer/installer.tar.gz --strip-components 1 --directory ${DUSK_ROOT}/installer

# Source OS-specific script if available
OS_SCRIPT="${DUSK_ROOT}/installer/os/${distro}.sh"
if [ -f "$OS_SCRIPT" ]; then
    echo -e "${GREEN}Loading OS-specific configuration for ${distro}...${NC}"
    source "$OS_SCRIPT"
else
    echo -e "${RED}Error: No OS support script found for ${distro}${NC}"
    echo "Supported distributions have scripts in ${DUSK_ROOT}/installer/os/"
    echo "Want to add support? See: https://github.com/dusk-network/node-installer#contributing-os-support"
    exit 1
fi

# Install dependencies (OS-specific function from sourced script)
if type install_deps >/dev/null 2>&1; then
    echo -e "${GREEN}Installing dependencies...${NC}"
    install_deps
else
    echo -e "${YELLOW}Warning: No install_deps function found in OS script${NC}"
fi

# Create dusk group if it doesn't exist
if ! getent group dusk >/dev/null 2>&1; then
    echo -e "${GREEN}Creating dusk group...${NC}"
    groupadd dusk
fi

# Add current user to dusk group
if ! groups "$CURRENT_USER" | grep -q "\bdusk\b"; then
    echo -e "${GREEN}Adding user $CURRENT_USER to dusk group...${NC}"
    usermod -aG dusk "$CURRENT_USER"
    echo "User $CURRENT_USER has been added to the dusk group."
    echo -e "${YELLOW}You may need to log out and back in for group changes to take effect.${NC}"
fi

# Fetch latest release version
echo -e "${GREEN}Fetching latest Rusk version...${NC}"
RELEASE_TAG=$(curl -s https://api.github.com/repos/dusk-network/rusk/releases/latest | grep '"tag_name"' | cut -d'"' -f4)
if [ -z "$RELEASE_TAG" ]; then
    echo -e "${RED}Failed to fetch latest release version${NC}"
    exit 1
fi
echo "Latest release: $RELEASE_TAG"

# Extract version number (remove 'v' prefix if present)
VERSION="${RELEASE_TAG#v}"
SANITIZED_VERSION=$(echo "$VERSION" | sed 's/-rc//')

# Build feature suffix if provided
FEATURE_SUFFIX=""
if [ -n "$FEATURE" ]; then
    FEATURE_SUFFIX="-${FEATURE}"
fi

# Download and install Rusk
download_component() {
    local component=$1
    local component_dir="${DUSK_ROOT}/bin"
    
    echo -e "${GREEN}Downloading ${component}...${NC}"
    
    local url="https://github.com/dusk-network/rusk/releases/download/${RELEASE_TAG}-${VERSION}/${component}-${SANITIZED_VERSION}-linux-${ARCH}${FEATURE_SUFFIX}.tar.gz"
    
    curl -sL "$url" -o ${DUSK_ROOT}/installer/${component}.tar.gz || {
        echo -e "${RED}Failed to download ${component}${NC}"
        echo "URL: $url"
        exit 1
    }
    
    tar xf ${DUSK_ROOT}/installer/${component}.tar.gz --strip-components 1 --directory "$component_dir"
    
    # Make binaries executable
    chmod +x ${component_dir}/*
}

# Download components
download_component "rusk"
download_component "rusk-wallet"

# Download network configuration
echo -e "${GREEN}Downloading ${NETWORK} configuration...${NC}"
CONFIG_URL="https://raw.githubusercontent.com/dusk-network/node-installer/main/conf/${NETWORK}.toml"
curl -sL "$CONFIG_URL" -o ${DUSK_ROOT}/conf/rusk.toml || {
    echo -e "${RED}Failed to download configuration for ${NETWORK}${NC}"
    exit 1
}

# Modify rusk.toml to disable HTTP and set Kadcast port
echo -e "${GREEN}Configuring rusk.toml...${NC}"

# Add HTTP disabled configuration at the end
cat >> ${DUSK_ROOT}/conf/rusk.toml << EOF

# HTTP Configuration - Disabled for multi-instance setup
[http]
listen = false
EOF

echo "HTTP disabled in configuration"

# Create service configuration for Kadcast
echo -e "${GREEN}Configuring Kadcast ports...${NC}"
cat > ${DUSK_ROOT}/services/rusk.conf.user << EOF
# Kadcast configuration for instance ${INSTANCE}
KADCAST_PUBLIC_ADDRESS=0.0.0.0:${KADCAST_PORT}
KADCAST_LISTEN_ADDRESS=0.0.0.0:${KADCAST_PORT}
EOF

echo "Kadcast configured on port ${KADCAST_PORT}"

# Create systemd service file
echo -e "${GREEN}Creating systemd service...${NC}"
cat > /etc/systemd/system/${SERVICE_NAME}.service << EOF
[Unit]
Description=Dusk Rusk Node - Instance ${INSTANCE}
After=network.target
Wants=network.target

[Service]
Type=simple
User=root
WorkingDirectory=${DUSK_ROOT}
ExecStart=${DUSK_ROOT}/bin/rusk --config ${DUSK_ROOT}/conf/rusk.toml
StandardOutput=append:${LOG_FILE}
StandardError=append:${LOG_FILE}
Restart=always
RestartSec=10
Environment="NETWORK=${NETWORK}"
EnvironmentFile=-/opt/dusk/services/dusk.conf
EnvironmentFile=-${DUSK_ROOT}/services/rusk.conf.user

[Install]
WantedBy=multi-user.target
EOF

# Set permissions
echo -e "${GREEN}Setting permissions...${NC}"
chown -R root:dusk ${DUSK_ROOT}
chmod -R 775 ${DUSK_ROOT}
chmod 750 ${DUSK_ROOT}/bin/*

# Create log files
touch ${LOG_FILE}
touch ${LOG_FILE_RECOVERY}
chown root:dusk ${LOG_FILE}
chown root:dusk ${LOG_FILE_RECOVERY}
chmod 664 ${LOG_FILE}
chmod 664 ${LOG_FILE_RECOVERY}

# Configure log rotation
if type configure_logrotate >/dev/null 2>&1; then
    echo -e "${GREEN}Configuring log rotation...${NC}"
    configure_logrotate
else
    # Default log rotation configuration
    cat > /etc/logrotate.d/${SERVICE_NAME} << EOF
${LOG_FILE} {
    daily
    rotate 7
    compress
    delaycompress
    missingok
    notifempty
    create 0664 root dusk
}

${LOG_FILE_RECOVERY} {
    daily
    rotate 7
    compress
    delaycompress
    missingok
    notifempty
    create 0664 root dusk
}
EOF
fi

# Reload systemd
systemctl daemon-reload

# Display firewall information
echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}Installation Complete!${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo -e "${YELLOW}IMPORTANT: Firewall Configuration${NC}"
echo "You need to open the following port for this instance:"
echo -e "  ${GREEN}${KADCAST_PORT}/udp${NC} - Kadcast consensus (REQUIRED)"
echo ""
echo "Example firewall commands:"
echo "  ufw allow ${KADCAST_PORT}/udp"
echo "  # OR"
echo "  iptables -A INPUT -p udp --dport ${KADCAST_PORT} -j ACCEPT"
echo ""
echo -e "${YELLOW}NEXT STEPS:${NC}"
echo ""
echo "1. Configure consensus keys:"
echo "   ${DUSK_ROOT}/bin/rusk-wallet export -d ${DUSK_ROOT}/conf -n consensus.keys"
echo ""
echo "2. Set consensus keys password (if not already set):"
echo "   echo 'DUSK_CONSENSUS_KEYS_PASS=your_password' | sudo tee /opt/dusk/services/dusk.conf"
echo ""
echo "3. Update Kadcast public address (replace YOUR_PUBLIC_IP):"
echo "   echo 'KADCAST_PUBLIC_ADDRESS=YOUR_PUBLIC_IP:${KADCAST_PORT}' | sudo tee ${DUSK_ROOT}/services/rusk.conf.user"
echo "   echo 'KADCAST_LISTEN_ADDRESS=0.0.0.0:${KADCAST_PORT}' | sudo tee -a ${DUSK_ROOT}/services/rusk.conf.user"
echo ""
echo "4. Enable and start the service:"
echo "   sudo systemctl enable ${SERVICE_NAME}"
echo "   sudo systemctl start ${SERVICE_NAME}"
echo ""
echo "5. Check service status:"
echo "   sudo systemctl status ${SERVICE_NAME}"
echo "   sudo journalctl -u ${SERVICE_NAME} -f"
echo ""
echo "6. View logs:"
echo "   tail -f ${LOG_FILE}"
echo ""
echo -e "${YELLOW}Installation Directory:${NC} ${DUSK_ROOT}"
echo -e "${YELLOW}Service Name:${NC} ${SERVICE_NAME}"
echo -e "${YELLOW}Kadcast Port:${NC} ${KADCAST_PORT}/udp"
echo -e "${YELLOW}HTTP:${NC} Disabled (can be enabled in ${DUSK_ROOT}/conf/rusk.toml)"
echo ""
echo -e "${GREEN}To enable HTTP later, edit ${DUSK_ROOT}/conf/rusk.toml:${NC}"
echo "  [http]"
echo "  listen = true"
echo "  listen_address = \"0.0.0.0:${HTTP_PORT}\""
echo ""
