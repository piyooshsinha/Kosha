#!/usr/bin/env bash
# Build Kosha from this checkout and install it as a systemd service, for a
# single-node install on a home server, NUC or VM (Debian/Ubuntu/Fedora/Arch).
#
#   sudo scripts/install.sh            # build, install, start on 0.0.0.0:9000
#   sudo scripts/install.sh uninstall  # stop and remove the service (keeps data)
#
# Settings (environment variables, read on first install):
#   KOSHA_LISTEN    default 0.0.0.0:9000
#   KOSHA_DATA_DIR  default /var/lib/kosha
#
# Credentials are generated once into /etc/kosha/kosha.env (mode 600).
# Edit that file and `systemctl restart kosha` to change any setting.
set -euo pipefail
cd "$(dirname "$0")/.."

LISTEN=${KOSHA_LISTEN:-0.0.0.0:9000}
DATA_DIR=${KOSHA_DATA_DIR:-/var/lib/kosha}
ENV_FILE=/etc/kosha/kosha.env
UNIT=/etc/systemd/system/kosha.service
BIN=/usr/local/bin/kosha

[ "$(id -u)" = 0 ] || { echo "run with sudo" >&2; exit 1; }
# Build as the user who invoked sudo, so target/ and ~/.cargo stay theirs.
BUILD_USER=${SUDO_USER:-root}
as_user() { if [ "$BUILD_USER" = root ]; then bash -lc "$1"; else sudo -u "$BUILD_USER" -H bash -lc "$1"; fi; }

if [ "${1:-}" = uninstall ]; then
  systemctl disable --now kosha 2>/dev/null || true
  rm -f "$UNIT" "$BIN"
  systemctl daemon-reload
  echo "removed the service and binary; data kept in $DATA_DIR, settings in $ENV_FILE"
  exit 0
fi

echo "== build dependencies"
if command -v cc >/dev/null && command -v curl >/dev/null && command -v openssl >/dev/null; then
  :
elif command -v apt-get >/dev/null; then
  apt-get update -qq && apt-get install -y -qq build-essential pkg-config curl openssl >/dev/null
elif command -v dnf >/dev/null; then
  dnf install -y -q gcc make pkgconf-pkg-config curl openssl >/dev/null
elif command -v pacman >/dev/null; then
  pacman -S --needed --noconfirm base-devel curl openssl >/dev/null
fi
if ! as_user 'command -v cargo' >/dev/null; then
  as_user 'curl -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal'
fi

echo "== build (takes a few minutes the first time)"
if as_user 'command -v npm' >/dev/null; then
  as_user 'cd ui && npm ci --silent && npm run build --silent'
else
  echo "npm not found: the web console is left out (install Node.js 20+ and re-run to add it)"
fi
as_user 'cargo build --release -p kosha-server'

echo "== install"
install -m 755 target/release/kosha "$BIN"
id kosha >/dev/null 2>&1 || useradd --system --home-dir "$DATA_DIR" --shell /usr/sbin/nologin kosha
install -d -o kosha -g kosha -m 750 "$DATA_DIR"
install -d -m 755 /etc/kosha
if [ ! -f "$ENV_FILE" ]; then
  umask 077
  cat > "$ENV_FILE" <<EOF
KOSHA_LISTEN=$LISTEN
KOSHA_DATA_DIR=$DATA_DIR
KOSHA_ACCESS_KEY=admin
KOSHA_SECRET_KEY=$(openssl rand -hex 20)
EOF
  umask 022
fi

cat > "$UNIT" <<'EOF'
[Unit]
Description=Kosha S3-compatible object store
After=network-online.target
Wants=network-online.target

[Service]
User=kosha
Group=kosha
EnvironmentFile=/etc/kosha/kosha.env
ExecStart=/usr/local/bin/kosha
Restart=on-failure
RestartSec=2
LimitNOFILE=1048576
# io_uring needs locked memory for registered buffers (--io-direct).
LimitMEMLOCK=infinity
NoNewPrivileges=yes
ProtectSystem=strict
ProtectHome=yes
PrivateTmp=yes
ReadWritePaths=/var/lib/kosha

[Install]
WantedBy=multi-user.target
EOF
# A custom data directory must be writable under ProtectSystem=strict.
sed -i "s|^ReadWritePaths=.*|ReadWritePaths=$(sed -n 's/^KOSHA_DATA_DIR=//p' "$ENV_FILE")|" "$UNIT"

systemctl daemon-reload
systemctl enable kosha >/dev/null 2>&1
systemctl restart kosha

# shellcheck disable=SC1090
. "$ENV_FILE"
port=${KOSHA_LISTEN##*:}
for _ in $(seq 40); do
  code=$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$port/" || true)
  [ "$code" != 000 ] && break
  sleep 0.5
done
if [ "$code" = 000 ]; then
  echo "kosha did not start; see: journalctl -u kosha -n 50" >&2
  exit 1
fi

ip=$(hostname -I 2>/dev/null | awk '{print $1}')
cat <<EOF

Kosha is running (systemctl status kosha, journalctl -u kosha -f).
  S3 endpoint  http://${ip:-127.0.0.1}:$port
  Console      http://${ip:-127.0.0.1}:$port/_kosha/ui/
  Access key   $KOSHA_ACCESS_KEY
  Secret key   $KOSHA_SECRET_KEY   (also in $ENV_FILE)
  Data         $KOSHA_DATA_DIR
EOF
