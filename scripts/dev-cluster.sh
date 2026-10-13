#!/usr/bin/env bash
# Run a 3-node Kosha cluster on this machine for development.
#
#   scripts/dev-cluster.sh start   # build and start nodes on :9001-9003
#   scripts/dev-cluster.sh stop
#   scripts/dev-cluster.sh kill 1  # SIGKILL one node to watch failover
#
# S3 credentials: admin / admin-secret-key. Console: http://127.0.0.1:9001/_kosha/ui/
set -euo pipefail
cd "$(dirname "$0")/.."
DIR=dev-cluster
BIN=target/release/kosha
export KOSHA_ACCESS_KEY=admin KOSHA_SECRET_KEY=admin-secret-key
export KOSHA_CLUSTER_SECRET=dev-cluster-secret-change-me

start_node() {
  local id=$1; shift
  mkdir -p "$DIR"
  nohup "$BIN" --node-id "$id" --listen "127.0.0.1:900$id" \
    --cluster-listen "127.0.0.1:910$id" --data-dir "$DIR/node$id" "$@" \
    > "$DIR/node$id.log" 2>&1 &
  echo $! > "$DIR/node$id.pid"
  echo "node $id: S3 http://127.0.0.1:900$id  (log $DIR/node$id.log)"
}

case "${1:-start}" in
  start)
    if [ -d ui ] && command -v npm >/dev/null; then (cd ui && npm ci --silent && npm run build --silent); fi
    cargo build --release -p kosha-server
    # A node that already has cluster state ignores --init/--join.
    start_node 1 --init
    sleep 1
    start_node 2 --join 127.0.0.1:9101
    start_node 3 --join 127.0.0.1:9101
    echo "console: http://127.0.0.1:9001/_kosha/ui/  (admin / admin-secret-key)"
    ;;
  stop)
    for f in "$DIR"/node*.pid; do [ -f "$f" ] && kill "$(cat "$f")" 2>/dev/null || true; rm -f "$f"; done
    ;;
  kill)
    kill -9 "$(cat "$DIR/node${2:?node id}.pid")" && echo "killed node $2"
    ;;
  restart)
    start_node "${2:?node id}"
    ;;
  *)
    echo "usage: $0 start|stop|kill <id>|restart <id>" >&2
    exit 1
    ;;
esac
