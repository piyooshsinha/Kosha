#!/usr/bin/env bash
# Benchmark Kosha, RustFS and MinIO with the same client and workloads.
#
#   scripts/compare-bench.sh <kosha-bin> <rustfs-bin> <minio-bin> [rounds] [servers...]
#
# Each server runs alone on 127.0.0.1:9000 with a fresh data directory under
# $BENCH_DIR (default /tmp/kosha-compare), at its default durability
# settings. The client is Kosha's s3bench (AWS SDK for Rust). Results are
# printed and appended to $BENCH_DIR/results.txt.
set -euo pipefail

KOSHA=$1 RUSTFS=$2 MINIO=$3
ROUNDS=${4:-1}
shift $(( $# < 4 ? $# : 4 ))
SERVERS=${*:-kosha rustfs minio}
BENCH_DIR=${BENCH_DIR:-/tmp/kosha-compare}
S3BENCH=${S3BENCH:-$(dirname "$0")/../target/release/examples/s3bench}
export AWS_ACCESS_KEY_ID=benchadmin AWS_SECRET_ACCESS_KEY=bench-secret-key-123
mkdir -p "$BENCH_DIR"
OUT=$BENCH_DIR/results.txt

start() {
  local name=$1 dir=$2
  case $name in
    kosha)
      exec env KOSHA_ACCESS_KEY=$AWS_ACCESS_KEY_ID KOSHA_SECRET_KEY=$AWS_SECRET_ACCESS_KEY \
        "$KOSHA" --data-dir "$dir" --listen 127.0.0.1:9000 ;;
    rustfs)
      exec env RUSTFS_ACCESS_KEY=$AWS_ACCESS_KEY_ID RUSTFS_SECRET_KEY=$AWS_SECRET_ACCESS_KEY \
        "$RUSTFS" server "$dir" --address 127.0.0.1:9000 ;;
    minio)
      exec env MINIO_ROOT_USER=$AWS_ACCESS_KEY_ID MINIO_ROOT_PASSWORD=$AWS_SECRET_ACCESS_KEY \
        "$MINIO" server "$dir" --address 127.0.0.1:9000 --console-address 127.0.0.1:9001 ;;
  esac
}

bench() { "$S3BENCH" --endpoint http://127.0.0.1:9000 "$@"; }

for round in $(seq "$ROUNDS"); do
  for name in $SERVERS; do
    dir=$BENCH_DIR/data-$name
    rm -rf "$dir" && mkdir -p "$dir"
    start "$name" "$dir" > "$BENCH_DIR/$name.log" 2>&1 &
    pid=$!
    for _ in $(seq 120); do
      code=$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:9000/ || true)
      [ "$code" != 000 ] && break
      sleep 0.5
    done
    sleep 3
    {
      echo "== $name (round $round)"
      bench --bucket small --size 4KiB --count 20000 --concurrency 32
      bench --bucket medium --size 64KiB --count 5000 --concurrency 32
      bench --bucket large --size 1MiB --count 1000 --concurrency 16 --copy
      bench --bucket huge --size 32MiB --count 40 --concurrency 4
      sync
      before=$(du -sb "$dir" | cut -f1)
      bench --bucket dedup --size 1MiB --count 1000 --concurrency 16 --duplicate
      sync
      after=$(du -sb "$dir" | cut -f1)
      echo "disk: 1000 identical 1 MiB objects added $(( (after - before) / 1048576 )) MiB; data dir total $(( after / 1048576 )) MiB"
      echo "peak RSS: $(( $(awk '/VmHWM/ {print $2}' /proc/$pid/status) / 1024 )) MiB"
    } 2>&1 | tee -a "$OUT"
    kill "$pid"; wait "$pid" 2>/dev/null || true
    rm -rf "$dir"
  done
done
