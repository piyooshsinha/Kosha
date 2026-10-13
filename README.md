# Kosha

**Kosha** (कोश, "treasury") is an S3-compatible object store written in Rust.

It speaks the S3 API, so existing tools and SDKs work unchanged. Underneath, it is
not a clone of S3's design. Kosha stores data the way modern backup and
content-addressed systems do:

| | Classic object store | Kosha |
|---|---|---|
| Unit of storage | whole object / fixed blocks | **content-defined chunks** (FastCDC) |
| Identity | key → file | key → manifest of **BLAKE3**-addressed chunks |
| Duplicate data | stored again | **stored once, globally** (dedup across objects, buckets, multipart parts) |
| Server-side copy | re-reads and re-writes bytes | **metadata only** (a 256 MiB copy takes ~5 ms) |
| Multipart complete | concatenates part files | **concatenates manifests**, no data moved |
| Compression | none or always | **adaptive** zstd: probes each chunk, skips incompressible data |
| Integrity | optional checksums | **every chunk verified on every read** (its address is its hash) |
| Small objects | one file each | **inlined** into metadata (≤ 8 KiB) |
| Metadata | files / ad-hoc | **ACID** transactions (redb): refcounts, stats and manifests commit atomically |
| Concurrency control | last writer wins | adds **conditional writes** (`If-None-Match: *`, `If-Match`) evaluated atomically |

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the design and the roadmap.

## Quick start

```sh
cargo build --release
KOSHA_ACCESS_KEY=admin KOSHA_SECRET_KEY=change-me-please \
  ./target/release/kosha --data-dir ./data --listen 0.0.0.0:9000
```

Use any S3 client:

```sh
export AWS_ACCESS_KEY_ID=admin AWS_SECRET_ACCESS_KEY=change-me-please AWS_DEFAULT_REGION=us-east-1
aws --endpoint-url http://localhost:9000 s3 mb s3://photos
aws --endpoint-url http://localhost:9000 s3 cp ./holiday.jpg s3://photos/2026/holiday.jpg
aws --endpoint-url http://localhost:9000 s3 ls s3://photos --recursive
```

```python
import boto3
s3 = boto3.client("s3", endpoint_url="http://localhost:9000",
                  aws_access_key_id="admin", aws_secret_access_key="change-me-please")
s3.put_object(Bucket="photos", Key="hello.txt", Body=b"hi")
```

Storage efficiency is reported live:

```sh
curl -s --aws-sigv4 "aws:amz:us-east-1:s3" --user admin:change-me-please \
  http://localhost:9000/_kosha/stats
# {"objects":7,"logical_bytes":832569357,"unique_bytes":292552704,"physical_bytes":292552944,
#  "dedup_ratio":2.846,"compression_ratio":1.000,"overall_ratio":2.846,...}
```

`GET /_kosha/health` is an unauthenticated liveness probe.

### Configuration

| Flag | Env | Default | |
|---|---|---|---|
| `--listen` | `KOSHA_LISTEN` | `127.0.0.1:9000` | bind address |
| `--data-dir` | `KOSHA_DATA_DIR` | `./data` | metadata + chunks |
| `--access-key` / `--secret-key` | `KOSHA_ACCESS_KEY` / `KOSHA_SECRET_KEY` | none | required unless `--anonymous` |
| `--anonymous` | `KOSHA_ANONYMOUS` | off | no auth, for local development only |
| `--region` | `KOSHA_REGION` | `us-east-1` | reported region (signatures for any region are accepted) |
| `--domain` | `KOSHA_DOMAIN` | none | enables virtual-hosted style, `bucket.<domain>` |
| `--compression-level` | `KOSHA_COMPRESSION_LEVEL` | `3` | zstd level, `0` disables |
| `--no-fsync` | `KOSHA_NO_FSYNC` | off | faster, but recent writes may be lost on power failure |

Logging uses `RUST_LOG`, for example `RUST_LOG=debug`.

## S3 compatibility

Tested against the **AWS SDK for Rust**, **boto3**, the **AWS CLI**, and **curl `--aws-sigv4`**.

| Area | Supported |
|---|---|
| Auth | SigV4 headers, SigV4 presigned URLs, streaming `aws-chunked` with per-chunk signature verification, unsigned-payload trailers, SigV2 presigned URLs (boto3's default) |
| Integrity | `x-amz-content-sha256` verified, `Content-MD5`, `x-amz-checksum-crc32` (header or trailer) |
| Buckets | Create, Delete, Head, List, GetBucketLocation, GetBucketVersioning (unversioned), GetBucketAcl; "not configured" responses for policy/CORS/lifecycle/tagging/encryption/... |
| Objects | Put, Get (single range, conditional, `response-*` overrides), Head, Delete, DeleteObjects, CopyObject (COPY/REPLACE metadata, copy-source conditions), user metadata and standard headers |
| Listing | ListObjectsV2 (prefix, delimiter, continuation, start-after, `encoding-type=url`), ListObjects v1 |
| Multipart | Create, UploadPart, UploadPartCopy (with range), Complete, Abort, ListParts, ListMultipartUploads |
| Writes | Conditional `If-None-Match: *` / `If-Match` on Put, Copy and Complete |

Not implemented yet: versioning, object lock, bucket policies/IAM, lifecycle,
SSE, tagging writes, SigV2 header auth. Unsupported operations return `501 NotImplemented`.

## Layout

```
crates/
  kosha-engine   storage engine: chunking, chunk store, metadata, GC (no HTTP)
  kosha-s3       S3 protocol: routing, SigV4/SigV2, aws-chunked, XML
  kosha-server   the `kosha` binary
```

## Development

```sh
cargo test --workspace     # unit tests, engine tests, AWS SDK compatibility tests
cargo clippy --workspace --all-targets
```

## License

Apache-2.0
