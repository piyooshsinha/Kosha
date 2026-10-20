# Security

Users and policies, HTTPS, cluster mutual TLS and encryption at rest.

## Security

### Users and policies

The keys from `KOSHA_ACCESS_KEY` / `KOSHA_SECRET_KEY` are **root** keys: they
bypass policies. Everyone else gets a **user** with its own access key, whose
permissions come from IAM-style policies. Users are stored in the replicated
metadata, so every node authenticates them.

```sh
alias kadmin='curl -s --aws-sigv4 "aws:amz:us-east-1:s3" --user admin:change-me-please'

# create a user with an identity policy; the response carries its keys (shown once)
kadmin -X POST http://localhost:9000/_kosha/admin/users -H 'content-type: application/json' -d '{
  "name": "analytics",
  "policy": {"Version": "2012-10-17", "Statement": [
    {"Effect": "Allow", "Action": ["s3:GetObject", "s3:ListBucket"],
     "Resource": ["arn:aws:s3:::reports", "arn:aws:s3:::reports/*"]}]}}'
# {"access_key":"KS4VUIBFMF75WK64R7EL","secret_key":"…","name":"analytics",…}

kadmin http://localhost:9000/_kosha/admin/users                          # list (no secrets)
kadmin -X PUT http://localhost:9000/_kosha/admin/users/KS4V… -d '{"enabled": false}'
kadmin -X PUT http://localhost:9000/_kosha/admin/users/KS4V… -d '{"rotate_secret": true}'
kadmin -X DELETE http://localhost:9000/_kosha/admin/users/KS4V…
```

**Bucket policies** use the same language plus `Principal`. `"Principal": "*"`
grants anonymous (unsigned) access, which is how you publish a static site or
a download area:

```sh
aws --endpoint-url http://localhost:9000 s3api put-bucket-policy --bucket site --policy '{
  "Version": "2012-10-17",
  "Statement": [
    {"Effect": "Allow", "Principal": "*", "Action": "s3:GetObject", "Resource": "arn:aws:s3:::site/*"},
    {"Effect": "Allow", "Principal": {"AWS": ["arn:aws:iam:::user/analytics"]},
     "Action": "s3:PutObject", "Resource": "arn:aws:s3:::site/uploads/*",
     "Condition": {"IpAddress": {"aws:SourceIp": "10.0.0.0/8"}}}]}'
curl http://localhost:9000/site/index.html        # no signature needed now
```

What the policy language supports:

| | |
|---|---|
| Statements | `Effect`, `Action`/`NotAction`, `Resource`/`NotResource` with `*` and `?`, `Principal` (`"*"`, user names, `arn:…:user/NAME`, access keys) |
| Variables | `${aws:username}`, `${aws:userid}` and any condition key, e.g. `arn:aws:s3:::home/${aws:username}/*` |
| Conditions | `String(Not)Equals(IgnoreCase)`, `String(Not)Like`, `Numeric*`, `(Not)IpAddress`, `Bool`, `Null`, `…IfExists`, `ForAnyValue:`/`ForAllValues:` |
| Condition keys | `aws:SourceIp`, `aws:SecureTransport`, `aws:username`, `aws:userid`, `aws:PrincipalType`, `aws:UserAgent`, `aws:Referer`, `aws:CurrentTime`, `aws:EpochTime`, `s3:prefix`, `s3:delimiter`, `s3:max-keys`, `s3:VersionId`, `s3:x-amz-server-side-encryption`, `s3:signatureversion`, `s3:authType`, … |
| Evaluation | an explicit `Deny` anywhere wins; otherwise an `Allow` in the identity **or** the bucket policy grants access; otherwise `AccessDenied` |

Every S3 operation maps to its IAM action (`s3:GetObject`,
`s3:ListBucket`, `s3:PutBucketPolicy`, …). `DeleteObjects` is checked key by
key, copies also need `s3:GetObject` on the source, and without
`s3:ListBucket` a missing key reads as `AccessDenied`, so callers cannot probe
which keys exist. Invalid policies are rejected with `MalformedPolicy` when
you set them.

### HTTPS

```sh
kosha --tls-cert server.pem --tls-key server.key ...   # serves S3 and the console over HTTPS
aws --endpoint-url https://s3.example.internal:9000 s3 ls
```

`aws:SecureTransport` is `true` for requests that arrive over TLS.

### Cluster mutual TLS

Give every node a certificate from your own CA. The certificate's SANs must
cover the address peers dial (`--cluster-advertise`, an IP or a DNS name):

```sh
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 3650 \
  -keyout ca.key -out ca.pem -subj "/CN=Kosha CA"
openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
  -keyout node1.key -out node1.csr -subj "/CN=node1"
openssl x509 -req -in node1.csr -CA ca.pem -CAkey ca.key -CAcreateserial -days 825 -out node1.pem \
  -extfile <(printf "subjectAltName=IP:10.0.0.1\nextendedKeyUsage=serverAuth,clientAuth")

kosha --node-id 1 ... --cluster-tls-cert node1.pem --cluster-tls-key node1.key --cluster-tls-ca ca.pem
```

All internal traffic (Raft, chunk replication, repair) then runs over TLS,
and each side verifies the other: a peer without a certificate from that CA
is refused before it can send anything, even if it knows the cluster
secret.

### Encryption at rest

```sh
openssl rand -hex 32 > /etc/kosha/kosha.key && chmod 600 /etc/kosha/kosha.key
kosha --encryption-key-file /etc/kosha/kosha.key ...
```

Object data is encrypted on disk **without giving up dedup**:

- **Chunk addresses** are a *keyed* BLAKE3 hash. Identical content still maps
  to one chunk, but someone with the disks cannot check whether a known file
  is stored by hashing it.
- **Chunk contents** are compressed, then encrypted with AES-256-GCM-SIV. The
  nonce is derived from the chunk address, so encryption is deterministic:
  replicas stay byte-identical and repair can compare them. GCM-SIV is
  designed to stay secure when the same plaintext meets the same nonce.
- **Every read is authenticated**: a modified chunk fails decryption.
- **Small objects are encrypted too.** They are stored as chunks instead of
  inline in the metadata.

Other things to know:

- **Choose encryption when you create a data directory.** Kosha records the
  data directory's encryption identity. It refuses to start without a key
  that can open it, without any key, or with a key on a directory that
  already holds unencrypted data.
- **Every cluster node needs the same key.** A node with another key, or none,
  is refused when it joins.
- **Back up the key.** Without it the data cannot be read (see rotation
  below).
- `x-amz-server-side-encryption: AES256` is returned on object responses, and
  GetBucketEncryption reports SSE-S3.
- **Refused, not ignored:** on a server without a key, a request for SSE is
  refused rather than stored in plaintext. SSE-KMS and SSE-C (customer keys)
  return `NotImplemented`.
- **Not encrypted:** metadata (bucket and key names, sizes, tags, user
  metadata, users' secret keys) stays readable in `meta.redb` and the Raft
  log. Treat the data directory as sensitive.
- **Cost:** see [Performance](BENCHMARKS.md#cost-of-encryption-at-rest).

**Rotating keys.** Give every node the new key as primary and the current one
as an old key, then restart them one at a time:

```sh
openssl rand -hex 32 > /etc/kosha/kosha-2027.key
kosha --encryption-key-file /etc/kosha/kosha-2027.key \
      --encryption-old-key-file /etc/kosha/kosha.key ...
curl -s --aws-sigv4 ... http://localhost:9000/_kosha/stats   # "rotation": {"chunks_pending": 0, ...}
```

- **New data** uses the new key immediately.
- **Existing data** is re-encrypted in the background: every node rewrites
  its chunk files and shards, and the leader re-uploads cold-tier copies.
  Every encrypted chunk records which key sealed it.
- **Progress** is in `/_kosha/stats` (`rotation`) and Prometheus
  (`kosha_rotation_pending`).
- **When every node reports 0 pending**, drop `--encryption-old-key-file`.
  The old key is then no longer needed for anything.
- **Chunk ids stay valid.** The key that hashes chunk ids never changes:
  it's created with the data directory and kept in `chunks/ID_KEY`, sealed
  under the current primary key.
- **New nodes joining a rotated cluster** need the cluster's original key
  once: `--encryption-identity-key-file`.
- **Legacy shards:** erasure-coded shards written by versions before this
  one in an encrypted cluster use the old shard format. They are counted as
  `legacy_shards`, can only be read with their original key, and are not
  re-encrypted. Rewrite those objects (for example with a server-side copy)
  to move them to the new format.
