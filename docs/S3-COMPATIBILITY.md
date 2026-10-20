# S3 compatibility

Which parts of the S3 API Kosha implements.

## S3 compatibility

Tested against the **AWS SDK for Rust** (in CI), **boto3**, the **AWS CLI**,
**curl `--aws-sigv4`**, and the console's own SigV4 signer.

| Area | Supported |
|---|---|
| Auth | SigV4 headers, SigV4 presigned URLs, streaming `aws-chunked` with per-chunk signature verification, unsigned-payload trailers, SigV2 headers and presigned URLs, anonymous requests (when a bucket policy allows them) |
| Access control | users with identity policies, Get/Put/DeleteBucketPolicy, GetBucketPolicyStatus, IAM conditions (see [Security](SECURITY.md#users-and-policies)) |
| Encryption | SSE-S3 semantics server-wide (`x-amz-server-side-encryption: AES256`, Get/Put/DeleteBucketEncryption) |
| Integrity | `x-amz-content-sha256` verified, `Content-MD5`, `x-amz-checksum-crc32` (header or trailer) |
| Buckets | Create, Delete, Head, List, GetBucketLocation, GetBucketAcl; "not configured" responses for website/replication/... |
| Objects | Put, Get (single range, conditional, `response-*` overrides), Head, Delete, DeleteObjects, CopyObject (COPY/REPLACE metadata and tagging directives, copy-source conditions), user metadata and standard headers |
| Tagging | `x-amz-tagging` on Put/Copy/CreateMultipartUpload, Get/Put/DeleteObjectTagging, `x-amz-tagging-count`, Get/Put/DeleteBucketTagging |
| CORS | Get/Put/DeleteBucketCors, unauthenticated `OPTIONS` preflight, CORS headers on matching responses (origin wildcards, credentials, exposed headers, max-age) |
| Versioning | Get/PutBucketVersioning (Enabled, Suspended), `versionId` on Get/Head/Delete/Copy source/tagging, delete markers, null versions, ListObjectVersions (prefix, delimiter, key/version markers), versioned DeleteObjects |
| Object Lock | buckets created with `x-amz-bucket-object-lock-enabled`, Get/PutObjectLockConfiguration (default retention), Get/PutObjectRetention (GOVERNANCE with `x-amz-bypass-governance-retention`, COMPLIANCE), Get/PutObjectLegalHold, `x-amz-object-lock-*` headers on Put/Copy/CreateMultipartUpload |
| Lifecycle | Get/Put/DeleteBucketLifecycleConfiguration: expiration (days or date), transition to a cold class (days or date), noncurrent version transition and expiration, expired delete marker cleanup, AbortIncompleteMultipartUpload; filters by prefix and tags |
| Storage classes | `x-amz-storage-class` on Put/Copy/CreateMultipartUpload, reported on Get/Head and in listings, RestoreObject (succeeds at once: cold data reads directly); cold classes need a [cold tier](GUIDE.md#cold-tier) |
| ACLs | Get/PutBucketAcl and Get/PutObjectAcl with the owner-only (`private`) ACL; anything granting more returns `NotImplemented` (use policies) |
| Intelligent-Tiering | Get/Put/Delete/ListBucketIntelligentTieringConfiguration (prefix and tag filters, `ARCHIVE_ACCESS` / `DEEP_ARCHIVE_ACCESS` days); objects report `INTELLIGENT_TIERING` while cold |
| Notifications | Get/PutBucketNotificationConfiguration (Queue/Topic/CloudFunction configurations, prefix/suffix filters) delivered to webhooks |
| Listing | ListObjectsV2 (prefix, delimiter, continuation, start-after, `encoding-type=url`), ListObjects v1 |
| Multipart | Create, UploadPart, UploadPartCopy (with range), Complete, Abort, ListParts, ListMultipartUploads (paginated) |
| Writes | Conditional `If-None-Match: *` / `If-Match` on Put, Copy and Complete |

Not implemented: ACLs that grant access to others (use policies), SSE-KMS
and SSE-C, and the AWS IAM API itself (users are managed through
`/_kosha/admin/users` and the console). Unsupported operations return `501 NotImplemented`.
