//! Compatibility tests driven by the official AWS SDK for Rust.

use std::time::Duration;

use aws_sdk_s3::Client;
use aws_sdk_s3::config::{BehaviorVersion, Credentials as AwsCreds, Region};
use aws_sdk_s3::presigning::PresigningConfig;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart, Delete, ObjectIdentifier};
use kosha_engine::{Engine, EngineConfig, SyncMode};
use kosha_s3::{Credentials, S3Config};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const AK: &str = "AKIAKOSHATEST";
const SK: &str = "kosha-secret-key-for-tests";

struct Server {
    addr: std::net::SocketAddr,
    _dir: tempfile::TempDir,
}

async fn start() -> Server {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = EngineConfig::new(dir.path());
    cfg.sync = SyncMode::None;
    let engine = Engine::open(cfg).unwrap();
    let app = kosha_s3::router(
        engine,
        S3Config {
            credentials: Credentials::new().with_key(AK, SK),
            ..Default::default()
        },
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    Server { addr, _dir: dir }
}

fn client(s: &Server, secret: &str) -> Client {
    let conf = aws_sdk_s3::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new("us-east-1"))
        .endpoint_url(format!("http://{}", s.addr))
        .credentials_provider(AwsCreds::new(AK, secret, None, None, "test"))
        .force_path_style(true)
        .build();
    Client::from_conf(conf)
}

fn random(n: usize, seed: u64) -> Vec<u8> {
    let mut x = seed | 1;
    (0..n)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x as u8
        })
        .collect()
}

async fn body(out: aws_sdk_s3::operation::get_object::GetObjectOutput) -> Vec<u8> {
    out.body.collect().await.unwrap().into_bytes().to_vec()
}

#[tokio::test]
async fn object_lifecycle() {
    let s = start().await;
    let c = client(&s, SK);

    c.create_bucket().bucket("media").send().await.unwrap();
    let buckets = c.list_buckets().send().await.unwrap();
    assert_eq!(buckets.buckets()[0].name(), Some("media"));
    c.head_bucket().bucket("media").send().await.unwrap();

    // Multi-chunk object with metadata.
    let data = random(9 * 1024 * 1024 + 123, 1);
    c.put_object()
        .bucket("media")
        .key("videos/clip one.bin")
        .content_type("video/mp4")
        .metadata("owner", "kosha")
        .body(ByteStream::from(data.clone()))
        .send()
        .await
        .unwrap();

    let head = c
        .head_object()
        .bucket("media")
        .key("videos/clip one.bin")
        .send()
        .await
        .unwrap();
    assert_eq!(head.content_length(), Some(data.len() as i64));
    assert_eq!(head.content_type(), Some("video/mp4"));
    assert_eq!(
        head.metadata().unwrap().get("owner").map(String::as_str),
        Some("kosha")
    );

    let got = c
        .get_object()
        .bucket("media")
        .key("videos/clip one.bin")
        .send()
        .await
        .unwrap();
    assert_eq!(body(got).await, data);

    let ranged = c
        .get_object()
        .bucket("media")
        .key("videos/clip one.bin")
        .range("bytes=1000000-5000000")
        .send()
        .await
        .unwrap();
    assert_eq!(
        ranged.content_range(),
        Some(format!("bytes 1000000-5000000/{}", data.len()).as_str())
    );
    assert_eq!(body(ranged).await, &data[1_000_000..=5_000_000]);

    // Small objects, odd keys.
    for k in [
        "a.txt",
        "docs/readme.md",
        "docs/guide/intro.md",
        "unicode/नमस्ते.txt",
        "spaces and+plus.txt",
    ] {
        c.put_object()
            .bucket("media")
            .key(k)
            .body(ByteStream::from(k.as_bytes().to_vec()))
            .send()
            .await
            .unwrap();
        let got = c.get_object().bucket("media").key(k).send().await.unwrap();
        assert_eq!(body(got).await, k.as_bytes(), "{k}");
    }

    // Listing with delimiter and pagination.
    let top = c
        .list_objects_v2()
        .bucket("media")
        .delimiter("/")
        .send()
        .await
        .unwrap();
    let keys: Vec<_> = top.contents().iter().filter_map(|o| o.key()).collect();
    let prefixes: Vec<_> = top
        .common_prefixes()
        .iter()
        .filter_map(|p| p.prefix())
        .collect();
    assert_eq!(keys, ["a.txt", "spaces and+plus.txt"]);
    assert_eq!(prefixes, ["docs/", "unicode/", "videos/"]);

    let mut all = Vec::new();
    let mut pages = c
        .list_objects_v2()
        .bucket("media")
        .max_keys(2)
        .into_paginator()
        .send();
    while let Some(page) = pages.next().await {
        all.extend(
            page.unwrap()
                .contents()
                .iter()
                .filter_map(|o| o.key().map(str::to_string)),
        );
    }
    assert_eq!(all.len(), 6);

    let v1 = c
        .list_objects()
        .bucket("media")
        .prefix("docs/")
        .send()
        .await
        .unwrap();
    assert_eq!(v1.contents().len(), 2);

    // Zero-copy server-side copy.
    c.copy_object()
        .bucket("media")
        .key("backup/clip.bin")
        .copy_source("media/videos/clip%20one.bin")
        .send()
        .await
        .unwrap();
    let copied = c
        .get_object()
        .bucket("media")
        .key("backup/clip.bin")
        .send()
        .await
        .unwrap();
    assert_eq!(copied.content_type(), Some("video/mp4"));
    assert_eq!(body(copied).await, data);

    // Batch delete.
    let ids: Vec<_> = ["a.txt", "docs/readme.md", "missing"]
        .iter()
        .map(|k| ObjectIdentifier::builder().key(*k).build().unwrap())
        .collect();
    let del = c
        .delete_objects()
        .bucket("media")
        .delete(Delete::builder().set_objects(Some(ids)).build().unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(del.deleted().len(), 3);

    let err = c
        .get_object()
        .bucket("media")
        .key("a.txt")
        .send()
        .await
        .unwrap_err();
    assert!(err.into_service_error().is_no_such_key());

    let err = c.delete_bucket().bucket("media").send().await.unwrap_err();
    assert_eq!(
        err.into_service_error().meta().code(),
        Some("BucketNotEmpty")
    );
}

#[tokio::test]
async fn multipart_upload() {
    let s = start().await;
    let c = client(&s, SK);
    c.create_bucket().bucket("uploads").send().await.unwrap();

    let up = c
        .create_multipart_upload()
        .bucket("uploads")
        .key("big.bin")
        .content_type("application/x-big")
        .send()
        .await
        .unwrap();
    let id = up.upload_id().unwrap();

    let p1 = random(5 * 1024 * 1024, 11);
    let p2 = random(1024 * 1024 + 7, 12);
    let mut parts = Vec::new();
    for (n, d) in [(1, &p1), (2, &p2)] {
        let r = c
            .upload_part()
            .bucket("uploads")
            .key("big.bin")
            .upload_id(id)
            .part_number(n)
            .body(ByteStream::from(d.clone()))
            .send()
            .await
            .unwrap();
        parts.push(
            CompletedPart::builder()
                .part_number(n)
                .e_tag(r.e_tag().unwrap())
                .build(),
        );
    }

    let listed = c
        .list_parts()
        .bucket("uploads")
        .key("big.bin")
        .upload_id(id)
        .send()
        .await
        .unwrap();
    assert_eq!(listed.parts().len(), 2);
    let uploads = c
        .list_multipart_uploads()
        .bucket("uploads")
        .send()
        .await
        .unwrap();
    assert_eq!(uploads.uploads().len(), 1);

    let done = c
        .complete_multipart_upload()
        .bucket("uploads")
        .key("big.bin")
        .upload_id(id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .set_parts(Some(parts))
                .build(),
        )
        .send()
        .await
        .unwrap();
    assert!(done.e_tag().unwrap().ends_with("-2\""));

    let got = c
        .get_object()
        .bucket("uploads")
        .key("big.bin")
        .send()
        .await
        .unwrap();
    assert_eq!(got.content_type(), Some("application/x-big"));
    assert_eq!(body(got).await, [p1, p2].concat());

    // Paginated ListMultipartUploads across several keys and uploads per key.
    let mut created = Vec::new();
    for k in ["p/a", "p/b", "p/b", "p/c", "p/c", "p/c"] {
        let up = c
            .create_multipart_upload()
            .bucket("uploads")
            .key(k)
            .send()
            .await
            .unwrap();
        created.push((k.to_string(), up.upload_id().unwrap().to_string()));
    }
    let mut seen = Vec::new();
    let (mut km, mut um): (Option<String>, Option<String>) = (None, None);
    loop {
        let page = c
            .list_multipart_uploads()
            .bucket("uploads")
            .prefix("p/")
            .max_uploads(2)
            .set_key_marker(km.clone())
            .set_upload_id_marker(um.clone())
            .send()
            .await
            .unwrap();
        assert!(page.uploads().len() <= 2);
        seen.extend(page.uploads().iter().map(|u| {
            (
                u.key().unwrap().to_string(),
                u.upload_id().unwrap().to_string(),
            )
        }));
        if !page.is_truncated().unwrap_or(false) {
            break;
        }
        km = page.next_key_marker().map(str::to_string);
        um = page.next_upload_id_marker().map(str::to_string);
    }
    created.sort();
    seen.sort();
    assert_eq!(seen, created, "every upload listed exactly once");
    for (k, id) in &created {
        c.abort_multipart_upload()
            .bucket("uploads")
            .key(k)
            .upload_id(id)
            .send()
            .await
            .unwrap();
    }

    // Abort path.
    let up = c
        .create_multipart_upload()
        .bucket("uploads")
        .key("gone")
        .send()
        .await
        .unwrap();
    c.abort_multipart_upload()
        .bucket("uploads")
        .key("gone")
        .upload_id(up.upload_id().unwrap())
        .send()
        .await
        .unwrap();
    let uploads = c
        .list_multipart_uploads()
        .bucket("uploads")
        .send()
        .await
        .unwrap();
    assert!(uploads.uploads().is_empty());
}

#[tokio::test]
async fn auth_and_conditions() {
    let s = start().await;
    let c = client(&s, SK);
    c.create_bucket().bucket("secure").send().await.unwrap();

    let bad = client(&s, "wrong-secret");
    let err = bad.list_buckets().send().await.unwrap_err();
    assert_eq!(
        err.into_service_error().meta().code(),
        Some("SignatureDoesNotMatch")
    );

    // Conditional create: the second writer loses.
    c.put_object()
        .bucket("secure")
        .key("lock")
        .if_none_match("*")
        .body(ByteStream::from_static(b"first"))
        .send()
        .await
        .unwrap();
    let err = c
        .put_object()
        .bucket("secure")
        .key("lock")
        .if_none_match("*")
        .body(ByteStream::from_static(b"second"))
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        err.into_service_error().meta().code(),
        Some("PreconditionFailed")
    );

    // Presigned GET fetched with a bare HTTP client.
    let req = c
        .get_object()
        .bucket("secure")
        .key("lock")
        .presigned(PresigningConfig::expires_in(Duration::from_secs(300)).unwrap())
        .await
        .unwrap();
    let uri: axum::http::Uri = req.uri().parse().unwrap();
    let mut conn = tokio::net::TcpStream::connect(s.addr).await.unwrap();
    let path = uri.path_and_query().unwrap().as_str();
    conn.write_all(
        format!(
            "GET {path} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n",
            s.addr
        )
        .as_bytes(),
    )
    .await
    .unwrap();
    let mut resp = String::new();
    conn.read_to_string(&mut resp).await.unwrap();
    assert!(resp.starts_with("HTTP/1.1 200"), "{resp}");
    assert!(resp.ends_with("first"), "{resp}");

    // Tampering with a presigned URL breaks it.
    let mut conn = tokio::net::TcpStream::connect(s.addr).await.unwrap();
    let tampered = path.replace("/secure/lock", "/secure/other");
    conn.write_all(
        format!(
            "GET {tampered} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n",
            s.addr
        )
        .as_bytes(),
    )
    .await
    .unwrap();
    let mut resp = String::new();
    conn.read_to_string(&mut resp).await.unwrap();
    assert!(resp.starts_with("HTTP/1.1 403"), "{resp}");
}

async fn raw_http(addr: std::net::SocketAddr, request: String) -> String {
    let mut conn = tokio::net::TcpStream::connect(addr).await.unwrap();
    conn.write_all(request.as_bytes()).await.unwrap();
    let mut resp = String::new();
    conn.read_to_string(&mut resp).await.unwrap();
    resp
}

#[tokio::test]
async fn tagging_and_cors() {
    use aws_sdk_s3::types::{CorsConfiguration, CorsRule, Tag, Tagging, TaggingDirective};
    let s = start().await;
    let c = client(&s, SK);
    c.create_bucket().bucket("tagged").send().await.unwrap();

    // Tags on upload, tag count on GET, replace, delete.
    c.put_object()
        .bucket("tagged")
        .key("doc")
        .tagging("project=kosha&team=storage%20core")
        .body(ByteStream::from_static(b"hello"))
        .send()
        .await
        .unwrap();
    let got = c
        .get_object()
        .bucket("tagged")
        .key("doc")
        .send()
        .await
        .unwrap();
    assert_eq!(got.tag_count(), Some(2));
    let tags = c
        .get_object_tagging()
        .bucket("tagged")
        .key("doc")
        .send()
        .await
        .unwrap();
    let team = tags.tag_set().iter().find(|t| t.key() == "team").unwrap();
    assert_eq!(team.value(), "storage core");

    let one = Tagging::builder()
        .tag_set(Tag::builder().key("stage").value("final").build().unwrap())
        .build()
        .unwrap();
    c.put_object_tagging()
        .bucket("tagged")
        .key("doc")
        .tagging(one)
        .send()
        .await
        .unwrap();
    let tags = c
        .get_object_tagging()
        .bucket("tagged")
        .key("doc")
        .send()
        .await
        .unwrap();
    assert_eq!(tags.tag_set().len(), 1);

    // Copy keeps tags by default, or replaces them.
    c.copy_object()
        .bucket("tagged")
        .key("kept")
        .copy_source("tagged/doc")
        .send()
        .await
        .unwrap();
    assert_eq!(
        c.get_object_tagging()
            .bucket("tagged")
            .key("kept")
            .send()
            .await
            .unwrap()
            .tag_set()[0]
            .key(),
        "stage"
    );
    c.copy_object()
        .bucket("tagged")
        .key("replaced")
        .copy_source("tagged/doc")
        .tagging_directive(TaggingDirective::Replace)
        .tagging("new=1")
        .send()
        .await
        .unwrap();
    assert_eq!(
        c.get_object_tagging()
            .bucket("tagged")
            .key("replaced")
            .send()
            .await
            .unwrap()
            .tag_set()[0]
            .key(),
        "new"
    );

    c.delete_object_tagging()
        .bucket("tagged")
        .key("doc")
        .send()
        .await
        .unwrap();
    assert!(
        c.get_object_tagging()
            .bucket("tagged")
            .key("doc")
            .send()
            .await
            .unwrap()
            .tag_set()
            .is_empty()
    );

    // Bucket tagging.
    let bt = Tagging::builder()
        .tag_set(
            Tag::builder()
                .key("cost-center")
                .value("42")
                .build()
                .unwrap(),
        )
        .build()
        .unwrap();
    c.put_bucket_tagging()
        .bucket("tagged")
        .tagging(bt)
        .send()
        .await
        .unwrap();
    assert_eq!(
        c.get_bucket_tagging()
            .bucket("tagged")
            .send()
            .await
            .unwrap()
            .tag_set()[0]
            .value(),
        "42"
    );
    c.delete_bucket_tagging()
        .bucket("tagged")
        .send()
        .await
        .unwrap();
    let err = c
        .get_bucket_tagging()
        .bucket("tagged")
        .send()
        .await
        .unwrap_err();
    assert_eq!(err.into_service_error().meta().code(), Some("NoSuchTagSet"));

    // CORS configuration round trip.
    let rule = CorsRule::builder()
        .allowed_origins("https://*.example.com")
        .allowed_methods("GET")
        .allowed_methods("PUT")
        .allowed_headers("*")
        .expose_headers("ETag")
        .max_age_seconds(600)
        .build()
        .unwrap();
    c.put_bucket_cors()
        .bucket("tagged")
        .cors_configuration(
            CorsConfiguration::builder()
                .cors_rules(rule)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    let cors = c.get_bucket_cors().bucket("tagged").send().await.unwrap();
    assert_eq!(cors.cors_rules()[0].allowed_methods(), ["GET", "PUT"]);

    // Preflight from an allowed origin, then from a disallowed one.
    let ok = raw_http(s.addr, format!(
        "OPTIONS /tagged/doc HTTP/1.1\r\nHost: {}\r\nOrigin: https://app.example.com\r\nAccess-Control-Request-Method: PUT\r\nAccess-Control-Request-Headers: content-type\r\nConnection: close\r\n\r\n", s.addr)).await;
    assert!(ok.starts_with("HTTP/1.1 200"), "{ok}");
    assert!(
        ok.to_lowercase()
            .contains("access-control-allow-origin: https://app.example.com"),
        "{ok}"
    );
    assert!(
        ok.to_lowercase().contains("access-control-max-age: 600"),
        "{ok}"
    );
    let denied = raw_http(s.addr, format!(
        "OPTIONS /tagged/doc HTTP/1.1\r\nHost: {}\r\nOrigin: https://evil.com\r\nAccess-Control-Request-Method: PUT\r\nConnection: close\r\n\r\n", s.addr)).await;
    assert!(denied.starts_with("HTTP/1.1 403"), "{denied}");

    // A real cross-origin GET (presigned) gets CORS headers.
    let req = c
        .get_object()
        .bucket("tagged")
        .key("doc")
        .presigned(PresigningConfig::expires_in(Duration::from_secs(60)).unwrap())
        .await
        .unwrap();
    let uri: axum::http::Uri = req.uri().parse().unwrap();
    let resp = raw_http(s.addr, format!(
        "GET {} HTTP/1.1\r\nHost: {}\r\nOrigin: https://app.example.com\r\nConnection: close\r\n\r\n",
        uri.path_and_query().unwrap(), s.addr)).await;
    let lower = resp.to_lowercase();
    assert!(resp.starts_with("HTTP/1.1 200"), "{resp}");
    assert!(
        lower.contains("access-control-allow-origin: https://app.example.com"),
        "{resp}"
    );
    assert!(
        lower.contains("access-control-expose-headers: etag"),
        "{resp}"
    );

    c.delete_bucket_cors()
        .bucket("tagged")
        .send()
        .await
        .unwrap();
    let err = c
        .get_bucket_cors()
        .bucket("tagged")
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        err.into_service_error().meta().code(),
        Some("NoSuchCORSConfiguration")
    );
}

#[tokio::test]
async fn versioning_object_lock_and_lifecycle() {
    use aws_sdk_s3::types::{
        BucketLifecycleConfiguration, BucketVersioningStatus, DefaultRetention, ExpirationStatus,
        LifecycleExpiration, LifecycleRule, LifecycleRuleFilter, NoncurrentVersionExpiration,
        ObjectLockConfiguration, ObjectLockEnabled, ObjectLockLegalHold, ObjectLockLegalHoldStatus,
        ObjectLockRetentionMode, ObjectLockRule, VersioningConfiguration,
    };
    let s = start().await;
    let c = client(&s, SK);

    // ---- versioning
    c.create_bucket().bucket("hist").send().await.unwrap();
    c.put_bucket_versioning()
        .bucket("hist")
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await
        .unwrap();
    let st = c
        .get_bucket_versioning()
        .bucket("hist")
        .send()
        .await
        .unwrap();
    assert_eq!(st.status(), Some(&BucketVersioningStatus::Enabled));
    let v1 = c
        .put_object()
        .bucket("hist")
        .key("doc")
        .body(ByteStream::from_static(b"one"))
        .send()
        .await
        .unwrap();
    let v2 = c
        .put_object()
        .bucket("hist")
        .key("doc")
        .body(ByteStream::from_static(b"two"))
        .send()
        .await
        .unwrap();
    let (id1, id2) = (
        v1.version_id().unwrap().to_string(),
        v2.version_id().unwrap().to_string(),
    );
    assert_ne!(id1, id2);
    let old = c
        .get_object()
        .bucket("hist")
        .key("doc")
        .version_id(&id1)
        .send()
        .await
        .unwrap();
    assert_eq!(old.version_id(), Some(id1.as_str()));
    assert_eq!(body(old).await, b"one");

    let del = c
        .delete_object()
        .bucket("hist")
        .key("doc")
        .send()
        .await
        .unwrap();
    assert_eq!(del.delete_marker(), Some(true));
    let marker = del.version_id().unwrap().to_string();
    let err = c
        .get_object()
        .bucket("hist")
        .key("doc")
        .send()
        .await
        .unwrap_err();
    let raw = err.raw_response().unwrap();
    assert_eq!(raw.status().as_u16(), 404);
    assert_eq!(raw.headers().get("x-amz-delete-marker"), Some("true"));

    let listed = c
        .list_object_versions()
        .bucket("hist")
        .send()
        .await
        .unwrap();
    assert_eq!(listed.versions().len(), 2);
    assert_eq!(listed.delete_markers().len(), 1);
    assert!(listed.delete_markers()[0].is_latest().unwrap());

    // Removing the marker undeletes; versioned DeleteObjects removes old versions.
    c.delete_object()
        .bucket("hist")
        .key("doc")
        .version_id(&marker)
        .send()
        .await
        .unwrap();
    assert_eq!(
        body(
            c.get_object()
                .bucket("hist")
                .key("doc")
                .send()
                .await
                .unwrap()
        )
        .await,
        b"two"
    );
    let ids = vec![
        ObjectIdentifier::builder()
            .key("doc")
            .version_id(&id1)
            .build()
            .unwrap(),
    ];
    let out = c
        .delete_objects()
        .bucket("hist")
        .delete(Delete::builder().set_objects(Some(ids)).build().unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(out.deleted()[0].version_id(), Some(id1.as_str()));
    assert_eq!(
        c.list_object_versions()
            .bucket("hist")
            .send()
            .await
            .unwrap()
            .versions()
            .len(),
        1
    );

    // ---- object lock
    c.create_bucket()
        .bucket("vault")
        .object_lock_enabled_for_bucket(true)
        .send()
        .await
        .unwrap();
    c.put_object_lock_configuration()
        .bucket("vault")
        .object_lock_configuration(
            ObjectLockConfiguration::builder()
                .object_lock_enabled(ObjectLockEnabled::Enabled)
                .rule(
                    ObjectLockRule::builder()
                        .default_retention(
                            DefaultRetention::builder()
                                .mode(ObjectLockRetentionMode::Governance)
                                .days(1)
                                .build(),
                        )
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();
    let cfg = c
        .get_object_lock_configuration()
        .bucket("vault")
        .send()
        .await
        .unwrap();
    assert_eq!(
        cfg.object_lock_configuration()
            .unwrap()
            .rule()
            .unwrap()
            .default_retention()
            .unwrap()
            .days(),
        Some(1)
    );
    let put = c
        .put_object()
        .bucket("vault")
        .key("rec")
        .body(ByteStream::from_static(b"keep"))
        .send()
        .await
        .unwrap();
    let vid = put.version_id().unwrap().to_string();
    let ret = c
        .get_object_retention()
        .bucket("vault")
        .key("rec")
        .version_id(&vid)
        .send()
        .await
        .unwrap();
    assert_eq!(
        ret.retention().unwrap().mode(),
        Some(&ObjectLockRetentionMode::Governance)
    );
    let head = c
        .head_object()
        .bucket("vault")
        .key("rec")
        .send()
        .await
        .unwrap();
    assert_eq!(
        head.object_lock_mode().map(|m| m.as_str()),
        Some("GOVERNANCE")
    );

    let err = c
        .delete_object()
        .bucket("vault")
        .key("rec")
        .version_id(&vid)
        .send()
        .await
        .unwrap_err();
    assert_eq!(err.into_service_error().meta().code(), Some("AccessDenied"));

    // Legal hold beats governance bypass.
    c.put_object_legal_hold()
        .bucket("vault")
        .key("rec")
        .version_id(&vid)
        .legal_hold(
            ObjectLockLegalHold::builder()
                .status(ObjectLockLegalHoldStatus::On)
                .build(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(
        c.get_object_legal_hold()
            .bucket("vault")
            .key("rec")
            .version_id(&vid)
            .send()
            .await
            .unwrap()
            .legal_hold()
            .unwrap()
            .status(),
        Some(&ObjectLockLegalHoldStatus::On)
    );
    let err = c
        .delete_object()
        .bucket("vault")
        .key("rec")
        .version_id(&vid)
        .bypass_governance_retention(true)
        .send()
        .await
        .unwrap_err();
    assert_eq!(err.into_service_error().meta().code(), Some("AccessDenied"));
    c.put_object_legal_hold()
        .bucket("vault")
        .key("rec")
        .version_id(&vid)
        .legal_hold(
            ObjectLockLegalHold::builder()
                .status(ObjectLockLegalHoldStatus::Off)
                .build(),
        )
        .send()
        .await
        .unwrap();
    c.delete_object()
        .bucket("vault")
        .key("rec")
        .version_id(&vid)
        .bypass_governance_retention(true)
        .send()
        .await
        .unwrap();

    // ---- lifecycle configuration
    let rule = LifecycleRule::builder()
        .id("logs")
        .status(ExpirationStatus::Enabled)
        .filter(LifecycleRuleFilter::builder().prefix("logs/").build())
        .expiration(LifecycleExpiration::builder().days(30).build())
        .noncurrent_version_expiration(
            NoncurrentVersionExpiration::builder()
                .noncurrent_days(7)
                .build(),
        )
        .build()
        .unwrap();
    c.put_bucket_lifecycle_configuration()
        .bucket("hist")
        .lifecycle_configuration(
            BucketLifecycleConfiguration::builder()
                .rules(rule)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    let got = c
        .get_bucket_lifecycle_configuration()
        .bucket("hist")
        .send()
        .await
        .unwrap();
    assert_eq!(got.rules()[0].expiration().unwrap().days(), Some(30));
    assert_eq!(
        got.rules()[0]
            .noncurrent_version_expiration()
            .unwrap()
            .noncurrent_days(),
        Some(7)
    );
    c.delete_bucket_lifecycle()
        .bucket("hist")
        .send()
        .await
        .unwrap();
    let err = c
        .get_bucket_lifecycle_configuration()
        .bucket("hist")
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        err.into_service_error().meta().code(),
        Some("NoSuchLifecycleConfiguration")
    );
}
