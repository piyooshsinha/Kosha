//! Compatibility tests driven by the official AWS SDK for Rust.

use std::time::Duration;

use aws_sdk_s3::Client;
use aws_sdk_s3::config::{BehaviorVersion, Credentials as AwsCreds, Region};
use aws_sdk_s3::presigning::PresigningConfig;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart, Delete, ObjectIdentifier};
use kosha_engine::{Engine, EngineConfig};
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
    cfg.fsync = false;
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
