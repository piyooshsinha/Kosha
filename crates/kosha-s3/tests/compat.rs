//! ACLs, RestoreObject and storage classes through the AWS SDK.

use std::collections::HashMap;
use std::sync::Arc;

use aws_sdk_s3::Client;
use aws_sdk_s3::config::{BehaviorVersion, Credentials as AwsCreds, Region};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{BucketCannedAcl, ObjectCannedAcl, RestoreRequest, StorageClass};
use bytes::Bytes;
use futures::future::BoxFuture;
use kosha_engine::{ColdStore, Engine, EngineConfig, SyncMode};
use kosha_s3::{Credentials, S3Config};
use parking_lot::Mutex;

const AK: &str = "ROOTKEY";
const SK: &str = "root-secret-key-for-tests";

#[derive(Default, Clone)]
struct MemCold(Arc<Mutex<HashMap<String, Bytes>>>);

impl ColdStore for MemCold {
    fn put(&self, key: String, data: Bytes) -> BoxFuture<'static, kosha_engine::Result<()>> {
        self.0.lock().insert(key, data);
        Box::pin(async { Ok(()) })
    }
    fn get(&self, key: String) -> BoxFuture<'static, kosha_engine::Result<Bytes>> {
        let r = self
            .0
            .lock()
            .get(&key)
            .cloned()
            .ok_or(kosha_engine::Error::Corrupt(key));
        Box::pin(async move { r })
    }
    fn delete(&self, key: String) -> BoxFuture<'static, kosha_engine::Result<()>> {
        self.0.lock().remove(&key);
        Box::pin(async { Ok(()) })
    }
}

fn code<E: aws_sdk_s3::error::ProvideErrorMetadata, R>(
    r: Result<impl std::fmt::Debug, aws_sdk_s3::error::SdkError<E, R>>,
) -> String {
    match r {
        Ok(v) => panic!("expected an error, got {v:?}"),
        Err(e) => e
            .as_service_error()
            .and_then(|s| s.code())
            .unwrap_or("none")
            .to_string(),
    }
}

#[tokio::test]
async fn acls_restore_and_storage_classes() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = EngineConfig::new(dir.path());
    cfg.sync = SyncMode::None;
    let engine = Engine::open(cfg).unwrap();
    let cold = MemCold::default();
    engine.set_cold_store(Arc::new(cold.clone())).unwrap();
    let app = kosha_s3::router(
        engine.clone(),
        S3Config {
            credentials: Credentials::new().with_key(AK, SK),
            ..Default::default()
        },
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let c = Client::from_conf(
        aws_sdk_s3::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new("us-east-1"))
            .endpoint_url(format!("http://{addr}"))
            .credentials_provider(AwsCreds::new(AK, SK, None, None, "test"))
            .force_path_style(true)
            .build(),
    );
    c.create_bucket().bucket("compat").send().await.unwrap();
    c.put_object()
        .bucket("compat")
        .key("doc.txt")
        .body(ByteStream::from_static(b"hello acl"))
        .send()
        .await
        .unwrap();

    // ACLs: reading returns the owner grant (not the object body).
    let acl = c
        .get_object_acl()
        .bucket("compat")
        .key("doc.txt")
        .send()
        .await
        .unwrap();
    assert_eq!(acl.grants().len(), 1);
    assert_eq!(
        acl.grants()[0].permission().map(|p| p.as_str()),
        Some("FULL_CONTROL")
    );
    c.put_object_acl()
        .bucket("compat")
        .key("doc.txt")
        .acl(ObjectCannedAcl::Private)
        .send()
        .await
        .unwrap();
    c.put_bucket_acl()
        .bucket("compat")
        .acl(BucketCannedAcl::Private)
        .send()
        .await
        .unwrap();
    assert_eq!(
        code(
            c.put_object_acl()
                .bucket("compat")
                .key("doc.txt")
                .acl(ObjectCannedAcl::PublicRead)
                .send()
                .await
        ),
        "NotImplemented"
    );
    assert_eq!(
        code(
            c.put_object_acl()
                .bucket("compat")
                .key("missing")
                .acl(ObjectCannedAcl::Private)
                .send()
                .await
        ),
        "NoSuchKey"
    );

    // Restoring a STANDARD object is refused, as in S3.
    let restore = || {
        c.restore_object()
            .bucket("compat")
            .key("doc.txt")
            .restore_request(RestoreRequest::builder().days(1).build())
    };
    assert_eq!(code(restore().send().await), "InvalidObjectState");

    // A cold upload: class reported, data offloaded, reads through, restore OK.
    let body = vec![7u8; 300_000];
    c.put_object()
        .bucket("compat")
        .key("archive.bin")
        .storage_class(StorageClass::GlacierIr)
        .body(ByteStream::from(body.clone()))
        .send()
        .await
        .unwrap();
    let head = c
        .head_object()
        .bucket("compat")
        .key("archive.bin")
        .send()
        .await
        .unwrap();
    assert_eq!(head.storage_class(), Some(&StorageClass::GlacierIr));
    let list = c.list_objects_v2().bucket("compat").send().await.unwrap();
    let classes: Vec<_> = list
        .contents()
        .iter()
        .map(|o| (o.key().unwrap(), o.storage_class().unwrap().as_str()))
        .collect();
    assert_eq!(
        classes,
        [("archive.bin", "GLACIER_IR"), ("doc.txt", "STANDARD")]
    );
    assert!(engine.run_offloader().await.unwrap() > 0);
    assert!(!cold.0.lock().is_empty());
    let got = c
        .get_object()
        .bucket("compat")
        .key("archive.bin")
        .send()
        .await
        .unwrap();
    assert_eq!(
        got.body.collect().await.unwrap().into_bytes().as_ref(),
        body.as_slice()
    );
    c.restore_object()
        .bucket("compat")
        .key("archive.bin")
        .restore_request(RestoreRequest::builder().days(1).build())
        .send()
        .await
        .unwrap();

    // Unknown storage classes are refused.
    assert_eq!(
        code(
            c.put_object()
                .bucket("compat")
                .key("x")
                .storage_class(StorageClass::from("PLATINUM"))
                .body(ByteStream::from_static(b"x"))
                .send()
                .await
        ),
        "InvalidStorageClass"
    );
}
