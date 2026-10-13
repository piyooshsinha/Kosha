//! Bucket event notifications delivered to a webhook.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use aws_sdk_s3::Client;
use aws_sdk_s3::config::{BehaviorVersion, Credentials as AwsCreds, Region};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    CompletedMultipartUpload, CompletedPart, Event as S3Event, FilterRule, FilterRuleName,
    NotificationConfiguration, NotificationConfigurationFilter, QueueConfiguration, S3KeyFilter,
    Tag, Tagging,
};
use axum::http::StatusCode;
use kosha_engine::{Engine, EngineConfig, LifecycleRule, SyncMode};
use kosha_s3::{Credentials, S3Config};
use parking_lot::Mutex;
use serde_json::Value;

const AK: &str = "ROOTKEY";
const SK: &str = "root-secret-key-for-tests";

/// Collects posted events; answers 500 while `failing` is set.
#[derive(Clone, Default)]
struct Receiver {
    events: Arc<Mutex<Vec<Value>>>,
    failing: Arc<AtomicBool>,
    attempts: Arc<Mutex<u64>>,
}

async fn start_receiver() -> (Receiver, SocketAddr) {
    let r = Receiver::default();
    let state = r.clone();
    let app = axum::Router::new().route(
        "/hook",
        axum::routing::post(move |body: String| {
            let state = state.clone();
            async move {
                *state.attempts.lock() += 1;
                if state.failing.load(Ordering::SeqCst) {
                    return StatusCode::INTERNAL_SERVER_ERROR;
                }
                state
                    .events
                    .lock()
                    .push(serde_json::from_str(&body).unwrap());
                StatusCode::OK
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (r, addr)
}

/// (eventName, key) of every delivered record, in delivery order.
fn names(r: &Receiver) -> Vec<(String, String)> {
    r.events
        .lock()
        .iter()
        .map(|v| {
            let rec = &v["Records"][0];
            (
                rec["eventName"].as_str().unwrap().to_string(),
                rec["s3"]["object"]["key"].as_str().unwrap().to_string(),
            )
        })
        .collect()
}

async fn wait_for(r: &Receiver, n: usize) {
    for _ in 0..200 {
        if r.events.lock().len() >= n {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let got = names(r);
    panic!("only {} of {n} events arrived: {got:?}", got.len());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn webhook_notifications() {
    let (receiver, hook_addr) = start_receiver().await;
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = EngineConfig::new(dir.path());
    cfg.sync = SyncMode::None;
    cfg.min_part_size = 1;
    let engine = Engine::open(cfg).unwrap();
    let mut s3cfg = S3Config {
        credentials: Credentials::new().with_key(AK, SK),
        ..Default::default()
    };
    s3cfg
        .webhooks
        .insert("audit".into(), format!("http://{hook_addr}/hook"));
    kosha_s3::notify::spawn_dispatcher(
        engine.clone(),
        s3cfg.webhooks.clone(),
        "us-east-1".into(),
        Arc::new(|| true),
    );
    let app = kosha_s3::router(engine.clone(), s3cfg);
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

    c.create_bucket().bucket("media").send().await.unwrap();
    let queue = |id: &str, events: Vec<S3Event>, prefix: Option<&str>| {
        let mut q = QueueConfiguration::builder()
            .id(id)
            .queue_arn("arn:kosha:sqs::audit:webhook")
            .set_events(Some(events));
        if let Some(p) = prefix {
            q = q.filter(
                NotificationConfigurationFilter::builder()
                    .key(
                        S3KeyFilter::builder()
                            .filter_rules(
                                FilterRule::builder()
                                    .name(FilterRuleName::Prefix)
                                    .value(p)
                                    .build(),
                            )
                            .build(),
                    )
                    .build(),
            );
        }
        q.build().unwrap()
    };
    // Unknown targets are refused.
    let bad = c
        .put_bucket_notification_configuration()
        .bucket("media")
        .notification_configuration(
            NotificationConfiguration::builder()
                .queue_configurations(
                    QueueConfiguration::builder()
                        .queue_arn("arn:kosha:sqs::nowhere:webhook")
                        .events(S3Event::S3ObjectCreated)
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .send()
        .await;
    assert!(bad.is_err());

    c.put_bucket_notification_configuration()
        .bucket("media")
        .notification_configuration(
            NotificationConfiguration::builder()
                .queue_configurations(queue(
                    "uploads",
                    vec![
                        S3Event::S3ObjectCreated,
                        S3Event::S3ObjectRemoved,
                        S3Event::from("s3:ObjectTagging:*"),
                        S3Event::from("s3:LifecycleExpiration:*"),
                    ],
                    Some("in/"),
                ))
                .build(),
        )
        .send()
        .await
        .unwrap();
    let got = c
        .get_bucket_notification_configuration()
        .bucket("media")
        .send()
        .await
        .unwrap();
    assert_eq!(got.queue_configurations().len(), 1);
    assert_eq!(got.queue_configurations()[0].id(), Some("uploads"));

    let put = |k: &'static str, body: &'static [u8]| {
        c.put_object()
            .bucket("media")
            .key(k)
            .body(ByteStream::from_static(body))
            .send()
    };
    put("in/a b.txt", b"hello").await.unwrap();
    put("out/ignored.txt", b"not selected by the prefix")
        .await
        .unwrap();
    c.copy_object()
        .bucket("media")
        .key("in/copy.txt")
        .copy_source("media/in/a b.txt")
        .send()
        .await
        .unwrap();
    let up = c
        .create_multipart_upload()
        .bucket("media")
        .key("in/multi.bin")
        .send()
        .await
        .unwrap();
    let part = c
        .upload_part()
        .bucket("media")
        .key("in/multi.bin")
        .upload_id(up.upload_id().unwrap())
        .part_number(1)
        .body(ByteStream::from_static(b"part one"))
        .send()
        .await
        .unwrap();
    c.complete_multipart_upload()
        .bucket("media")
        .key("in/multi.bin")
        .upload_id(up.upload_id().unwrap())
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .parts(
                    CompletedPart::builder()
                        .part_number(1)
                        .e_tag(part.e_tag().unwrap())
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();
    c.put_object_tagging()
        .bucket("media")
        .key("in/copy.txt")
        .tagging(
            Tagging::builder()
                .tag_set(Tag::builder().key("k").value("v").build().unwrap())
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    c.delete_object()
        .bucket("media")
        .key("in/copy.txt")
        .send()
        .await
        .unwrap();
    // Deleting a key that does not exist produces no event.
    c.delete_object()
        .bucket("media")
        .key("in/never-existed")
        .send()
        .await
        .unwrap();

    wait_for(&receiver, 5).await;
    let expected = vec![
        ("ObjectCreated:Put", "in/a+b.txt"),
        ("ObjectCreated:Copy", "in/copy.txt"),
        ("ObjectCreated:CompleteMultipartUpload", "in/multi.bin"),
        ("ObjectTagging:Put", "in/copy.txt"),
        ("ObjectRemoved:Delete", "in/copy.txt"),
    ];
    let expected: Vec<(String, String)> = expected
        .into_iter()
        .map(|(a, b)| (a.to_string(), b.to_string()))
        .collect();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(names(&receiver), expected);

    // The message follows the S3 event structure.
    let first = receiver.events.lock()[0]["Records"][0].clone();
    assert_eq!(first["eventVersion"], "2.1");
    assert_eq!(first["s3"]["bucket"]["name"], "media");
    assert_eq!(first["s3"]["configurationId"], "uploads");
    assert_eq!(first["s3"]["object"]["size"], 5);
    assert_eq!(
        first["s3"]["object"]["eTag"],
        "5d41402abc4b2a76b9719d911017c592"
    );
    let seqs: Vec<String> = receiver
        .events
        .lock()
        .iter()
        .map(|v| {
            v["Records"][0]["s3"]["object"]["sequencer"]
                .as_str()
                .unwrap()
                .to_string()
        })
        .collect();
    let mut sorted = seqs.clone();
    sorted.sort();
    assert_eq!(seqs, sorted, "sequencers increase");

    // While the receiver fails, events wait in the replicated queue and are
    // retried; nothing is lost or reordered.
    receiver.failing.store(true, Ordering::SeqCst);
    for k in ["in/r1", "in/r2", "in/r3"] {
        c.put_object()
            .bucket("media")
            .key(k)
            .body(ByteStream::from_static(b"x"))
            .send()
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert!(*receiver.attempts.lock() > 5, "deliveries were retried");
    assert_eq!(
        engine.pending_event_counts().unwrap(),
        vec![("audit".to_string(), 3)]
    );
    receiver.failing.store(false, Ordering::SeqCst);
    wait_for(&receiver, 8).await;
    let tail: Vec<String> = names(&receiver)[5..]
        .iter()
        .map(|(_, k)| k.clone())
        .collect();
    assert_eq!(tail, ["in/r1", "in/r2", "in/r3"]);

    // Lifecycle expirations are reported as such.
    engine
        .set_bucket_config(
            "media",
            kosha_engine::BucketConfigUpdate::Lifecycle(Some(vec![LifecycleRule {
                id: Some("expire".into()),
                enabled: true,
                prefix: "in/r".into(),
                tags: Default::default(),
                expiration_days: Some(1),
                expiration_date: None,
                expired_object_delete_marker: false,
                noncurrent_days: None,
                abort_incomplete_days: None,
                transition_days: None,
                transition_date: None,
                transition_class: None,
                noncurrent_transition_days: None,
                noncurrent_transition_class: None,
            }])),
        )
        .await
        .unwrap();
    engine
        .run_lifecycle(chrono::Utc::now() + chrono::Duration::days(2))
        .await
        .unwrap();
    wait_for(&receiver, 11).await;
    let tail: Vec<(String, String)> = names(&receiver)[8..].to_vec();
    assert!(
        tail.iter().all(|(n, _)| n == "LifecycleExpiration:Delete"),
        "{tail:?}"
    );

    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(engine.pending_event_counts().unwrap().is_empty());
}
