//! Users, identity policies, bucket policies (including anonymous access)
//! and SigV2 header authentication.

use std::net::SocketAddr;

use aws_sdk_s3::Client;
use aws_sdk_s3::config::{BehaviorVersion, Credentials as AwsCreds, Region};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{Delete, ObjectIdentifier};
use base64::Engine as _;
use hmac::{Hmac, KeyInit, Mac};
use kosha_engine::{Engine, EngineConfig, SyncMode};
use kosha_s3::{Credentials, S3Config};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const AK: &str = "ROOTKEY";
const SK: &str = "root-secret-key-for-tests";

struct Server {
    addr: SocketAddr,
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
    tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap()
    });
    Server { addr, _dir: dir }
}

fn client(s: &Server, ak: &str, sk: &str) -> Client {
    let conf = aws_sdk_s3::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new("us-east-1"))
        .endpoint_url(format!("http://{}", s.addr))
        .credentials_provider(AwsCreds::new(ak, sk, None, None, "test"))
        .force_path_style(true)
        .build();
    Client::from_conf(conf)
}

/// A raw HTTP/1.1 request, optionally signed with SigV2
/// (`Authorization: AWS key:signature`). Returns (status, body).
async fn http(
    addr: SocketAddr,
    method: &str,
    path: &str,
    body: &str,
    sign: Option<(&str, &str)>,
) -> (u16, String) {
    let date = chrono::Utc::now()
        .format("%a, %d %b %Y %H:%M:%S GMT")
        .to_string();
    let ctype = if body.is_empty() {
        ""
    } else {
        "application/json"
    };
    let mut req = format!(
        "{method} {path} HTTP/1.1\r\nHost: {addr}\r\nDate: {date}\r\nConnection: close\r\nContent-Length: {}\r\n",
        body.len()
    );
    if !ctype.is_empty() {
        req.push_str(&format!("Content-Type: {ctype}\r\n"));
    }
    if let Some((ak, sk)) = sign {
        let resource = path.split('?').next().unwrap();
        let sts = format!("{method}\n\n{ctype}\n{date}\n{resource}");
        let mut m = Hmac::<sha1::Sha1>::new_from_slice(sk.as_bytes()).unwrap();
        m.update(sts.as_bytes());
        let sig = base64::engine::general_purpose::STANDARD.encode(m.finalize().into_bytes());
        req.push_str(&format!("Authorization: AWS {ak}:{sig}\r\n"));
    }
    req.push_str("\r\n");
    req.push_str(body);
    let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
    s.write_all(req.as_bytes()).await.unwrap();
    let mut out = String::new();
    s.read_to_string(&mut out).await.unwrap();
    let status = out[9..12].parse().unwrap();
    let body = out.split_once("\r\n\r\n").map(|x| x.1).unwrap_or("");
    // Responses are small; strip chunked framing if present.
    let body = if out
        .to_ascii_lowercase()
        .contains("transfer-encoding: chunked")
    {
        body.lines()
            .collect::<Vec<_>>()
            .chunks(2)
            .filter_map(|c| c.get(1).copied())
            .collect::<String>()
    } else {
        body.to_string()
    };
    (status, body)
}

async fn create_user(s: &Server, name: &str, policy: Value) -> (String, String) {
    let (status, body) = http(
        s.addr,
        "POST",
        "/_kosha/admin/users",
        &json!({ "name": name, "policy": policy }).to_string(),
        Some((AK, SK)),
    )
    .await;
    assert_eq!(status, 201, "{body}");
    let v: Value = serde_json::from_str(&body).unwrap();
    (
        v["access_key"].as_str().unwrap().into(),
        v["secret_key"].as_str().unwrap().into(),
    )
}

fn denied<T, E, R>(r: Result<T, aws_sdk_s3::error::SdkError<E, R>>)
where
    E: aws_sdk_s3::error::ProvideErrorMetadata + std::fmt::Debug,
    R: std::fmt::Debug,
{
    match r {
        Err(e) => {
            let code = e
                .as_service_error()
                .and_then(|s| s.code())
                .map(str::to_string);
            assert_eq!(code.as_deref(), Some("AccessDenied"), "{e:?}");
        }
        Ok(_) => panic!("expected AccessDenied"),
    }
}

#[tokio::test]
async fn users_and_identity_policies() {
    let s = start().await;
    let root = client(&s, AK, SK);
    for b in ["data", "other"] {
        root.create_bucket().bucket(b).send().await.unwrap();
    }
    root.put_object()
        .bucket("other")
        .key("secret.txt")
        .body(ByteStream::from_static(b"top secret"))
        .send()
        .await
        .unwrap();

    // Ana may read and write under data/ana/ and list with that prefix,
    // and may never delete anything under data/ana/keep/.
    let (ak, sk) = create_user(
        &s,
        "ana",
        json!({
            "Version": "2012-10-17",
            "Statement": [
                {"Effect": "Allow", "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
                 "Resource": "arn:aws:s3:::data/${aws:username}/*"},
                {"Effect": "Allow", "Action": "s3:ListBucket", "Resource": "arn:aws:s3:::data",
                 "Condition": {"StringLike": {"s3:prefix": "ana/*"}}},
                {"Effect": "Deny", "Action": "s3:DeleteObject", "Resource": "arn:aws:s3:::data/ana/keep/*"}
            ]
        }),
    )
    .await;
    let ana = client(&s, &ak, &sk);

    ana.put_object()
        .bucket("data")
        .key("ana/a.txt")
        .body(ByteStream::from_static(b"hi"))
        .send()
        .await
        .unwrap();
    ana.put_object()
        .bucket("data")
        .key("ana/keep/b.txt")
        .body(ByteStream::from_static(b"keep"))
        .send()
        .await
        .unwrap();
    let got = ana
        .get_object()
        .bucket("data")
        .key("ana/a.txt")
        .send()
        .await
        .unwrap();
    assert_eq!(
        got.body.collect().await.unwrap().into_bytes().as_ref(),
        b"hi"
    );
    let l = ana
        .list_objects_v2()
        .bucket("data")
        .prefix("ana/")
        .send()
        .await
        .unwrap();
    assert_eq!(l.key_count(), Some(2));

    denied(ana.list_objects_v2().bucket("data").send().await);
    denied(ana.list_buckets().send().await);
    denied(ana.create_bucket().bucket("anas").send().await);
    denied(
        ana.get_object()
            .bucket("other")
            .key("secret.txt")
            .send()
            .await,
    );
    denied(
        ana.put_object()
            .bucket("data")
            .key("bob/x")
            .body(ByteStream::from_static(b"x"))
            .send()
            .await,
    );
    denied(
        ana.delete_object()
            .bucket("data")
            .key("ana/keep/b.txt")
            .send()
            .await,
    );
    // Copying needs read access to the source too.
    denied(
        ana.copy_object()
            .bucket("data")
            .key("ana/stolen.txt")
            .copy_source("other/secret.txt")
            .send()
            .await,
    );

    // DeleteObjects is checked key by key.
    let del = ana
        .delete_objects()
        .bucket("data")
        .delete(
            Delete::builder()
                .objects(
                    ObjectIdentifier::builder()
                        .key("ana/a.txt")
                        .build()
                        .unwrap(),
                )
                .objects(
                    ObjectIdentifier::builder()
                        .key("ana/keep/b.txt")
                        .build()
                        .unwrap(),
                )
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(del.deleted().len(), 1);
    assert_eq!(del.deleted()[0].key(), Some("ana/a.txt"));
    assert_eq!(del.errors().len(), 1);
    assert_eq!(del.errors()[0].key(), Some("ana/keep/b.txt"));
    assert_eq!(del.errors()[0].code(), Some("AccessDenied"));

    // Users cannot use the admin API.
    let (status, _) = http(s.addr, "GET", "/_kosha/admin/users", "", Some((&ak, &sk))).await;
    assert_eq!(status, 403);

    // Disabling the user revokes its key; deleting removes it.
    let (status, body) = http(
        s.addr,
        "PUT",
        &format!("/_kosha/admin/users/{ak}"),
        r#"{"enabled": false}"#,
        Some((AK, SK)),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    let e = ana
        .get_object()
        .bucket("data")
        .key("ana/keep/b.txt")
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        e.as_service_error().and_then(|e| e.meta().code()),
        Some("InvalidAccessKeyId")
    );

    let (status, body) = http(s.addr, "GET", "/_kosha/admin/users", "", Some((AK, SK))).await;
    assert_eq!(status, 200);
    let v: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["users"][0]["name"], "ana");
    assert_eq!(v["users"][0]["enabled"], false);
    assert!(v["users"][0].get("secret_key").is_none());

    let (status, _) = http(
        s.addr,
        "DELETE",
        &format!("/_kosha/admin/users/{ak}"),
        "",
        Some((AK, SK)),
    )
    .await;
    assert_eq!(status, 204);
    let (status, _) = http(
        s.addr,
        "GET",
        &format!("/_kosha/admin/users/{ak}"),
        "",
        Some((AK, SK)),
    )
    .await;
    assert_eq!(status, 404);

    // Invalid policies and duplicate names are refused.
    let (status, body) = http(
        s.addr,
        "POST",
        "/_kosha/admin/users",
        r#"{"name":"bad","policy":{"Statement":[{"Effect":"Allow","Action":"ec2:*","Resource":"*"}]}}"#,
        Some((AK, SK)),
    )
    .await;
    assert_eq!(status, 400, "{body}");
    create_user(&s, "dup", json!(null)).await;
    let (status, _) = http(
        s.addr,
        "POST",
        "/_kosha/admin/users",
        r#"{"name":"dup"}"#,
        Some((AK, SK)),
    )
    .await;
    assert_eq!(status, 409);
}

#[tokio::test]
async fn bucket_policies_and_anonymous_access() {
    let s = start().await;
    let root = client(&s, AK, SK);
    root.create_bucket().bucket("site").send().await.unwrap();
    root.put_object()
        .bucket("site")
        .key("index.html")
        .body(ByteStream::from_static(b"<h1>hello</h1>"))
        .send()
        .await
        .unwrap();

    // Without a policy, anonymous requests are refused.
    let (status, _) = http(s.addr, "GET", "/site/index.html", "", None).await;
    assert_eq!(status, 403);
    let st = root
        .get_bucket_policy_status()
        .bucket("site")
        .send()
        .await
        .unwrap();
    assert_eq!(st.policy_status().and_then(|p| p.is_public()), Some(false));
    let e = root
        .get_bucket_policy()
        .bucket("site")
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        e.as_service_error().and_then(|e| e.meta().code()),
        Some("NoSuchBucketPolicy")
    );

    // Public read for everyone; bob (a user without an identity policy)
    // may also upload under uploads/.
    let (bob_ak, bob_sk) = create_user(&s, "bob", json!(null)).await;
    let policy = json!({
        "Version": "2012-10-17",
        "Statement": [
            {"Sid": "PublicRead", "Effect": "Allow", "Principal": "*",
             "Action": "s3:GetObject", "Resource": "arn:aws:s3:::site/*"},
            {"Effect": "Allow", "Principal": {"AWS": ["arn:aws:iam::000000000000:user/bob"]},
             "Action": "s3:PutObject", "Resource": "arn:aws:s3:::site/uploads/*"},
            {"Effect": "Deny", "Principal": "*", "Action": "s3:GetObject",
             "Resource": "arn:aws:s3:::site/private/*"}
        ]
    });
    root.put_bucket_policy()
        .bucket("site")
        .policy(policy.to_string())
        .send()
        .await
        .unwrap();
    let got = root
        .get_bucket_policy()
        .bucket("site")
        .send()
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(got.policy().unwrap()).unwrap(),
        policy
    );
    let st = root
        .get_bucket_policy_status()
        .bucket("site")
        .send()
        .await
        .unwrap();
    assert_eq!(st.policy_status().and_then(|p| p.is_public()), Some(true));

    let (status, body) = http(s.addr, "GET", "/site/index.html", "", None).await;
    assert_eq!((status, body.as_str()), (200, "<h1>hello</h1>"));
    // Without s3:ListBucket a missing key is AccessDenied, not NoSuchKey.
    let (status, body) = http(s.addr, "GET", "/site/missing.html", "", None).await;
    assert_eq!(status, 403, "{body}");
    let (status, _) = http(s.addr, "PUT", "/site/hack.html", "x", None).await;
    assert_eq!(status, 403);
    let (status, _) = http(s.addr, "GET", "/site?list-type=2", "", None).await;
    assert_eq!(status, 403);

    let bob = client(&s, &bob_ak, &bob_sk);
    bob.put_object()
        .bucket("site")
        .key("uploads/b.txt")
        .body(ByteStream::from_static(b"b"))
        .send()
        .await
        .unwrap();
    denied(
        bob.put_object()
            .bucket("site")
            .key("index.html")
            .body(ByteStream::from_static(b"defaced"))
            .send()
            .await,
    );
    // Explicit deny beats root? No: root keys bypass policies entirely.
    root.put_object()
        .bucket("site")
        .key("private/p.txt")
        .body(ByteStream::from_static(b"p"))
        .send()
        .await
        .unwrap();
    root.get_object()
        .bucket("site")
        .key("private/p.txt")
        .send()
        .await
        .unwrap();
    // ...but it applies to everyone else, even though PublicRead matches.
    let (status, _) = http(s.addr, "GET", "/site/private/p.txt", "", None).await;
    assert_eq!(status, 403);
    denied(
        bob.get_object()
            .bucket("site")
            .key("private/p.txt")
            .send()
            .await,
    );

    // Malformed policies are refused.
    let e = root
        .put_bucket_policy()
        .bucket("site")
        .policy(r#"{"Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::elsewhere/*"}]}"#)
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        e.as_service_error().and_then(|e| e.meta().code()),
        Some("MalformedPolicy")
    );

    root.delete_bucket_policy()
        .bucket("site")
        .send()
        .await
        .unwrap();
    let (status, _) = http(s.addr, "GET", "/site/index.html", "", None).await;
    assert_eq!(status, 403);
}

#[tokio::test]
async fn sigv2_header_auth() {
    let s = start().await;
    let (status, _) = http(s.addr, "PUT", "/v2bucket", "", Some((AK, SK))).await;
    assert_eq!(status, 200);
    let (status, _) = http(
        s.addr,
        "PUT",
        "/v2bucket/hello.txt",
        "hello v2",
        Some((AK, SK)),
    )
    .await;
    assert_eq!(status, 200);
    let (status, body) = http(s.addr, "GET", "/v2bucket/hello.txt", "", Some((AK, SK))).await;
    assert_eq!((status, body.as_str()), (200, "hello v2"));
    let (status, body) = http(
        s.addr,
        "GET",
        "/v2bucket/hello.txt",
        "",
        Some((AK, "wrong-secret")),
    )
    .await;
    assert_eq!(status, 403, "{body}");
    assert!(body.contains("SignatureDoesNotMatch"));
}
