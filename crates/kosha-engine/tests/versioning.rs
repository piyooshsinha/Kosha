use chrono::{Duration, Utc};
use futures::TryStreamExt;
use kosha_engine::*;

fn engine(dir: &std::path::Path) -> Engine {
    let mut cfg = EngineConfig::new(dir);
    cfg.sync = SyncMode::None;
    cfg.chunking = ChunkerConfig {
        min: 1024,
        avg: 4096,
        max: 16384,
    };
    Engine::open(cfg).unwrap()
}

fn body(n: usize, seed: u8) -> Vec<u8> {
    (0..n)
        .map(|i| (i as u8).wrapping_mul(31).wrapping_add(seed))
        .collect()
}

async fn put(e: &Engine, b: &str, k: &str, data: &[u8]) -> ObjectMeta {
    let mut w = e.writer();
    w.write(data).await.unwrap();
    e.put_object(
        b,
        k,
        w.finish().await.unwrap(),
        ObjectAttrs::default(),
        WriteCondition::default(),
    )
    .await
    .unwrap()
}

async fn put_locked(
    e: &Engine,
    b: &str,
    k: &str,
    data: &[u8],
    lock: ObjectLockState,
) -> Result<ObjectMeta> {
    let mut w = e.writer();
    w.write(data).await.unwrap();
    e.put_object_locked(
        b,
        k,
        w.finish().await.unwrap(),
        ObjectAttrs::default(),
        WriteCondition::default(),
        lock,
    )
    .await
}

async fn read(e: &Engine, m: &ObjectMeta) -> Vec<u8> {
    e.read(m, 0, m.size)
        .try_collect::<Vec<_>>()
        .await
        .unwrap()
        .concat()
}

async fn versions(e: &Engine, b: &str) -> Vec<(String, String, bool, bool)> {
    e.list_versions(
        b,
        VersionListQuery {
            max_keys: 1000,
            ..Default::default()
        },
    )
    .await
    .unwrap()
    .versions
    .into_iter()
    .map(|v| (v.key, v.version_id, v.is_latest, v.meta.is_none()))
    .collect()
}

fn delete_version(key: &str, v: &str) -> DeleteTarget {
    DeleteTarget {
        key: key.into(),
        version_id: Some(v.into()),
    }
}

#[tokio::test]
async fn versioning_lifecycle() {
    let dir = tempfile::tempdir().unwrap();
    let e = engine(dir.path());
    e.create_bucket("history").await.unwrap();
    let v1 = body(50_000, 1);
    put(&e, "history", "doc", &v1).await;
    put(&e, "history", "other", b"small").await;

    // Enabling versioning turns existing objects into null versions.
    e.set_bucket_config(
        "history",
        BucketConfigUpdate::Versioning(Versioning::Enabled),
    )
    .await
    .unwrap();
    assert_eq!(
        e.head_object("history", "doc")
            .await
            .unwrap()
            .version_id
            .as_deref(),
        Some("null")
    );

    let v2 = body(60_000, 2);
    let m2 = put(&e, "history", "doc", &v2).await;
    let id2 = m2.version_id.clone().unwrap();
    assert_ne!(id2, "null");
    assert_eq!(
        read(&e, &e.head_object("history", "doc").await.unwrap()).await,
        v2
    );
    let old = e.head_version("history", "doc", "null").await.unwrap();
    assert!(!old.is_latest);
    assert_eq!(read(&e, old.meta.as_ref().unwrap()).await, v1);

    // DELETE without a version adds a delete marker and hides the object.
    let out = e
        .delete_targets(
            "history",
            vec![DeleteTarget {
                key: "doc".into(),
                version_id: None,
            }],
            false,
        )
        .await
        .unwrap();
    assert!(out[0].delete_marker);
    let marker = out[0].version_id.clone().unwrap();
    assert!(matches!(
        e.head_object("history", "doc").await,
        Err(Error::NoSuchKey)
    ));
    assert_eq!(
        e.latest_delete_marker("history", "doc").await.unwrap(),
        Some(marker.clone())
    );
    assert_eq!(
        versions(&e, "history").await,
        vec![
            ("doc".into(), marker.clone(), true, true),
            ("doc".into(), id2.clone(), false, false),
            ("doc".into(), "null".into(), false, false),
            ("other".into(), "null".into(), true, false),
        ]
    );
    let listed = e
        .list_objects(
            "history",
            ListQuery {
                max_keys: 100,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(listed.objects.len(), 1, "only 'other' is visible");

    // Removing the marker brings v2 back; removing v2 brings v1 back.
    e.delete_targets("history", vec![delete_version("doc", &marker)], false)
        .await
        .unwrap();
    assert_eq!(
        read(&e, &e.head_object("history", "doc").await.unwrap()).await,
        v2
    );
    e.delete_targets("history", vec![delete_version("doc", &id2)], false)
        .await
        .unwrap();
    assert_eq!(
        read(&e, &e.head_object("history", "doc").await.unwrap()).await,
        v1
    );

    // Suspended: writes replace the null version (and free its chunks).
    e.set_bucket_config(
        "history",
        BucketConfigUpdate::Versioning(Versioning::Suspended),
    )
    .await
    .unwrap();
    let v3 = body(40_000, 3);
    put(&e, "history", "doc", &v3).await;
    let doc_versions: Vec<_> = versions(&e, "history")
        .await
        .into_iter()
        .filter(|v| v.0 == "doc")
        .collect();
    assert_eq!(
        doc_versions,
        vec![("doc".into(), "null".into(), true, false)]
    );

    // A non-empty versioned bucket cannot be deleted; clearing every version allows it.
    assert!(matches!(
        e.delete_bucket("history").await,
        Err(Error::BucketNotEmpty)
    ));
    let all: Vec<_> = versions(&e, "history")
        .await
        .into_iter()
        .map(|(k, v, _, _)| delete_version(&k, &v))
        .collect();
    e.delete_targets("history", all, false).await.unwrap();
    e.delete_bucket("history").await.unwrap();
    let s = e.stats().await.unwrap();
    assert_eq!((s.objects, s.logical_bytes, s.chunks), (0, 0, 0));
}

#[tokio::test]
async fn version_listing_paginates() {
    let dir = tempfile::tempdir().unwrap();
    let e = engine(dir.path());
    e.create_bucket("pages").await.unwrap();
    e.set_bucket_config("pages", BucketConfigUpdate::Versioning(Versioning::Enabled))
        .await
        .unwrap();
    for k in ["a", "b", "c"] {
        for i in 0..3u8 {
            put(&e, "pages", k, &[i]).await;
        }
    }
    let mut seen = Vec::new();
    let (mut km, mut vm) = (None, None);
    loop {
        let p = e
            .list_versions(
                "pages",
                VersionListQuery {
                    key_marker: km.clone(),
                    version_marker: vm.clone(),
                    max_keys: 2,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        seen.extend(
            p.versions
                .iter()
                .map(|v| (v.key.clone(), v.version_id.clone(), v.is_latest)),
        );
        if !p.is_truncated {
            break;
        }
        km = p.next_key_marker;
        vm = p.next_version_marker;
    }
    assert_eq!(seen.len(), 9);
    assert_eq!(
        seen.iter().filter(|v| v.2).count(),
        3,
        "exactly one latest per key"
    );
    let mut ids: Vec<_> = seen.iter().map(|v| v.1.clone()).collect();
    ids.dedup();
    assert_eq!(ids.len(), 9, "no version repeated across pages");
}

#[tokio::test]
async fn object_lock_rules() {
    let dir = tempfile::tempdir().unwrap();
    let e = engine(dir.path());
    // Lock needs a lock-enabled bucket.
    e.create_bucket("plain").await.unwrap();
    let held = ObjectLockState {
        retention: None,
        legal_hold: true,
    };
    assert!(matches!(
        put_locked(&e, "plain", "x", b"1", held.clone()).await,
        Err(Error::InvalidRequest(_))
    ));

    e.create_bucket_with("vault", true).await.unwrap();
    assert_eq!(
        e.bucket("vault").await.unwrap().config.versioning,
        Versioning::Enabled
    );
    e.set_bucket_config(
        "vault",
        BucketConfigUpdate::ObjectLock(ObjectLockConfig {
            default_retention: Some(DefaultRetention {
                mode: RetentionMode::Governance,
                days: 1,
            }),
        }),
    )
    .await
    .unwrap();
    // Versioning cannot be suspended on a lock-enabled bucket.
    assert!(matches!(
        e.set_bucket_config(
            "vault",
            BucketConfigUpdate::Versioning(Versioning::Suspended)
        )
        .await,
        Err(Error::InvalidBucketState(_))
    ));

    // Default GOVERNANCE retention: deletion refused, allowed with bypass.
    let g = put(&e, "vault", "gov", b"governed").await;
    let r = g.lock.retention.clone().unwrap();
    assert_eq!(r.mode, RetentionMode::Governance);
    assert!(r.until > Utc::now() + Duration::hours(23));
    let gid = g.version_id.clone().unwrap();
    let out = e
        .delete_targets("vault", vec![delete_version("gov", &gid)], false)
        .await
        .unwrap();
    assert!(matches!(out[0].error, Some(Rejection::AccessDenied(_))));
    let out = e
        .delete_targets("vault", vec![delete_version("gov", &gid)], true)
        .await
        .unwrap();
    assert!(out[0].error.is_none());

    // COMPLIANCE: no bypass, cannot shorten, can extend.
    let until = Utc::now() + Duration::hours(1);
    let c = put_locked(
        &e,
        "vault",
        "comp",
        b"compliant",
        ObjectLockState {
            retention: Some(Retention {
                mode: RetentionMode::Compliance,
                until,
            }),
            legal_hold: false,
        },
    )
    .await
    .unwrap();
    let cid = c.version_id.clone().unwrap();
    let out = e
        .delete_targets("vault", vec![delete_version("comp", &cid)], true)
        .await
        .unwrap();
    assert!(matches!(out[0].error, Some(Rejection::AccessDenied(_))));
    let shorter = Some(Some(Retention {
        mode: RetentionMode::Compliance,
        until: until - Duration::minutes(30),
    }));
    assert!(matches!(
        e.set_object_lock("vault", "comp", Some(cid.clone()), shorter, None, true)
            .await,
        Err(Error::AccessDenied(_))
    ));
    let longer = Some(Some(Retention {
        mode: RetentionMode::Compliance,
        until: until + Duration::days(1),
    }));
    e.set_object_lock("vault", "comp", Some(cid.clone()), longer, None, false)
        .await
        .unwrap();

    // Legal hold blocks deletion even when retention is lifted.
    let h = put(&e, "vault", "held", b"evidence").await;
    let hid = h.version_id.clone().unwrap();
    e.set_object_lock(
        "vault",
        "held",
        Some(hid.clone()),
        Some(None),
        Some(true),
        true,
    )
    .await
    .unwrap();
    let out = e
        .delete_targets("vault", vec![delete_version("held", &hid)], true)
        .await
        .unwrap();
    assert!(matches!(out[0].error, Some(Rejection::AccessDenied(_))));
    e.set_object_lock("vault", "held", Some(hid.clone()), None, Some(false), false)
        .await
        .unwrap();
    let out = e
        .delete_targets("vault", vec![delete_version("held", &hid)], false)
        .await
        .unwrap();
    assert!(out[0].error.is_none());

    // Plain DELETE (a delete marker) is always allowed: the data survives.
    let out = e
        .delete_targets(
            "vault",
            vec![DeleteTarget {
                key: "comp".into(),
                version_id: None,
            }],
            false,
        )
        .await
        .unwrap();
    assert!(out[0].delete_marker && out[0].error.is_none());
    let kept = e.head_version("vault", "comp", &cid).await.unwrap();
    assert_eq!(read(&e, kept.meta.as_ref().unwrap()).await, b"compliant");
}

async fn put_tagged(e: &Engine, b: &str, k: &str, tags: &[(&str, &str)]) {
    let mut w = e.writer();
    w.write(k.as_bytes()).await.unwrap();
    let attrs = ObjectAttrs {
        tags: tags
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
        ..Default::default()
    };
    e.put_object(
        b,
        k,
        w.finish().await.unwrap(),
        attrs,
        WriteCondition::default(),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn lifecycle_rules() {
    let dir = tempfile::tempdir().unwrap();
    let e = engine(dir.path());
    let later = Utc::now() + Duration::days(3);

    // Current-object expiration honours prefix and tag filters.
    e.create_bucket("logs").await.unwrap();
    put_tagged(&e, "logs", "app/1.log", &[("tier", "tmp")]).await;
    put_tagged(&e, "logs", "app/2.log", &[("tier", "keep")]).await;
    put_tagged(&e, "logs", "other/3.log", &[("tier", "tmp")]).await;
    e.create_upload("logs", "app/partial", ObjectAttrs::default())
        .await
        .unwrap();
    let rule = LifecycleRule {
        enabled: true,
        prefix: "app/".into(),
        tags: [("tier".to_string(), "tmp".to_string())].into(),
        expiration_days: Some(1),
        abort_incomplete_days: Some(2),
        transition_days: None,
        transition_date: None,
        transition_class: None,
        noncurrent_transition_days: None,
        noncurrent_transition_class: None,
        ..Default::default()
    };
    e.set_bucket_config("logs", BucketConfigUpdate::Lifecycle(Some(vec![rule])))
        .await
        .unwrap();
    // Nothing is old enough yet.
    assert_eq!(
        e.run_lifecycle(Utc::now()).await.unwrap(),
        LifecycleReport::default()
    );
    let r = e.run_lifecycle(later).await.unwrap();
    assert_eq!((r.expired, r.uploads_aborted), (1, 1));
    let left: Vec<_> = e
        .list_objects(
            "logs",
            ListQuery {
                max_keys: 10,
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .objects
        .into_iter()
        .map(|o| o.key)
        .collect();
    assert_eq!(left, ["app/2.log", "other/3.log"]);

    // Noncurrent expiration and expired delete markers in a versioned bucket.
    e.create_bucket("vers").await.unwrap();
    e.set_bucket_config("vers", BucketConfigUpdate::Versioning(Versioning::Enabled))
        .await
        .unwrap();
    put(&e, "vers", "kept", b"v1").await;
    put(&e, "vers", "kept", b"v2").await;
    put(&e, "vers", "gone", b"v1").await;
    e.delete_object("vers", "gone").await.unwrap();
    let rule = LifecycleRule {
        enabled: true,
        noncurrent_days: Some(1),
        expired_object_delete_marker: true,
        ..Default::default()
    };
    e.set_bucket_config("vers", BucketConfigUpdate::Lifecycle(Some(vec![rule])))
        .await
        .unwrap();
    let r = e.run_lifecycle(later).await.unwrap();
    assert_eq!((r.noncurrent_removed, r.markers_removed), (2, 1));
    let remaining = versions(&e, "vers").await;
    assert_eq!(remaining.len(), 1);
    assert_eq!((remaining[0].0.as_str(), remaining[0].2), ("kept", true));
    assert_eq!(
        read(&e, &e.head_object("vers", "kept").await.unwrap()).await,
        b"v2"
    );

    // Locked versions are refused, not removed.
    e.create_bucket_with("locked", true).await.unwrap();
    let until = Utc::now() + Duration::days(30);
    for data in [b"a".as_slice(), b"b"] {
        put_locked(
            &e,
            "locked",
            "k",
            data,
            ObjectLockState {
                retention: Some(Retention {
                    mode: RetentionMode::Compliance,
                    until,
                }),
                legal_hold: false,
            },
        )
        .await
        .unwrap();
    }
    let rule = LifecycleRule {
        enabled: true,
        noncurrent_days: Some(1),
        ..Default::default()
    };
    e.set_bucket_config("locked", BucketConfigUpdate::Lifecycle(Some(vec![rule])))
        .await
        .unwrap();
    let r = e.run_lifecycle(later).await.unwrap();
    assert_eq!((r.noncurrent_removed, r.refused), (0, 1));
    assert_eq!(versions(&e, "locked").await.len(), 2);
}
