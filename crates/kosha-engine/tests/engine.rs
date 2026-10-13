use futures::TryStreamExt;
use kosha_engine::*;

fn config(dir: &std::path::Path) -> EngineConfig {
    let mut cfg = EngineConfig::new(dir);
    cfg.sync = SyncMode::None;
    cfg.chunking = ChunkerConfig {
        min: 1024,
        avg: 4096,
        max: 16384,
    };
    cfg.min_part_size = 1;
    cfg
}

fn data(n: usize, seed: u64) -> Vec<u8> {
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

async fn put(e: &Engine, b: &str, k: &str, body: &[u8]) -> ObjectMeta {
    let mut w = e.writer();
    for piece in body.chunks(5000) {
        w.write(piece).await.unwrap();
    }
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

async fn get(e: &Engine, b: &str, k: &str, start: u64, end: Option<u64>) -> Vec<u8> {
    let m = e.head_object(b, k).await.unwrap();
    let parts: Vec<_> = e
        .read(&m, start, end.unwrap_or(m.size))
        .try_collect()
        .await
        .unwrap();
    parts.concat()
}

#[tokio::test]
async fn put_get_range_delete() {
    let dir = tempfile::tempdir().unwrap();
    let e = Engine::open(config(dir.path())).unwrap();
    e.create_bucket("photos").await.unwrap();

    let body = data(200_000, 1);
    let m = put(&e, "photos", "a/b.bin", &body).await;
    assert_eq!(m.size, body.len() as u64);
    assert_eq!(m.etag, hex::encode(md5::Md5::digest(&body)));
    assert!(m.segments.len() > 5);

    assert_eq!(get(&e, "photos", "a/b.bin", 0, None).await, body);
    assert_eq!(
        get(&e, "photos", "a/b.bin", 12_345, Some(150_001)).await,
        &body[12_345..150_001]
    );

    let tiny = put(&e, "photos", "tiny", b"hi").await;
    assert!(matches!(tiny.segments[..], [Segment::Inline(_)]));
    assert_eq!(get(&e, "photos", "tiny", 0, None).await, b"hi");

    let empty = put(&e, "photos", "empty", b"").await;
    assert_eq!(empty.size, 0);
    assert_eq!(get(&e, "photos", "empty", 0, None).await, b"");

    assert!(e.delete_object("photos", "a/b.bin").await.unwrap());
    assert!(matches!(
        e.head_object("photos", "a/b.bin").await,
        Err(Error::NoSuchKey)
    ));
    assert!(matches!(
        e.delete_bucket("photos").await,
        Err(Error::BucketNotEmpty)
    ));
    e.delete_objects("photos", vec!["tiny".into(), "empty".into()])
        .await
        .unwrap();
    e.delete_bucket("photos").await.unwrap();

    let s = e.stats().await.unwrap();
    assert_eq!(
        (s.objects, s.chunks, s.physical_bytes, s.buckets),
        (0, 0, 0, 0)
    );
    let leftover = walk(&dir.path().join("chunks"));
    assert_eq!(leftover, 0, "all chunk files collected");
}

use md5::Digest;

fn walk(p: &std::path::Path) -> usize {
    std::fs::read_dir(p)
        .unwrap()
        .map(|e| {
            let e = e.unwrap();
            if e.file_type().unwrap().is_dir() {
                walk(&e.path())
            } else {
                1
            }
        })
        .sum()
}

#[tokio::test]
async fn dedup_and_zero_copy_copy() {
    let dir = tempfile::tempdir().unwrap();
    let e = Engine::open(config(dir.path())).unwrap();
    e.create_bucket("bk1").await.unwrap();

    let base = data(300_000, 9);
    put(&e, "bk1", "v1", &base).await;
    let after_one = e.stats().await.unwrap();

    // A slightly edited copy shares most chunks with the original.
    let mut edited = base.clone();
    edited.splice(150_000..150_000, b"inserted".iter().copied());
    put(&e, "bk1", "v2", &edited).await;
    let after_two = e.stats().await.unwrap();
    assert_eq!(after_two.logical_bytes, (base.len() + edited.len()) as u64);
    assert!(
        after_two.unique_bytes < after_one.unique_bytes + 60_000,
        "{after_two:?}"
    );

    // Copies are metadata only.
    e.copy_object(
        "bk1",
        "v1",
        "bk1",
        "v1-copy",
        None,
        WriteCondition::default(),
    )
    .await
    .unwrap();
    assert_eq!(
        e.stats().await.unwrap().unique_bytes,
        after_two.unique_bytes
    );
    assert_eq!(get(&e, "bk1", "v1-copy", 0, None).await, base);

    // Deleting the original keeps the shared chunks alive for the copy.
    e.delete_object("bk1", "v1").await.unwrap();
    assert_eq!(get(&e, "bk1", "v1-copy", 0, None).await, base);
    assert_eq!(get(&e, "bk1", "v2", 0, None).await, edited);
}

#[tokio::test]
async fn conditional_writes() {
    let dir = tempfile::tempdir().unwrap();
    let e = Engine::open(config(dir.path())).unwrap();
    e.create_bucket("locks").await.unwrap();
    let cond = WriteCondition {
        if_none_match_any: true,
        ..Default::default()
    };
    let w = e.writer();
    e.put_object(
        "locks",
        "leader",
        w.finish().await.unwrap(),
        ObjectAttrs::default(),
        cond.clone(),
    )
    .await
    .unwrap();
    let w = e.writer();
    let r = e
        .put_object(
            "locks",
            "leader",
            w.finish().await.unwrap(),
            ObjectAttrs::default(),
            cond,
        )
        .await;
    assert!(matches!(r, Err(Error::PreconditionFailed)));
}

#[tokio::test]
async fn listing_with_delimiter_and_pagination() {
    let dir = tempfile::tempdir().unwrap();
    let e = Engine::open(config(dir.path())).unwrap();
    e.create_bucket("list").await.unwrap();
    for k in [
        "a.txt",
        "dir1/x",
        "dir1/y",
        "dir2/z",
        "dir2/sub/w",
        "e.txt",
        "f/",
    ] {
        put(&e, "list", k, b"x").await;
    }

    let q = |prefix: &str, delim: Option<&str>, after: Option<String>, max| ListQuery {
        prefix: prefix.into(),
        delimiter: delim.map(Into::into),
        start_after: after,
        max_keys: max,
    };
    let p = e
        .list_objects("list", q("", Some("/"), None, 1000))
        .await
        .unwrap();
    let keys: Vec<_> = p.objects.iter().map(|o| o.key.as_str()).collect();
    assert_eq!(keys, ["a.txt", "e.txt"]);
    assert_eq!(p.common_prefixes, ["dir1/", "dir2/", "f/"]);
    assert!(!p.is_truncated);

    // Page through with max_keys=2 and make sure nothing repeats or is lost.
    let mut seen = Vec::new();
    let mut after = None;
    loop {
        let p = e
            .list_objects("list", q("", Some("/"), after.clone(), 2))
            .await
            .unwrap();
        seen.extend(p.objects.iter().map(|o| o.key.clone()));
        seen.extend(p.common_prefixes.iter().cloned());
        if !p.is_truncated {
            break;
        }
        after = p.next_marker;
    }
    seen.sort();
    assert_eq!(seen, ["a.txt", "dir1/", "dir2/", "e.txt", "f/"]);

    let p = e
        .list_objects("list", q("dir2/", Some("/"), None, 1000))
        .await
        .unwrap();
    assert_eq!(
        p.objects.iter().map(|o| o.key.as_str()).collect::<Vec<_>>(),
        ["dir2/z"]
    );
    assert_eq!(p.common_prefixes, ["dir2/sub/"]);

    let p = e
        .list_objects("list", q("dir", None, None, 1000))
        .await
        .unwrap();
    assert_eq!(p.objects.len(), 4);
}

#[tokio::test]
async fn multipart_upload() {
    let dir = tempfile::tempdir().unwrap();
    let e = Engine::open(config(dir.path())).unwrap();
    e.create_bucket("mpu").await.unwrap();
    let up = e
        .create_upload("mpu", "big", ObjectAttrs::default())
        .await
        .unwrap();

    let p1 = data(50_000, 3);
    let p2 = data(30_000, 4);
    let mut etags = Vec::new();
    for (n, body) in [(1u32, &p1), (2, &p2)] {
        let mut w = e.writer();
        w.write(body).await.unwrap();
        let part = e
            .put_part("mpu", "big", &up.upload_id, n, w.finish().await.unwrap())
            .await
            .unwrap();
        etags.push((n, part.etag));
    }
    // Overwrite part 2 with the same content - refcounts must stay balanced.
    let mut w = e.writer();
    w.write(&p2).await.unwrap();
    e.put_part("mpu", "big", &up.upload_id, 2, w.finish().await.unwrap())
        .await
        .unwrap();

    let (_, parts) = e.list_parts("mpu", "big", &up.upload_id).await.unwrap();
    assert_eq!(parts.len(), 2);
    assert_eq!(e.list_uploads("mpu", "").await.unwrap().len(), 1);

    let m = e
        .complete_upload(
            "mpu",
            "big",
            &up.upload_id,
            etags,
            WriteCondition::default(),
        )
        .await
        .unwrap();
    assert!(m.etag.ends_with("-2"));
    assert_eq!(
        get(&e, "mpu", "big", 0, None).await,
        [p1.clone(), p2.clone()].concat()
    );
    assert!(e.list_uploads("mpu", "").await.unwrap().is_empty());

    e.delete_object("mpu", "big").await.unwrap();
    let s = e.stats().await.unwrap();
    assert_eq!((s.chunks, s.physical_bytes), (0, 0));
}

#[tokio::test]
async fn survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    let body = data(100_000, 5);
    {
        let e = Engine::open(config(dir.path())).unwrap();
        e.create_bucket("persist").await.unwrap();
        put(&e, "persist", "k", &body).await;
    }
    let e = Engine::open(config(dir.path())).unwrap();
    assert_eq!(get(&e, "persist", "k", 0, None).await, body);
}

/// Many writers share chunks while others delete them. Every object that
/// survives must remain fully readable (no chunk collected out from under it).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_put_delete_keeps_shared_chunks() {
    let dir = tempfile::tempdir().unwrap();
    let e = Engine::open(config(dir.path())).unwrap();
    e.create_bucket("race").await.unwrap();
    let shared = std::sync::Arc::new(data(120_000, 77));

    let mut tasks = Vec::new();
    for t in 0..8 {
        let (e, shared) = (e.clone(), shared.clone());
        tasks.push(tokio::spawn(async move {
            for i in 0..25 {
                let key = format!("t{t}/k{}", i % 3);
                put(&e, "race", &key, &shared).await;
                if i % 2 == 0 {
                    e.delete_object("race", &key).await.unwrap();
                }
            }
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }

    let page = e
        .list_objects(
            "race",
            ListQuery {
                max_keys: 1000,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(!page.objects.is_empty());
    for o in &page.objects {
        assert_eq!(get(&e, "race", &o.key, 0, None).await, *shared, "{}", o.key);
    }
    let s = e.stats().await.unwrap();
    assert_eq!(s.objects as usize, page.objects.len());
    assert_eq!(
        s.logical_bytes,
        page.objects.len() as u64 * shared.len() as u64
    );
    // All objects share the same chunks.
    assert!(s.unique_bytes <= shared.len() as u64);
}

/// Group commit puts many concurrent commands in one transaction. Conflicting
/// conditional creates inside the same batch must still have exactly one
/// winner, and every independent write must persist.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn group_commit_preserves_per_command_semantics() {
    let dir = tempfile::tempdir().unwrap();
    let e = Engine::open(config(dir.path())).unwrap();
    e.create_bucket("batch").await.unwrap();

    let mut racers = Vec::new();
    for i in 0..64 {
        let e = e.clone();
        racers.push(tokio::spawn(async move {
            let mut w = e.writer();
            w.write(format!("racer {i}").as_bytes()).await.unwrap();
            let cond = WriteCondition {
                if_none_match_any: true,
                ..Default::default()
            };
            e.put_object(
                "batch",
                "lock",
                w.finish().await.unwrap(),
                ObjectAttrs::default(),
                cond,
            )
            .await
            .is_ok()
        }));
    }
    let mut writers = Vec::new();
    for i in 0..200 {
        let e = e.clone();
        writers.push(tokio::spawn(async move {
            put(&e, "batch", &format!("k{i:03}"), format!("v{i}").as_bytes()).await;
        }));
    }
    let wins: usize = futures::future::join_all(racers)
        .await
        .into_iter()
        .map(|r| r.unwrap() as usize)
        .sum();
    for w in writers {
        w.await.unwrap();
    }
    assert_eq!(wins, 1);

    let page = e
        .list_objects(
            "batch",
            ListQuery {
                max_keys: 1000,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(page.objects.len(), 201);
    assert_eq!(e.stats().await.unwrap().objects, 201);
    assert_eq!(get(&e, "batch", "k123", 0, None).await, b"v123");
}
