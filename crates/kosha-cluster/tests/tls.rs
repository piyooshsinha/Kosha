//! Mutual TLS between cluster nodes.

use std::path::Path;
use std::time::Duration;

use futures::TryStreamExt;
use kosha_cluster::tls::TlsFiles;
use kosha_cluster::{Cluster, ClusterConfig};
use kosha_engine::*;
use rcgen::{
    BasicConstraints, CertificateParams, CertifiedIssuer, ExtendedKeyUsagePurpose, IsCa, KeyPair,
};

/// Writes `<name>.pem` / `<name>.key` signed by `ca`, valid for 127.0.0.1.
fn leaf(dir: &Path, name: &str, ca: &CertifiedIssuer<'_, KeyPair>) -> TlsFiles {
    let key = KeyPair::generate().unwrap();
    let mut p = CertificateParams::new(vec!["127.0.0.1".into(), "localhost".into()]).unwrap();
    p.extended_key_usages = vec![
        ExtendedKeyUsagePurpose::ServerAuth,
        ExtendedKeyUsagePurpose::ClientAuth,
    ];
    let cert = p.signed_by(&key, ca).unwrap();
    let (c, k) = (
        dir.join(format!("{name}.pem")),
        dir.join(format!("{name}.key")),
    );
    std::fs::write(&c, cert.pem()).unwrap();
    std::fs::write(&k, key.serialize_pem()).unwrap();
    TlsFiles {
        cert: c,
        key: k,
        ca: Some(dir.join("ca.pem")),
    }
}

fn ca(dir: &Path, file: &str) -> CertifiedIssuer<'static, KeyPair> {
    let mut p = CertificateParams::new(Vec::<String>::new()).unwrap();
    p.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let ca = CertifiedIssuer::self_signed(p, KeyPair::generate().unwrap()).unwrap();
    std::fs::write(dir.join(file), ca.pem()).unwrap();
    ca
}

struct Node {
    engine: Engine,
    cluster: Cluster,
    addr: String,
    _dir: tempfile::TempDir,
}

async fn start_node(id: u64, tls: TlsFiles) -> Node {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = EngineConfig::new(dir.path().join("data"));
    cfg.sync = SyncMode::None;
    let engine = Engine::open(cfg).unwrap();
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap();
    let mut ccfg = ClusterConfig::new(id, port, "test-secret", dir.path().join("raft"));
    ccfg.heartbeat_ms = 50;
    ccfg.election_timeout_ms = (300, 600);
    ccfg.tls = Some(tls);
    let cluster = Cluster::start(engine.clone(), ccfg).await.unwrap();
    Node {
        engine,
        cluster,
        addr: port.to_string(),
        _dir: dir,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_over_mutual_tls() {
    let certs = tempfile::tempdir().unwrap();
    let good = ca(certs.path(), "ca.pem");
    let n1 = start_node(1, leaf(certs.path(), "n1", &good)).await;
    let n2 = start_node(2, leaf(certs.path(), "n2", &good)).await;
    n1.cluster.initialize().await.unwrap();
    n1.cluster
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();
    n2.cluster.join(&n1.addr).await.unwrap();
    n2.cluster
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();

    // Metadata and chunks travel over TLS.
    n2.engine.create_bucket("secure").await.unwrap();
    let body: Vec<u8> = (0..300_000u32).map(|i| (i * 7 % 251) as u8).collect();
    let mut w = n2.engine.writer();
    w.write(&body).await.unwrap();
    let meta = n2
        .engine
        .put_object(
            "secure",
            "k",
            w.finish().await.unwrap(),
            ObjectAttrs::default(),
            WriteCondition::default(),
        )
        .await
        .unwrap();
    let parts: Vec<_> = n1
        .engine
        .read(&meta, 0, meta.size)
        .try_collect()
        .await
        .unwrap();
    assert_eq!(parts.concat(), body);

    // A node whose certificate another CA signed is refused, even with the
    // right shared secret.
    let rogue_dir = tempfile::tempdir().unwrap();
    let evil = ca(rogue_dir.path(), "ca.pem");
    let mut rogue_files = leaf(rogue_dir.path(), "rogue", &evil);
    // It trusts the real CA, so only our side's verification stops it.
    std::fs::copy(certs.path().join("ca.pem"), rogue_dir.path().join("ca.pem")).unwrap();
    rogue_files.ca = Some(rogue_dir.path().join("ca.pem"));
    let rogue = start_node(3, rogue_files).await;
    let joined = tokio::time::timeout(Duration::from_secs(5), rogue.cluster.join(&n1.addr)).await;
    assert!(
        !matches!(joined, Ok(Ok(()))),
        "rogue node joined: {joined:?}"
    );
    assert_eq!(n1.cluster.status().nodes.len(), 2);
}
