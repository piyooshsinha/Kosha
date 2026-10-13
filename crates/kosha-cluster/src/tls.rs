//! TLS for the S3 endpoint and mutual TLS between cluster nodes.
//!
//! [`TlsListener`] plugs into `axum::serve`: it accepts TCP connections and
//! runs handshakes concurrently, so a slow client cannot stall others.

use std::io;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::WebPkiClientVerifier;
use rustls::{ClientConfig, RootCertStore, ServerConfig};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::server::TlsStream;

fn provider() -> Arc<rustls::crypto::CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

fn err(path: &Path, what: &str, e: impl std::fmt::Display) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        format!("{}: {what}: {e}", path.display()),
    )
}

pub fn load_certs(path: &Path) -> io::Result<Vec<CertificateDer<'static>>> {
    let certs = CertificateDer::pem_file_iter(path)
        .map_err(|e| err(path, "reading certificates", e))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| err(path, "parsing certificates", e))?;
    if certs.is_empty() {
        return Err(err(
            path,
            "reading certificates",
            "no PEM certificates found",
        ));
    }
    Ok(certs)
}

pub fn load_key(path: &Path) -> io::Result<PrivateKeyDer<'static>> {
    PrivateKeyDer::from_pem_file(path).map_err(|e| err(path, "reading private key", e))
}

fn roots(ca: &Path) -> io::Result<RootCertStore> {
    let mut store = RootCertStore::empty();
    for c in load_certs(ca)? {
        store
            .add(c)
            .map_err(|e| err(ca, "adding CA certificate", e))?;
    }
    Ok(store)
}

/// Certificate files for one endpoint.
#[derive(Clone, Debug)]
pub struct TlsFiles {
    pub cert: std::path::PathBuf,
    pub key: std::path::PathBuf,
    /// CA that signs peer certificates. On a server this turns on client
    /// certificate verification (mutual TLS).
    pub ca: Option<std::path::PathBuf>,
}

impl TlsFiles {
    pub fn server_config(&self) -> io::Result<Arc<ServerConfig>> {
        let builder = ServerConfig::builder_with_provider(provider())
            .with_safe_default_protocol_versions()
            .map_err(io::Error::other)?;
        let builder = match &self.ca {
            Some(ca) => {
                let verifier =
                    WebPkiClientVerifier::builder_with_provider(Arc::new(roots(ca)?), provider())
                        .build()
                        .map_err(|e| err(ca, "client verifier", e))?;
                builder.with_client_cert_verifier(verifier)
            }
            None => builder.with_no_client_auth(),
        };
        let mut cfg = builder
            .with_single_cert(load_certs(&self.cert)?, load_key(&self.key)?)
            .map_err(|e| err(&self.cert, "certificate and key", e))?;
        // axum is built with HTTP/1 only.
        cfg.alpn_protocols = vec![b"http/1.1".to_vec()];
        Ok(Arc::new(cfg))
    }

    /// Client side of mutual TLS: present our certificate and trust only
    /// peers signed by the CA. Server names (DNS or IP SANs) are verified.
    pub fn client_config(&self) -> io::Result<ClientConfig> {
        let ca = self
            .ca
            .as_ref()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "cluster TLS needs a CA"))?;
        let mut cfg = ClientConfig::builder_with_provider(provider())
            .with_safe_default_protocol_versions()
            .map_err(io::Error::other)?
            .with_root_certificates(roots(ca)?)
            .with_client_auth_cert(load_certs(&self.cert)?, load_key(&self.key)?)
            .map_err(|e| err(&self.cert, "client certificate and key", e))?;
        cfg.alpn_protocols = vec![b"http/1.1".to_vec()];
        Ok(cfg)
    }
}

/// A TLS listener for `axum::serve`.
pub struct TlsListener {
    rx: mpsc::Receiver<(TlsStream<TcpStream>, SocketAddr)>,
    local: SocketAddr,
}

impl TlsListener {
    pub fn new(tcp: TcpListener, config: Arc<ServerConfig>) -> io::Result<Self> {
        let local = tcp.local_addr()?;
        let acceptor = TlsAcceptor::from(config);
        let (tx, rx) = mpsc::channel(256);
        tokio::spawn(async move {
            loop {
                let (sock, addr) = match tcp.accept().await {
                    Ok(c) => c,
                    Err(e) => {
                        // Typically EMFILE; back off instead of spinning.
                        tracing::warn!(error = %e, "accept failed");
                        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                        continue;
                    }
                };
                let _ = sock.set_nodelay(true);
                if tx.is_closed() {
                    break;
                }
                let (acceptor, tx) = (acceptor.clone(), tx.clone());
                tokio::spawn(async move {
                    let hs = tokio::time::timeout(
                        std::time::Duration::from_secs(10),
                        acceptor.accept(sock),
                    );
                    match hs.await {
                        Ok(Ok(stream)) => {
                            let _ = tx.send((stream, addr)).await;
                        }
                        Ok(Err(e)) => tracing::debug!(%addr, error = %e, "TLS handshake failed"),
                        Err(_) => tracing::debug!(%addr, "TLS handshake timed out"),
                    }
                });
            }
        });
        Ok(TlsListener { rx, local })
    }
}

impl axum::serve::Listener for TlsListener {
    type Io = TlsStream<TcpStream>;
    type Addr = SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        match self.rx.recv().await {
            Some(c) => c,
            // The accept task only ends once the receiver is gone.
            None => std::future::pending().await,
        }
    }

    fn local_addr(&self) -> io::Result<Self::Addr> {
        Ok(self.local)
    }
}
