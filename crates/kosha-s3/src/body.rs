//! Request body ingestion: decodes `aws-chunked` framing, verifies chunk
//! signatures, payload hashes and CRC32 checksums while streaming, so the
//! body is never buffered whole.

use axum::body::Body;
use axum::http::HeaderMap;
use base64::Engine as _;
use bytes::{Buf, Bytes, BytesMut};
use futures::stream::{BoxStream, StreamExt};
use sha2::{Digest, Sha256};

use crate::auth::{ChunkSigner, Payload};
use crate::error::{S3Error, S3Result};

const MAX_CHUNK_HEADER: usize = 4096;

fn b64() -> base64::engine::GeneralPurpose {
    base64::engine::general_purpose::STANDARD
}

fn bad_digest(what: &str) -> S3Error {
    S3Error::new(
        axum::http::StatusCode::BAD_REQUEST,
        "BadDigest",
        format!("The {what} you specified did not match what we received."),
    )
}

fn incomplete(msg: &str) -> S3Error {
    S3Error::new(axum::http::StatusCode::BAD_REQUEST, "IncompleteBody", msg)
}

// One per request, so the size difference between variants does not matter.
#[allow(clippy::large_enum_variant)]
enum Framing {
    Raw,
    Chunked {
        signer: Option<ChunkSigner>,
        buf: BytesMut,
        /// Bytes left in the current chunk's data section.
        remaining: usize,
        /// Signature of the chunk currently being read (signed mode).
        signature: Option<String>,
        /// Data of the current chunk, needed to verify its signature.
        current: BytesMut,
        done: bool,
        trailers: Vec<(String, String)>,
    },
}

pub struct BodySource {
    inner: BoxStream<'static, Result<Bytes, axum::Error>>,
    framing: Framing,
    sha256: Option<(Sha256, String)>,
    crc32: Option<(crc32fast::Hasher, Option<String>)>,
    received: u64,
}

impl BodySource {
    pub fn new(body: Body, payload: Payload, headers: &HeaderMap) -> S3Result<Self> {
        let header = |n: &str| headers.get(n).and_then(|v| v.to_str().ok());
        let chunked = payload.is_chunked()
            || header("content-encoding").is_some_and(|v| v.contains("aws-chunked"));

        let framing = if chunked {
            Framing::Chunked {
                signer: match &payload {
                    Payload::StreamingSigned(s) => Some(s.clone()),
                    _ => None,
                },
                buf: BytesMut::new(),
                remaining: 0,
                signature: None,
                current: BytesMut::new(),
                done: false,
                trailers: Vec::new(),
            }
        } else {
            Framing::Raw
        };

        let sha256 = match payload {
            Payload::Sha256(h) => Some((Sha256::new(), h)),
            _ => None,
        };

        // CRC32 may arrive as a header, or be announced as a trailer.
        let crc32 = if let Some(v) = header("x-amz-checksum-crc32") {
            Some((crc32fast::Hasher::new(), Some(v.to_string())))
        } else if header("x-amz-trailer")
            .is_some_and(|t| t.split(',').any(|n| n.trim() == "x-amz-checksum-crc32"))
        {
            Some((crc32fast::Hasher::new(), None))
        } else {
            None
        };

        Ok(BodySource {
            inner: body.into_data_stream().boxed(),
            framing,
            sha256,
            crc32,
            received: 0,
        })
    }

    async fn pull(&mut self) -> S3Result<Option<Bytes>> {
        match self.inner.next().await {
            Some(Ok(b)) => Ok(Some(b)),
            Some(Err(e)) => Err(incomplete(&format!("error reading request body: {e}"))),
            None => Ok(None),
        }
    }

    /// Next piece of decoded payload, or `None` at the end (after all
    /// integrity checks have passed).
    pub async fn next(&mut self) -> S3Result<Option<Bytes>> {
        let data = match self.framing {
            Framing::Raw => self.pull().await?,
            Framing::Chunked { .. } => self.next_chunked().await?,
        };
        match data {
            Some(d) => {
                self.received += d.len() as u64;
                if let Some((h, _)) = &mut self.sha256 {
                    h.update(&d);
                }
                if let Some((h, _)) = &mut self.crc32 {
                    h.update(&d);
                }
                Ok(Some(d))
            }
            None => {
                self.verify_end()?;
                Ok(None)
            }
        }
    }

    /// Read the remaining body into memory, up to `limit` bytes.
    pub async fn collect(mut self, limit: usize) -> S3Result<Bytes> {
        let mut out = BytesMut::new();
        while let Some(d) = self.next().await? {
            if out.len() + d.len() > limit {
                return Err(S3Error::new(
                    axum::http::StatusCode::BAD_REQUEST,
                    "EntityTooLarge",
                    "request body is too large",
                ));
            }
            out.extend_from_slice(&d);
        }
        Ok(out.freeze())
    }

    fn verify_end(&mut self) -> S3Result<()> {
        if let Some((h, want)) = self.sha256.take()
            && hex::encode(h.finalize()) != want
        {
            return Err(S3Error::new(
                axum::http::StatusCode::BAD_REQUEST,
                "XAmzContentSHA256Mismatch",
                "The provided 'x-amz-content-sha256' header does not match what was computed.",
            ));
        }
        if let Some((h, want)) = self.crc32.take() {
            let want = want.or_else(|| match &self.framing {
                Framing::Chunked { trailers, .. } => trailers
                    .iter()
                    .find(|(k, _)| k == "x-amz-checksum-crc32")
                    .map(|(_, v)| v.clone()),
                Framing::Raw => None,
            });
            if let Some(want) = want
                && b64().encode(h.finalize().to_be_bytes()) != want
            {
                return Err(bad_digest("CRC32 checksum"));
            }
        }
        Ok(())
    }

    async fn next_chunked(&mut self) -> S3Result<Option<Bytes>> {
        loop {
            let Framing::Chunked {
                signer,
                buf,
                remaining,
                signature,
                current,
                done,
                trailers,
            } = &mut self.framing
            else {
                unreachable!()
            };
            if *done {
                return Ok(None);
            }

            if *remaining > 0 {
                if buf.is_empty() {
                    let more = self
                        .pull()
                        .await?
                        .ok_or_else(|| incomplete("truncated chunk"))?;
                    let Framing::Chunked { buf, .. } = &mut self.framing else {
                        unreachable!()
                    };
                    buf.extend_from_slice(&more);
                    continue;
                }
                let take = (*remaining).min(buf.len());
                let piece = buf.split_to(take).freeze();
                *remaining -= take;
                if signer.is_some() {
                    current.extend_from_slice(&piece);
                }
                return Ok(Some(piece));
            }

            // Between chunks: finish the previous chunk, then parse a header.
            if let Some(sig) = signature.take() {
                // Need the CRLF that terminates the data section.
                if buf.len() < 2 {
                    let more = self
                        .pull()
                        .await?
                        .ok_or_else(|| incomplete("truncated chunk"))?;
                    let Framing::Chunked { buf, signature, .. } = &mut self.framing else {
                        unreachable!()
                    };
                    buf.extend_from_slice(&more);
                    *signature = Some(sig);
                    continue;
                }
                if &buf[..2] != b"\r\n" {
                    return Err(incomplete("malformed aws-chunked framing"));
                }
                buf.advance(2);
                if let Some(s) = signer {
                    s.verify(current, &sig)?;
                }
                current.clear();
            }

            let Some(pos) = buf.windows(2).position(|w| w == b"\r\n") else {
                if buf.len() > MAX_CHUNK_HEADER {
                    return Err(incomplete("aws-chunked header too long"));
                }
                match self.pull().await? {
                    Some(more) => {
                        let Framing::Chunked { buf, .. } = &mut self.framing else {
                            unreachable!()
                        };
                        buf.extend_from_slice(&more);
                        continue;
                    }
                    None => return Err(incomplete("truncated aws-chunked body")),
                }
            };
            let line = String::from_utf8_lossy(&buf[..pos]).into_owned();
            buf.advance(pos + 2);

            let (size_hex, ext) = line.split_once(';').unwrap_or((line.as_str(), ""));
            let size = usize::from_str_radix(size_hex.trim(), 16)
                .map_err(|_| incomplete("invalid aws-chunked chunk size"))?;
            let sig = ext
                .split(';')
                .find_map(|e| e.trim().strip_prefix("chunk-signature="))
                .map(str::to_string);
            if signer.is_some() && sig.is_none() {
                return Err(S3Error::signature_mismatch());
            }

            if size == 0 {
                if let (Some(s), Some(sig)) = (signer.as_mut(), sig) {
                    s.verify(b"", &sig)?;
                }
                // Trailer lines until a blank line or end of stream.
                let mut rest = std::mem::take(buf);
                while let Some(more) = self.pull().await? {
                    rest.extend_from_slice(&more);
                }
                let Framing::Chunked {
                    done, trailers: tr, ..
                } = &mut self.framing
                else {
                    unreachable!()
                };
                for line in String::from_utf8_lossy(&rest).split("\r\n") {
                    if let Some((k, v)) = line.split_once(':') {
                        tr.push((k.trim().to_ascii_lowercase(), v.trim().to_string()));
                    }
                }
                *done = true;
                return Ok(None);
            }

            *remaining = size;
            *signature = Some(sig.unwrap_or_default());
            let _ = trailers;
        }
    }
}

/// Decode a `Content-MD5` header.
pub fn content_md5(headers: &HeaderMap) -> S3Result<Option<[u8; 16]>> {
    let Some(v) = headers.get("content-md5") else {
        return Ok(None);
    };
    let raw = b64().decode(v.as_bytes()).map_err(|_| {
        S3Error::new(
            axum::http::StatusCode::BAD_REQUEST,
            "InvalidDigest",
            "Content-MD5 is not valid base64",
        )
    })?;
    let arr: [u8; 16] = raw.try_into().map_err(|_| {
        S3Error::new(
            axum::http::StatusCode::BAD_REQUEST,
            "InvalidDigest",
            "Content-MD5 must be 16 bytes",
        )
    })?;
    Ok(Some(arr))
}

pub fn check_md5(expected: Option<[u8; 16]>, actual: &[u8; 16]) -> S3Result<()> {
    match expected {
        Some(e) if &e != actual => Err(bad_digest("Content-MD5")),
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn decode(raw: &'static [u8], split: usize, headers: HeaderMap) -> S3Result<Vec<u8>> {
        let pieces: Vec<Result<Bytes, std::io::Error>> = raw
            .chunks(split)
            .map(|c| Ok(Bytes::from_static(c)))
            .collect();
        let body = Body::from_stream(futures::stream::iter(pieces));
        let mut src = BodySource::new(body, Payload::StreamingUnsigned, &headers)?;
        let mut out = Vec::new();
        while let Some(d) = src.next().await? {
            out.extend_from_slice(&d);
        }
        Ok(out)
    }

    #[tokio::test]
    async fn decodes_unsigned_chunked_with_trailer() {
        let raw: &[u8] = b"5\r\nhello\r\n6\r\n world\r\n0\r\nx-amz-checksum-crc32:DUoRhQ==\r\n\r\n";
        let mut h = HeaderMap::new();
        h.insert("x-amz-trailer", "x-amz-checksum-crc32".parse().unwrap());
        for split in [1, 3, 7, 100] {
            assert_eq!(decode(raw, split, h.clone()).await.unwrap(), b"hello world");
        }
    }

    #[tokio::test]
    async fn rejects_bad_crc_trailer() {
        let raw: &[u8] = b"5\r\nhello\r\n0\r\nx-amz-checksum-crc32:AAAAAA==\r\n\r\n";
        let mut h = HeaderMap::new();
        h.insert("x-amz-trailer", "x-amz-checksum-crc32".parse().unwrap());
        assert_eq!(decode(raw, 4, h).await.unwrap_err().code, "BadDigest");
    }

    /// The streaming upload example from the AWS SigV4 documentation:
    /// 64 KiB + 1 KiB of 'a', signed in a chain seeded by the request signature.
    fn aws_example(tamper: bool) -> Vec<u8> {
        let mut raw = Vec::new();
        let chunk1 = vec![b'a'; 65536];
        let mut chunk2 = vec![b'a'; 1024];
        if tamper {
            chunk2[10] = b'b';
        }
        raw.extend_from_slice(b"10000;chunk-signature=ad80c730a21e5b8d04586a2213dd63b9a0e99e0e2307b0ade35a65485a288648\r\n");
        raw.extend_from_slice(&chunk1);
        raw.extend_from_slice(b"\r\n400;chunk-signature=0055627c9e194cb4542bae2aa5492e3c1575bbb81b612b7d234b86a503ef5497\r\n");
        raw.extend_from_slice(&chunk2);
        raw.extend_from_slice(b"\r\n0;chunk-signature=b6c6ea8a5354eaf15b3cb7646744f4275b71ea724fed81ceb9323e279d449df9\r\n\r\n");
        raw
    }

    async fn decode_signed(raw: Vec<u8>, split: usize) -> S3Result<usize> {
        let pieces: Vec<Result<Bytes, std::io::Error>> = raw
            .chunks(split)
            .map(|c| Ok(Bytes::copy_from_slice(c)))
            .collect();
        let body = Body::from_stream(futures::stream::iter(pieces));
        let signer = crate::auth::ChunkSigner::for_test(
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            "20130524T000000Z",
            "us-east-1",
            "4f232c4386841ef735655705268965c44a0e4690baa4adea153f7db9fa80a0a9",
        );
        let mut src = BodySource::new(body, Payload::StreamingSigned(signer), &HeaderMap::new())?;
        let mut n = 0;
        while let Some(d) = src.next().await? {
            n += d.len();
        }
        Ok(n)
    }

    #[tokio::test]
    async fn verifies_signed_chunk_chain() {
        for split in [1000, 4096, 70_000] {
            assert_eq!(
                decode_signed(aws_example(false), split).await.unwrap(),
                66560
            );
        }
        assert_eq!(
            decode_signed(aws_example(true), 4096)
                .await
                .unwrap_err()
                .code,
            "SignatureDoesNotMatch"
        );
    }

    #[tokio::test]
    async fn rejects_truncated_body() {
        let raw: &[u8] = b"a\r\nhello";
        assert_eq!(
            decode(raw, 4, HeaderMap::new()).await.unwrap_err().code,
            "IncompleteBody"
        );
    }
}
