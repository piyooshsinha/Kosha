use axum::http::{HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};

use crate::xml::escape;

/// An S3 error response.
#[derive(Debug)]
pub struct S3Error {
    pub status: StatusCode,
    pub code: &'static str,
    pub message: String,
    pub resource: Option<String>,
    pub request_id: Option<String>,
    /// Extra response headers (e.g. `x-amz-delete-marker`).
    pub headers: Vec<(&'static str, String)>,
}

pub type S3Result<T> = Result<T, S3Error>;

impl S3Error {
    pub fn new(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
        S3Error {
            status,
            code,
            message: message.into(),
            resource: None,
            request_id: None,
            headers: Vec::new(),
        }
    }

    pub fn invalid_argument(msg: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "InvalidArgument", msg)
    }

    pub fn invalid_request(msg: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "InvalidRequest", msg)
    }

    pub fn malformed_xml(msg: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "MalformedXML", msg)
    }

    pub fn access_denied(msg: impl Into<String>) -> Self {
        Self::new(StatusCode::FORBIDDEN, "AccessDenied", msg)
    }

    pub fn signature_mismatch() -> Self {
        Self::new(
            StatusCode::FORBIDDEN,
            "SignatureDoesNotMatch",
            "The request signature we calculated does not match the signature you provided.",
        )
    }

    pub fn not_implemented(what: &str) -> Self {
        Self::new(
            StatusCode::NOT_IMPLEMENTED,
            "NotImplemented",
            format!("{what} is not implemented"),
        )
    }

    pub fn internal(msg: impl Into<String>) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, "InternalError", msg)
    }

    pub fn with_resource(mut self, r: impl Into<String>) -> Self {
        self.resource = Some(r.into());
        self
    }
}

impl From<kosha_engine::Error> for S3Error {
    fn from(e: kosha_engine::Error) -> Self {
        use kosha_engine::Error as E;
        let msg = e.to_string();
        let (status, code) = match e {
            E::NoSuchBucket => (StatusCode::NOT_FOUND, "NoSuchBucket"),
            E::BucketAlreadyExists => (StatusCode::CONFLICT, "BucketAlreadyOwnedByYou"),
            E::BucketNotEmpty => (StatusCode::CONFLICT, "BucketNotEmpty"),
            E::InvalidBucketName(_) => (StatusCode::BAD_REQUEST, "InvalidBucketName"),
            E::NoSuchKey => (StatusCode::NOT_FOUND, "NoSuchKey"),
            E::InvalidKey(_) => (StatusCode::BAD_REQUEST, "KeyTooLongError"),
            E::NoSuchUpload => (StatusCode::NOT_FOUND, "NoSuchUpload"),
            E::InvalidPart(_) => (StatusCode::BAD_REQUEST, "InvalidPart"),
            E::InvalidPartOrder => (StatusCode::BAD_REQUEST, "InvalidPartOrder"),
            E::EntityTooSmall => (StatusCode::BAD_REQUEST, "EntityTooSmall"),
            E::PreconditionFailed => (StatusCode::PRECONDITION_FAILED, "PreconditionFailed"),
            E::BadDigest => (StatusCode::BAD_REQUEST, "BadDigest"),
            E::Body(_) => (StatusCode::BAD_REQUEST, "IncompleteBody"),
            E::NoSuchVersion => (StatusCode::NOT_FOUND, "NoSuchVersion"),
            E::InvalidRequest(_) => (StatusCode::BAD_REQUEST, "InvalidRequest"),
            E::InvalidBucketState(_) => (StatusCode::CONFLICT, "InvalidBucketState"),
            E::AccessDenied(_) => (StatusCode::FORBIDDEN, "AccessDenied"),
            E::MethodNotAllowed(_) => (StatusCode::METHOD_NOT_ALLOWED, "MethodNotAllowed"),
            E::NoSuchUser => (StatusCode::NOT_FOUND, "NoSuchEntity"),
            E::UserAlreadyExists(_) => (StatusCode::CONFLICT, "EntityAlreadyExists"),
            E::Corrupt(_) | E::Io(_) | E::Meta(_) | E::Internal(_) => {
                tracing::error!(error = %msg, "internal storage error");
                (StatusCode::INTERNAL_SERVER_ERROR, "InternalError")
            }
        };
        S3Error::new(status, code, msg)
    }
}

impl IntoResponse for S3Error {
    fn into_response(self) -> Response {
        let body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>{}</Code><Message>{}</Message>{}<RequestId>{}</RequestId></Error>",
            self.code,
            escape(&self.message),
            self.resource
                .as_deref()
                .map(|r| format!("<Resource>{}</Resource>", escape(r)))
                .unwrap_or_default(),
            escape(self.request_id.as_deref().unwrap_or_default()),
        );
        let mut resp = (self.status, body).into_response();
        for (k, v) in &self.headers {
            if let Ok(v) = axum::http::HeaderValue::from_str(v) {
                resp.headers_mut().insert(*k, v);
            }
        }
        resp.headers_mut()
            .insert("content-type", HeaderValue::from_static("application/xml"));
        resp
    }
}
