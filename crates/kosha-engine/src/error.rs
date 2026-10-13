use thiserror::Error;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("bucket does not exist")]
    NoSuchBucket,
    #[error("bucket already exists")]
    BucketAlreadyExists,
    #[error("bucket is not empty")]
    BucketNotEmpty,
    #[error("invalid bucket name: {0}")]
    InvalidBucketName(String),
    #[error("key does not exist")]
    NoSuchKey,
    #[error("invalid object key: {0}")]
    InvalidKey(String),
    #[error("multipart upload does not exist")]
    NoSuchUpload,
    #[error("invalid part: {0}")]
    InvalidPart(String),
    #[error("parts must be listed in ascending order")]
    InvalidPartOrder,
    #[error("part is smaller than the minimum allowed size")]
    EntityTooSmall,
    #[error("precondition failed")]
    PreconditionFailed,
    #[error("the specified version does not exist")]
    NoSuchVersion,
    #[error("{0}")]
    InvalidRequest(String),
    #[error("{0}")]
    InvalidBucketState(String),
    #[error("{0}")]
    AccessDenied(String),
    #[error("{0}")]
    MethodNotAllowed(String),
    #[error("the user does not exist")]
    NoSuchUser,
    #[error("{0}")]
    UserAlreadyExists(String),
    #[error("content digest does not match")]
    BadDigest,
    #[error("data corruption detected: {0}")]
    Corrupt(String),
    #[error("upload body error: {0}")]
    Body(String),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("metadata store error: {0}")]
    Meta(String),
    #[error("internal error: {0}")]
    Internal(String),
}

macro_rules! from_meta_err {
    ($($t:ty),*) => {$(
        impl From<$t> for Error {
            fn from(e: $t) -> Self { Error::Meta(e.to_string()) }
        }
    )*};
}

from_meta_err!(
    redb::Error,
    redb::DatabaseError,
    redb::TransactionError,
    redb::TableError,
    redb::StorageError,
    redb::CommitError,
    rmp_serde::encode::Error,
    rmp_serde::decode::Error
);

impl From<tokio::task::JoinError> for Error {
    fn from(e: tokio::task::JoinError) -> Self {
        Error::Internal(format!("background task failed: {e}"))
    }
}
