use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("redis error: {0}")]
    Redis(#[from] redis::RedisError),

    #[error("login failed (status {status})")]
    LoginFailed { status: u16 },

    #[error("non-success status {status} from {endpoint}")]
    BadStatus { endpoint: &'static str, status: u16 },
}

pub type Result<T> = std::result::Result<T, Error>;

impl Error {
    /// HTTP status code if this error carries one, regardless of variant.
    pub fn http_status(&self) -> Option<u16> {
        match self {
            Error::Http(e) => e.status().map(|s| s.as_u16()),
            Error::BadStatus { status, .. } => Some(*status),
            Error::LoginFailed { status } => Some(*status),
            Error::Redis(_) => None,
        }
    }

    /// True if this error is a transport-level failure with no HTTP status
    /// (timeout, connection refused/reset, DNS, TLS handshake).
    pub fn is_transport_error(&self) -> bool {
        match self {
            Error::Http(e) => {
                e.is_timeout() || e.is_connect() || e.is_request() && e.status().is_none()
            }
            _ => false,
        }
    }
}
