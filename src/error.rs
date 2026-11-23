use serde::{ser::SerializeStruct, Serialize, Serializer};

pub type Result<T> = std::result::Result<T, Error>;

/// Errors returned to the frontend by the plugin's commands.
///
/// Serialized as `{ code, message }` so the JS side can turn it into a typed `ReactiveError`.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("unknown reactive source `{0}`")]
    UnknownSource(String),
    #[error("reactive source `{0}` is read-only")]
    ReadOnly(String),
    #[error("reactive source `{0}` is not a state, so it has no current value")]
    NotAState(String),
    #[error("invalid arguments for `{name}`: {message}")]
    InvalidArgs { name: String, message: String },
    #[error("invalid value for `{name}`: {message}")]
    InvalidValue { name: String, message: String },
    #[error("failed to serialize value: {0}")]
    Serialize(#[from] serde_json::Error),
    #[error("access to reactive source `{0}` is not allowed by this window's capabilities")]
    Forbidden(String),
    #[error("reactive source `{0}` is a sink; push to it with `rx.sink()` instead of subscribing")]
    NotSubscribable(String),
    #[error("reactive source `{0}` is not a sink")]
    NotASink(String),
    #[error("no open sink with id {0}")]
    UnknownSink(u64),
    #[error("sink `{0}` has stopped accepting values")]
    SinkClosed(String),
    #[error("{0}")]
    SinkFailed(String),
    #[error("the page that sent this call has been replaced")]
    StalePage,
}

impl Error {
    pub fn code(&self) -> &'static str {
        match self {
            Error::UnknownSource(_) => "UnknownSource",
            Error::ReadOnly(_) => "ReadOnly",
            Error::NotAState(_) => "NotAState",
            Error::InvalidArgs { .. } => "InvalidArgs",
            Error::InvalidValue { .. } => "InvalidValue",
            Error::Serialize(_) => "Serialize",
            Error::Forbidden(_) => "Forbidden",
            Error::NotSubscribable(_) => "NotSubscribable",
            Error::NotASink(_) => "NotASink",
            Error::UnknownSink(_) => "UnknownSink",
            Error::SinkClosed(_) => "SinkClosed",
            Error::SinkFailed(_) => "SinkFailed",
            Error::StalePage => "StalePage",
        }
    }
}

impl Serialize for Error {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        let mut s = serializer.serialize_struct("Error", 2)?;
        s.serialize_field("code", self.code())?;
        s.serialize_field("message", &self.to_string())?;
        s.end()
    }
}
