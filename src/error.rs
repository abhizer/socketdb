use sqlparser::parser::ParserError;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid meta command `{0}`")]
    InvalidMetaCommand(String),
    #[error("io error: `{0}`")]
    IOError(String),
    #[error("error deserializing the database from disk: `{0}`")]
    DeserializingError(String),
    #[error("`{0}`")]
    ParsingError(ParserError),
    #[error("invalid query: `{0}` not supported")]
    InvalidQuery(String),
    #[error("invalid query: Column `{col}` not found in table `{table}`")]
    ColumnNotFound {
        col: String,
        table: String,
    },
    #[error("invalid operation: `{0}` not allowed")]
    InvalidOperation(String),
    #[error("invalid query: table `{0}` not found")]
    TableNotFound(String),
    #[error("invalid query: table `{0}` already exists")]
    TableAlreadyExists(String),
    #[error("unsupported feature: `{0}`")]
    Unsupported(String),
    #[error("evaluation error: `{0}`")]
    EvaluationError(String),
    #[error("unknown error")]
    Unknown,
}

impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Self::IOError(value.to_string())
    }
}

impl From<bincode::Error> for Error {
    fn from(value: bincode::Error) -> Self {
        Self::DeserializingError(value.to_string())
    }
}

impl From<ParserError> for Error {
    fn from(value: ParserError) -> Self {
        Self::ParsingError(value)
    }
}
