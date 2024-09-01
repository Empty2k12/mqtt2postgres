//! Errors that might happen during execution

use thiserror::Error;

#[derive(Debug, Eq, PartialEq, Error)]
#[non_exhaustive]
pub enum Error {
    #[error("query is invalid: {error}")]
    /// Error happens when a query is invalid
    InvalidQueryError { error: String },

    #[error("connection error: {error}")]
    /// Error happens when there is a connection error to Postgres
    ConnectionError { error: String },

    #[error("json error: {error}")]
    /// Error happens when there is a JSON error
    JSONError { error: String },
}
