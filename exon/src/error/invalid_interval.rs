use std::fmt::Display;

use datafusion::error::DataFusionError;

#[derive(Debug, Default)]
/// An error that occurs when an interval is invalid.
pub struct InvalidIntervalError;

impl std::error::Error for InvalidIntervalError {}

impl Display for InvalidIntervalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "the supplied interval is invalid")
    }
}

impl From<InvalidIntervalError> for DataFusionError {
    fn from(e: InvalidIntervalError) -> Self {
        DataFusionError::Execution(e.to_string())
    }
}

impl From<InvalidIntervalError> for std::fmt::Error {
    fn from(_: InvalidIntervalError) -> Self {
        std::fmt::Error
    }
}
