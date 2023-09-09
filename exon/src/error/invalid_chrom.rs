use std::fmt::Display;

use datafusion::error::DataFusionError;

#[derive(Debug, Default)]
/// An error that occurs when an interval is invalid.
pub struct InvalidChromError;

impl std::error::Error for InvalidChromError {}

impl Display for InvalidChromError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "the supplied interval is invalid")
    }
}

impl From<InvalidChromError> for DataFusionError {
    fn from(e: InvalidChromError) -> Self {
        DataFusionError::Execution(e.to_string())
    }
}

impl From<InvalidChromError> for std::fmt::Error {
    fn from(_: InvalidChromError) -> Self {
        std::fmt::Error
    }
}
