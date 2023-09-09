use std::fmt::Display;

use datafusion::error::DataFusionError;

#[derive(Debug)]
/// An error that occurs when a region is invalid.
pub struct InvalidRegionError;

impl std::error::Error for InvalidRegionError {}

impl Display for InvalidRegionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "the supplied region is invalid")
    }
}

impl From<InvalidRegionError> for DataFusionError {
    fn from(e: InvalidRegionError) -> Self {
        DataFusionError::Execution(e.to_string())
    }
}

impl From<InvalidRegionError> for std::fmt::Error {
    fn from(_: InvalidRegionError) -> Self {
        std::fmt::Error
    }
}
