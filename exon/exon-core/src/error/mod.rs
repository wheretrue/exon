// Copyright 2023 WHERE TRUE Technologies.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{error::Error, fmt::Display, str::Utf8Error};

use arrow::error::ArrowError;
use datafusion::{error::DataFusionError, sql::sqlparser::parser::ParserError};
use exon_gff::ExonGFFError;
use noodles::bgzf::virtual_position::TryFromU64U16TupleError;

use self::invalid_chrom::InvalidRegionNameError;

/// Error for an invalid region.
pub mod invalid_region;

/// Error for an invalid interval.
pub mod invalid_interval;

/// Error for an invalid chromosome.
pub mod invalid_chrom;

/// Possible errors for Exon.
#[derive(Debug)]
pub enum ExonError {
    /// Error from the DataFusion package.
    DataFusionError(DataFusionError),

    /// Error from the Arrow package.
    ArrowError(ArrowError),

    /// Execution error
    ExecutionError(String),

    /// Object store error
    ObjectStoreError(object_store::Error),

    /// Noodles error
    NoodlesError(noodles::core::Error),

    /// IO error
    IOError(std::io::Error),

    /// Invalid File Type
    InvalidFileType(String),

    /// Invalid Configuration
    Configuration(String),

    /// Invalid GFF error
    ExonGFFError(ExonGFFError),

    /// SQL Parser error
    ParserError(String),

    /// Unsupported function
    UnsupportedFunction(String),
}

impl From<DataFusionError> for ExonError {
    fn from(error: DataFusionError) -> Self {
        ExonError::DataFusionError(error)
    }
}

impl From<ArrowError> for ExonError {
    fn from(error: ArrowError) -> Self {
        ExonError::ArrowError(error)
    }
}

impl From<noodles::core::Error> for ExonError {
    fn from(error: noodles::core::Error) -> Self {
        ExonError::NoodlesError(error)
    }
}

impl From<std::io::Error> for ExonError {
    fn from(error: std::io::Error) -> Self {
        ExonError::IOError(error)
    }
}

impl From<object_store::Error> for ExonError {
    fn from(error: object_store::Error) -> Self {
        ExonError::ObjectStoreError(error)
    }
}

impl From<object_store::path::Error> for ExonError {
    fn from(error: object_store::path::Error) -> Self {
        ExonError::ObjectStoreError(error.into())
    }
}

impl From<TryFromU64U16TupleError> for ExonError {
    fn from(_error: TryFromU64U16TupleError) -> Self {
        ExonError::ExecutionError("Error creating virtual position".to_string())
    }
}

impl From<noodles::core::region::ParseError> for ExonError {
    fn from(_error: noodles::core::region::ParseError) -> Self {
        ExonError::ExecutionError("Error parsing region".to_string())
    }
}

impl From<InvalidRegionNameError> for ExonError {
    fn from(error: InvalidRegionNameError) -> Self {
        ExonError::ExecutionError(format!("Error parsing region: {}", error))
    }
}

impl From<Utf8Error> for ExonError {
    fn from(error: Utf8Error) -> Self {
        ExonError::ExecutionError(format!("Error parsing string: {}", error))
    }
}

impl From<url::ParseError> for ExonError {
    fn from(error: url::ParseError) -> Self {
        ExonError::ExecutionError(format!("Error parsing URL: {}", error))
    }
}

impl From<ExonError> for std::io::Error {
    fn from(error: ExonError) -> Self {
        std::io::Error::new(std::io::ErrorKind::Other, format!("{}", error))
    }
}

impl From<ParserError> for ExonError {
    fn from(error: ParserError) -> Self {
        ExonError::ParserError(format!("{}", error))
    }
}

impl Display for ExonError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExonError::DataFusionError(error) => write!(f, "DataFusionError: {}", error),
            ExonError::ArrowError(error) => write!(f, "ArrowError: {}", error),
            ExonError::ExecutionError(error) => write!(f, "ExecutionError: {}", error),
            ExonError::ObjectStoreError(error) => write!(f, "ObjectStoreError: {}", error),
            ExonError::NoodlesError(error) => write!(f, "NoodlesError: {}", error),
            ExonError::IOError(error) => write!(f, "IOError: {}", error),
            ExonError::InvalidFileType(error) => write!(f, "InvalidFileType: {}", error),
            ExonError::Configuration(error) => write!(f, "InvalidConfig: {}", error),
            ExonError::ExonGFFError(error) => write!(f, "ExonGFFError: {}", error),
            ExonError::ParserError(error) => write!(f, "ParserError: {}", error),
            ExonError::UnsupportedFunction(error) => write!(f, "UnsupportedFunction: {}", error),
        }
    }
}

impl Error for ExonError {}

impl From<ExonError> for DataFusionError {
    fn from(error: ExonError) -> Self {
        match error {
            ExonError::DataFusionError(error) => error,
            ExonError::ArrowError(error) => DataFusionError::ArrowError(error, None),
            ExonError::ExecutionError(error) => DataFusionError::Execution(error),
            ExonError::Configuration(error) => DataFusionError::Configuration(error),
            _ => DataFusionError::Execution(format!("ExonError: {}", error)),
        }
    }
}

/// Result type for Exon.
pub type Result<T, E = ExonError> = std::result::Result<T, E>;
