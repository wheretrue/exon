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

use std::{error::Error, fmt::Display};

use arrow::error::ArrowError;

/// An error returned when reading a FASTA file fails for some reason.
#[derive(Debug)]
pub enum ExonFastaError {
    InvalidDefinition(String),
    InvalidRecord(String),
    ArrowError(ArrowError),
    IOError(std::io::Error),
    ParseError(String),
}

impl Display for ExonFastaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExonFastaError::InvalidDefinition(msg) => write!(f, "Invalid definition: {}", msg),
            ExonFastaError::InvalidRecord(msg) => write!(f, "Invalid record: {}", msg),
            ExonFastaError::ArrowError(error) => write!(f, "Arrow error: {}", error),
            ExonFastaError::IOError(error) => write!(f, "IO error: {}", error),
            ExonFastaError::ParseError(msg) => write!(f, "Parse error: {}", msg),
        }
    }
}

impl Error for ExonFastaError {}

impl From<std::io::Error> for ExonFastaError {
    fn from(error: std::io::Error) -> Self {
        ExonFastaError::IOError(error)
    }
}

impl From<noodles::fasta::record::definition::ParseError> for ExonFastaError {
    fn from(error: noodles::fasta::record::definition::ParseError) -> Self {
        ExonFastaError::ParseError(error.to_string())
    }
}

impl From<ArrowError> for ExonFastaError {
    fn from(error: ArrowError) -> Self {
        ExonFastaError::ArrowError(error)
    }
}

impl From<ExonFastaError> for ArrowError {
    fn from(error: ExonFastaError) -> Self {
        ArrowError::ExternalError(Box::new(error))
    }
}

pub type ExonFastaResult<T, E = ExonFastaError> = std::result::Result<T, E>;
