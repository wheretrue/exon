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

/// An error returned when reading a FASTA file fails for some reason.
#[derive(Debug)]
pub enum ExonFASTAError {
    InvalidDefinition(String),
    InvalidRecord(String),
    ArrowError(ArrowError),
    IOError(std::io::Error),
    ParseError(String),
    ArrayBuilderError(String),
    InvalidNucleotide(u8),
    InvalidAminoAcid(u8),
    InvalidSequenceDataType(String),
}

impl Display for ExonFASTAError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExonFASTAError::InvalidDefinition(msg) => write!(f, "Invalid definition: {}", msg),
            ExonFASTAError::InvalidRecord(msg) => write!(f, "Invalid record: {}", msg),
            ExonFASTAError::ArrowError(error) => write!(f, "Arrow error: {}", error),
            ExonFASTAError::IOError(error) => write!(f, "IO error: {}", error),
            ExonFASTAError::ParseError(msg) => write!(f, "Parse error: {}", msg),
            ExonFASTAError::ArrayBuilderError(msg) => write!(f, "Array builder error: {}", msg),
            ExonFASTAError::InvalidNucleotide(nucleotide) => {
                write!(f, "Invalid nucleotide: {}", nucleotide)
            }
            ExonFASTAError::InvalidAminoAcid(amino_acid) => {
                write!(
                    f,
                    "Invalid amino acid: {}",
                    std::char::from_u32(*amino_acid as u32).unwrap()
                )
            }
            ExonFASTAError::InvalidSequenceDataType(data_type) => {
                write!(f, "Invalid sequence data type: {}", data_type)
            }
        }
    }
}

impl Error for ExonFASTAError {}

impl From<std::io::Error> for ExonFASTAError {
    fn from(error: std::io::Error) -> Self {
        ExonFASTAError::IOError(error)
    }
}

impl From<noodles::fasta::record::definition::ParseError> for ExonFASTAError {
    fn from(error: noodles::fasta::record::definition::ParseError) -> Self {
        ExonFASTAError::ParseError(error.to_string())
    }
}

impl From<ArrowError> for ExonFASTAError {
    fn from(error: ArrowError) -> Self {
        ExonFASTAError::ArrowError(error)
    }
}

impl From<ExonFASTAError> for ArrowError {
    fn from(error: ExonFASTAError) -> Self {
        ArrowError::ExternalError(Box::new(error))
    }
}

impl From<Utf8Error> for ExonFASTAError {
    fn from(error: Utf8Error) -> Self {
        ExonFASTAError::ParseError(error.to_string())
    }
}

pub type ExonFASTAResult<T, E = ExonFASTAError> = std::result::Result<T, E>;
