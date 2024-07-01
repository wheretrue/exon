// Copyright 2024 WHERE TRUE Technologies.
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

use std::{
    error::Error,
    fmt::Display,
    num::{ParseFloatError, ParseIntError},
};

#[derive(Debug)]
pub enum ExonSDFError {
    InvalidInput(String),
    MissingDataField,
    Internal(String),
    IoError(std::io::Error),
    ArrowError(arrow::error::ArrowError),
    UnexpectedEndofAtomBlock,
    FailedToParseAtom(String),
    UnexpectedEndofBondBlock,
    FailedToParseBond(String),
    ParseError(String),
}

impl Display for ExonSDFError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExonSDFError::InvalidInput(msg) => write!(f, "Invalid input: {}", msg),
            ExonSDFError::MissingDataField => write!(f, "Missing data field"),
            ExonSDFError::Internal(msg) => {
                write!(f, "Internal error (please contact the developers): {}", msg)
            }
            ExonSDFError::IoError(err) => write!(f, "I/O error: {}", err),
            ExonSDFError::ArrowError(err) => write!(f, "Arrow error: {}", err),
            ExonSDFError::UnexpectedEndofAtomBlock => write!(f, "Unexpected end of atom block"),
            ExonSDFError::FailedToParseAtom(msg) => write!(f, "Failed to parse atom: {}", msg),
            ExonSDFError::UnexpectedEndofBondBlock => write!(f, "Unexpected end of bond block"),
            ExonSDFError::FailedToParseBond(msg) => write!(f, "Failed to parse bond: {}", msg),
            ExonSDFError::ParseError(msg) => write!(f, "Parse error: {}", msg),
        }
    }
}

impl Error for ExonSDFError {}

pub type Result<T> = std::result::Result<T, ExonSDFError>;

impl From<std::io::Error> for ExonSDFError {
    fn from(err: std::io::Error) -> Self {
        ExonSDFError::Internal(err.to_string())
    }
}

impl From<arrow::error::ArrowError> for ExonSDFError {
    fn from(err: arrow::error::ArrowError) -> Self {
        ExonSDFError::ArrowError(err)
    }
}

impl From<ParseFloatError> for ExonSDFError {
    fn from(err: ParseFloatError) -> Self {
        ExonSDFError::ParseError(err.to_string())
    }
}

impl From<ParseIntError> for ExonSDFError {
    fn from(err: ParseIntError) -> Self {
        ExonSDFError::ParseError(err.to_string())
    }
}
