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

use std::{error::Error, fmt::Display, num::ParseIntError, str::Utf8Error};

use arrow::error::ArrowError;

#[derive(Debug)]
pub enum ExonGFFError {
    InvalidRecord(String),
    InvalidDirective(String),
    ExternalError(Box<dyn std::error::Error + Send + Sync>),
    IoError(std::io::Error),
}

impl Display for ExonGFFError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExonGFFError::InvalidRecord(s) => write!(f, "Invalid record: {}", s),
            ExonGFFError::InvalidDirective(s) => write!(f, "Invalid directive: {}", s),
            ExonGFFError::ExternalError(e) => write!(f, "External error: {}", e),
            ExonGFFError::IoError(e) => write!(f, "IO error: {}", e),
        }
    }
}

impl Error for ExonGFFError {}

impl From<noodles::gff::line::ParseError> for ExonGFFError {
    fn from(e: noodles::gff::line::ParseError) -> Self {
        match e {
            noodles::gff::line::ParseError::InvalidRecord(s) => {
                ExonGFFError::InvalidRecord(s.to_string())
            }
            noodles::gff::line::ParseError::InvalidDirective(s) => {
                ExonGFFError::InvalidDirective(s.to_string())
            }
        }
    }
}

impl From<ArrowError> for ExonGFFError {
    fn from(e: ArrowError) -> Self {
        ExonGFFError::ExternalError(Box::new(e))
    }
}

impl From<ExonGFFError> for ArrowError {
    fn from(e: ExonGFFError) -> Self {
        ArrowError::ExternalError(Box::new(e))
    }
}

impl From<std::io::Error> for ExonGFFError {
    fn from(e: std::io::Error) -> Self {
        ExonGFFError::IoError(e)
    }
}

impl From<Utf8Error> for ExonGFFError {
    fn from(e: Utf8Error) -> Self {
        ExonGFFError::ExternalError(Box::new(e))
    }
}

impl From<ParseIntError> for ExonGFFError {
    fn from(e: ParseIntError) -> Self {
        ExonGFFError::ExternalError(Box::new(e))
    }
}

pub type Result<T> = std::result::Result<T, ExonGFFError>;
