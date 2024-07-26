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

#[derive(Debug)]
pub enum ExonFastqError {
    Arrow(arrow::error::ArrowError),
    Parse(String),
    IO(std::io::Error),
    InvalidColumnIndex(usize),
}

impl Error for ExonFastqError {}

pub type ExonFastqResult<T> = Result<T, ExonFastqError>;

impl Display for ExonFastqError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExonFastqError::Arrow(error) => write!(f, "Arrow error: {}", error),
            ExonFastqError::Parse(msg) => write!(f, "Parse error: {}", msg),
            ExonFastqError::IO(error) => write!(f, "IO error: {}", error),
            ExonFastqError::InvalidColumnIndex(idx) => {
                write!(f, "Invalid column index: {}", idx)
            }
        }
    }
}

impl From<arrow::error::ArrowError> for ExonFastqError {
    fn from(error: arrow::error::ArrowError) -> Self {
        ExonFastqError::Arrow(error)
    }
}

impl From<ExonFastqError> for arrow::error::ArrowError {
    fn from(error: ExonFastqError) -> Self {
        arrow::error::ArrowError::ExternalError(Box::new(error))
    }
}

impl From<Utf8Error> for ExonFastqError {
    fn from(error: Utf8Error) -> Self {
        ExonFastqError::Parse(error.to_string())
    }
}

impl From<std::io::Error> for ExonFastqError {
    fn from(error: std::io::Error) -> Self {
        ExonFastqError::IO(error)
    }
}
