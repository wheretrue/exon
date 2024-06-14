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

use std::{error::Error, fmt::Display};

#[derive(Debug)]
pub enum ExonBEDError {
    InvalidNumberOfFields(usize),
    InvalidNumberOfFieldsType(String),
    ArrowError(arrow::error::ArrowError),
}

impl Display for ExonBEDError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExonBEDError::InvalidNumberOfFields(n) => {
                write!(f, "Wrong number of fields: {}", n)
            }
            ExonBEDError::InvalidNumberOfFieldsType(e) => {
                write!(f, "Invalid number of fields type: {}", e)
            }
            ExonBEDError::ArrowError(e) => {
                write!(f, "Arrow error: {}", e)
            }
        }
    }
}

impl From<std::num::ParseIntError> for ExonBEDError {
    fn from(e: std::num::ParseIntError) -> Self {
        ExonBEDError::InvalidNumberOfFieldsType(e.to_string())
    }
}

impl From<arrow::error::ArrowError> for ExonBEDError {
    fn from(e: arrow::error::ArrowError) -> Self {
        ExonBEDError::ArrowError(e)
    }
}

impl Error for ExonBEDError {}

pub type ExonBEDResult<T> = Result<T, ExonBEDError>;
