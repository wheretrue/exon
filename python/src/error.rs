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

use arrow::error::ArrowError;
use datafusion::error::DataFusionError;
use exon::error::ExonError;
use pyo3::PyErr;

#[derive(Debug)]
pub enum ExonPyError {
    IOError(String),
    Other(String),
}

pub type ExonPyResult<T> = Result<T, ExonPyError>;

impl ExonPyError {
    pub fn new(msg: &str) -> Self {
        Self::Other(msg.to_string())
    }
}

impl From<ExonPyError> for PyErr {
    fn from(value: ExonPyError) -> Self {
        match value {
            ExonPyError::IOError(msg) => PyErr::new::<pyo3::exceptions::PyIOError, _>(msg),
            ExonPyError::Other(msg) => PyErr::new::<pyo3::exceptions::PyIOError, _>(msg),
        }
    }
}

impl From<DataFusionError> for ExonPyError {
    fn from(value: DataFusionError) -> Self {
        match value {
            DataFusionError::IoError(msg) => Self::IOError(msg.to_string()),
            DataFusionError::ObjectStore(err) => Self::IOError(err.to_string()),
            _ => Self::Other(value.to_string()),
        }
    }
}

impl From<ExonError> for ExonPyError {
    fn from(value: ExonError) -> Self {
        match value {
            ExonError::IOError(e) => ExonPyError::IOError(e.to_string()),
            _ => ExonPyError::Other("Other Error".to_string()),
        }
    }
}

impl From<ArrowError> for ExonPyError {
    fn from(value: ArrowError) -> Self {
        Self::Other(value.to_string())
    }
}

impl From<std::io::Error> for ExonPyError {
    fn from(value: std::io::Error) -> Self {
        Self::IOError(value.to_string())
    }
}
