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

use std::env::VarError;

use datafusion::error::DataFusionError;

#[derive(Debug)]
pub enum ExomeError {
    DataFusionError(DataFusionError),
    Execution(String),
    TonicError(tonic::Status),
    EnvironmentError(String),
}

impl From<DataFusionError> for ExomeError {
    fn from(error: DataFusionError) -> Self {
        ExomeError::DataFusionError(error)
    }
}

impl From<tonic::Status> for ExomeError {
    fn from(error: tonic::Status) -> Self {
        ExomeError::TonicError(error)
    }
}

impl From<VarError> for ExomeError {
    fn from(error: VarError) -> Self {
        ExomeError::EnvironmentError(error.to_string())
    }
}

impl From<tonic::transport::Error> for ExomeError {
    fn from(error: tonic::transport::Error) -> Self {
        ExomeError::Execution(error.to_string())
    }
}

impl std::fmt::Display for ExomeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExomeError::DataFusionError(e) => write!(f, "Error: {}", e),
            ExomeError::Execution(e) => write!(f, "Error: {}", e),
            ExomeError::TonicError(e) => write!(f, "Error: {}", e),
            ExomeError::EnvironmentError(e) => write!(f, "Error: {}", e),
        }
    }
}

pub type ExomeResult<T> = std::result::Result<T, ExomeError>;
