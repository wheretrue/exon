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
    fmt::{self, Display, Formatter},
};

use datafusion::error::DataFusionError;
use exon::ExonError;

#[derive(Debug)]
enum ExonRError {
    DataFusionError(DataFusionError),
}

impl Display for ExonRError {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            ExonRError::DataFusionError(e) => write!(f, "DataFusionError: {}", e),
        }
    }
}

impl Error for ExonRError {}

impl From<DataFusionError> for ExonRError {
    fn from(e: DataFusionError) -> Self {
        ExonRError::DataFusionError(e)
    }
}

impl From<ExonError> for ExonRError {
    fn from(e: ExonError) -> Self {
        ExonRError::DataFusionError(DataFusionError::Execution(e.to_string()))
    }
}
