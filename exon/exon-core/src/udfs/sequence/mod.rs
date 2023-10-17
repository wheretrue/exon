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

#[cfg(any(target_os = "linux", target_os = "macos"))]
mod alignment;

mod gc_content;
mod reverse_complement;

use std::sync::Arc;

use arrow::datatypes::DataType;
use datafusion::{
    logical_expr::{ScalarUDF, Volatility},
    physical_plan::functions::make_scalar_function,
    prelude::create_udf,
};

/// Returns a vector of ScalarUDFs from the sequence module.
pub fn register_udfs() -> Vec<ScalarUDF> {
    vec![
        create_udf(
            "gc_content",
            vec![DataType::Utf8],
            Arc::new(DataType::Float32),
            Volatility::Immutable,
            make_scalar_function(gc_content::gc_content),
        ),
        create_udf(
            "reverse_complement",
            vec![DataType::Utf8],
            Arc::new(DataType::Utf8),
            Volatility::Immutable,
            make_scalar_function(reverse_complement::reverse_complement),
        ),
        // only build on linux and macos
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        create_udf(
            "alignment_score",
            vec![DataType::Utf8, DataType::Utf8],
            Arc::new(DataType::Float32),
            Volatility::Immutable,
            make_scalar_function(alignment::alignment_score),
        ),
    ]
}
