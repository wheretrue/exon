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

mod bin_vectors;
mod contains_peak;

use datafusion::logical_expr::ScalarUDF;

/// Returns a vector of ScalarUDFs from the sequence module.
pub fn register_udfs() -> Vec<ScalarUDF> {
    vec![
        bin_vectors::bin_vectors_udf(),
        contains_peak::contains_peak_udf(),
    ]
}
