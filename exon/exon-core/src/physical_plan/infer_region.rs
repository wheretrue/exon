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

use std::str::FromStr;

use datafusion::logical_expr::{expr::ScalarFunction, Expr};
use noodles::core::Region;

pub(crate) fn infer_region_from_udf(scalar_udf: &ScalarFunction, name: &str) -> Option<Region> {
    if scalar_udf.name() == name {
        match &scalar_udf.args[0] {
            Expr::Literal(l) => {
                let region_str = l.to_string();
                let region = Region::from_str(region_str.as_str()).ok()?;
                Some(region)
            }
            _ => None,
        }
    } else {
        None
    }
}
