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

use std::sync::Arc;

use arrow::{array::GenericStringArray, datatypes::DataType};
use datafusion::{
    common::cast::as_string_array,
    error::Result,
    logical_expr::{ColumnarValue, ScalarUDFImpl, Volatility},
    scalar::ScalarValue,
};

#[derive(Debug)]
pub(crate) struct TrimPolyA {
    signature: datafusion::logical_expr::Signature,
}

impl Default for TrimPolyA {
    fn default() -> Self {
        let signature =
            datafusion::logical_expr::Signature::exact(vec![DataType::Utf8], Volatility::Immutable);

        Self { signature }
    }
}

/// Trim polyA tail from a sequence.
fn trim_polya(sequence: &str) -> String {
    let mut end = sequence.len();

    for c in sequence.chars().rev() {
        // if we hit something besides A or a, we're done
        if c != 'A' && c != 'a' {
            break;
        }
        end -= 1;
    }

    sequence[..end].to_string()
}

impl ScalarUDFImpl for TrimPolyA {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "trim_polya"
    }

    fn signature(&self) -> &datafusion::logical_expr::Signature {
        &self.signature
    }

    fn invoke(
        &self,
        args: &[datafusion::logical_expr::ColumnarValue],
    ) -> Result<datafusion::logical_expr::ColumnarValue> {
        match args.first() {
            Some(ColumnarValue::Array(array)) => {
                let f = as_string_array(array)?
                    .iter()
                    .map(|sequence| match sequence {
                        Some(sequence) => {
                            let trimmed = trim_polya(sequence);
                            Some(trimmed)
                        }
                        None => None,
                    })
                    .collect::<Vec<_>>();

                Ok(ColumnarValue::Array(Arc::new(
                    GenericStringArray::<i32>::from(f),
                )))
            }
            Some(ColumnarValue::Scalar(scalar)) => match scalar {
                ScalarValue::Utf8(Some(sequence)) => {
                    let trimmed = trim_polya(sequence);
                    Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(trimmed))))
                }
                ScalarValue::Utf8(None) => Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None))),
                _ => Err(datafusion::error::DataFusionError::Execution(format!(
                    "Invalid argument for function {}",
                    self.name()
                ))),
            },
            None => Err(datafusion::error::DataFusionError::Execution(
                "Failed to get sequence".to_string(),
            )),
        }
    }

    fn return_type(
        &self,
        arg_types: &[arrow::datatypes::DataType],
    ) -> Result<arrow::datatypes::DataType> {
        if arg_types.len() != 1 {
            return Err(datafusion::error::DataFusionError::Execution(format!(
                "Invalid number of arguments for function {}. Expected 1, got {}",
                self.name(),
                arg_types.len()
            )));
        }

        Ok(DataType::Utf8)
    }
}
