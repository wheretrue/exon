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

use arrow::{array::Float32Array, datatypes::DataType};
use datafusion::{
    common::cast::as_string_array,
    error::Result,
    logical_expr::{ColumnarValue, ScalarUDFImpl, Volatility},
    scalar::ScalarValue,
};

#[derive(Debug)]
pub(crate) struct GCContent {
    signature: datafusion::logical_expr::Signature,
}

impl Default for GCContent {
    fn default() -> Self {
        let signature =
            datafusion::logical_expr::Signature::exact(vec![DataType::Utf8], Volatility::Immutable);

        Self { signature }
    }
}

impl ScalarUDFImpl for GCContent {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "gc_content"
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
                            let gc_count =
                                sequence.chars().filter(|c| *c == 'G' || *c == 'C').count() as f32;
                            let total_count = sequence.len() as f32;
                            Some(gc_count / total_count)
                        }
                        None => None,
                    })
                    .collect::<Float32Array>();

                Ok(ColumnarValue::Array(Arc::new(f)))
            }
            Some(ColumnarValue::Scalar(scalar)) => match scalar {
                ScalarValue::Utf8(Some(sequence)) => {
                    let gc_count =
                        sequence.chars().filter(|c| *c == 'G' || *c == 'C').count() as f32;
                    let total_count = sequence.len() as f32;
                    let f = gc_count / total_count;
                    Ok(ColumnarValue::Scalar(ScalarValue::Float32(Some(f))))
                }
                ScalarValue::Utf8(None) => Ok(ColumnarValue::Scalar(ScalarValue::Float32(None))),
                _ => Err(datafusion::error::DataFusionError::Execution(
                    "gc_content takes a string".to_string(),
                )),
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
            return Err(datafusion::error::DataFusionError::Execution(
                "gc_content takes one argument".to_string(),
            ));
        }

        Ok(DataType::Float32)
    }
}
