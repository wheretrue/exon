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

use core::str;
use std::sync::Arc;

use arrow::{
    array::{Array, GenericListBuilder, Int32Builder},
    datatypes::{DataType, Field},
};
use datafusion::{
    common::cast::as_string_array,
    error::Result,
    logical_expr::{ColumnarValue, ScalarUDFImpl, Volatility},
    scalar::ScalarValue,
};

#[derive(Debug)]
pub(crate) struct QualityScoreStringToList {
    signature: datafusion::logical_expr::Signature,
}

impl Default for QualityScoreStringToList {
    fn default() -> Self {
        let signature =
            datafusion::logical_expr::Signature::exact(vec![DataType::Utf8], Volatility::Immutable);

        Self { signature }
    }
}

impl ScalarUDFImpl for QualityScoreStringToList {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "quality_scores_to_list"
    }

    fn signature(&self) -> &datafusion::logical_expr::Signature {
        &self.signature
    }

    fn invoke(
        &self,
        args: &[datafusion::logical_expr::ColumnarValue],
    ) -> Result<datafusion::logical_expr::ColumnarValue> {
        if args.len() > 1 {
            return Err(datafusion::error::DataFusionError::Execution(format!(
                "{} takes a single argument",
                self.name()
            )));
        }

        let arg = &args[0];

        match arg {
            ColumnarValue::Array(arr) => {
                let strings = as_string_array(arr)?;
                let capacity = strings.len();

                let values_builder = Int32Builder::with_capacity(capacity);
                let mut builder = GenericListBuilder::<i32, Int32Builder>::with_capacity(
                    values_builder,
                    capacity,
                );

                for i in 0..capacity {
                    let sequence = strings.value(i);

                    let mut scores = Vec::with_capacity(sequence.len());
                    for c in sequence.chars() {
                        scores.push(c as i32 - 33);
                    }

                    builder.values().append_slice(&scores);
                    builder.append(true);
                }

                let list = builder.finish();
                Ok(ColumnarValue::Array(Arc::new(list)))
            }
            ColumnarValue::Scalar(scalar) => match scalar {
                ScalarValue::Utf8(Some(sequence)) => {
                    let mut scores = Vec::with_capacity(sequence.len());
                    for c in sequence.chars() {
                        scores.push(c as i32 - 33);
                    }

                    let values_builder = Int32Builder::new();
                    let mut builder = GenericListBuilder::<i32, Int32Builder>::new(values_builder);

                    builder.values().append_slice(&scores);
                    builder.append(true);

                    let values = builder.finish();
                    Ok(ColumnarValue::Array(Arc::new(values)))
                }
                _ => Err(datafusion::error::DataFusionError::Execution(format!(
                    "{} takes a string",
                    self.name()
                ))),
            },
        }
    }

    fn return_type(
        &self,
        arg_types: &[arrow::datatypes::DataType],
    ) -> Result<arrow::datatypes::DataType> {
        if arg_types.len() != 1 {
            return Err(datafusion::error::DataFusionError::Execution(format!(
                "{} takes a single argument",
                self.name()
            )));
        }

        let field = Field::new("item", DataType::Int32, true);
        Ok(DataType::List(Arc::new(field)))
    }
}
