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
    array::{as_list_array, Array, GenericStringBuilder, Int32Array, ListArray},
    datatypes::{DataType, Field},
};
use datafusion::{
    error::Result,
    logical_expr::{ColumnarValue, ScalarUDFImpl, Volatility},
    scalar::ScalarValue,
};

#[derive(Debug)]
pub(crate) struct QualityScoreListToString {
    signature: datafusion::logical_expr::Signature,
}

impl Default for QualityScoreListToString {
    fn default() -> Self {
        let t = DataType::List(Arc::new(Field::new("item", DataType::Int32, true)));

        let signature = datafusion::logical_expr::Signature::exact(vec![t], Volatility::Immutable);

        Self { signature }
    }
}

impl ScalarUDFImpl for QualityScoreListToString {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "quality_scores_to_string"
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
                let arr = as_list_array(arr);

                let mut builder =
                    GenericStringBuilder::<i32>::with_capacity(arr.len(), arr.len() * 2);

                for i in 0..arr.len() {
                    let list = arr.value(i);
                    let list = list
                        .as_any()
                        .downcast_ref::<arrow::array::Int32Array>()
                        .ok_or(datafusion::error::DataFusionError::Execution(format!(
                            "Expected Int32Array, found {:?}",
                            list.data_type()
                        )))?;

                    let mut v = Vec::with_capacity(list.len());

                    for j in 0..list.len() {
                        let score = list.value(j);
                        let score = (score + 33) as u8;

                        v.push(score);
                    }

                    let s = str::from_utf8(&v).map_err(|e| {
                        datafusion::error::DataFusionError::Execution(format!(
                            "Invalid UTF-8: {}",
                            e
                        ))
                    })?;

                    builder.append_value(s);
                }

                Ok(ColumnarValue::Array(Arc::new(builder.finish())))
            }
            ColumnarValue::Scalar(scalar) => match scalar {
                ScalarValue::List(list) => {
                    let list = list.as_any().downcast_ref::<ListArray>().ok_or(
                        datafusion::error::DataFusionError::Execution(format!(
                            "Expected Vec<Option<i32>>, found {:?}",
                            list
                        )),
                    )?;

                    let mut builder =
                        GenericStringBuilder::<i32>::with_capacity(list.len(), list.len() * 2);

                    for score in list.iter() {
                        let score = score.ok_or(datafusion::error::DataFusionError::Execution(
                            "Quality scores must be integers".to_string(),
                        ))?;

                        let score = score.as_any().downcast_ref::<Int32Array>().ok_or(
                            datafusion::error::DataFusionError::Execution(format!(
                                "Expected Int32Array, found {:?}",
                                score
                            )),
                        )?;

                        // collect list into a Vec<u8>, raising an error if any of the values are None
                        let mut v = Vec::with_capacity(score.len());

                        for i in 0..score.len() {
                            let score = score.value(i);
                            let score = (score + 33) as u8;

                            v.push(score);
                        }

                        let s = str::from_utf8(&v).map_err(|e| {
                            datafusion::error::DataFusionError::Execution(format!(
                                "Invalid UTF-8: {}",
                                e
                            ))
                        })?;

                        builder.append_value(s);
                    }

                    Ok(ColumnarValue::Array(Arc::new(builder.finish())))
                }
                _ => Err(datafusion::error::DataFusionError::Execution(format!(
                    "{} takes a list of integers",
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

        Ok(DataType::Utf8)
    }
}
