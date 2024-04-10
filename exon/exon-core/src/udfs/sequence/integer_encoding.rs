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
    array::{Array, Int16Builder, ListBuilder},
    datatypes::{DataType, Field},
};
use datafusion::{
    common::cast::as_string_array,
    error::Result,
    logical_expr::{ColumnarValue, ScalarUDFImpl, Volatility},
    scalar::ScalarValue,
};

#[derive(Debug)]
pub(crate) struct IntegerEncoding {
    signature: datafusion::logical_expr::Signature,
}

impl Default for IntegerEncoding {
    fn default() -> Self {
        let sequence = DataType::Utf8;
        let pattern = DataType::Utf8;

        let signature = datafusion::logical_expr::Signature::exact(
            vec![sequence, pattern],
            Volatility::Immutable,
        );

        Self { signature }
    }
}

fn encoding_values_to_map(encoding_values: &str) -> std::collections::HashMap<char, i16> {
    let mut encoding_map = std::collections::HashMap::new();
    for (value, c) in encoding_values.chars().enumerate() {
        encoding_map.insert(c, value as i16);
    }
    encoding_map
}

impl ScalarUDFImpl for IntegerEncoding {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "integer_encoding"
    }

    fn signature(&self) -> &datafusion::logical_expr::Signature {
        &self.signature
    }

    fn invoke(
        &self,
        args: &[datafusion::logical_expr::ColumnarValue],
    ) -> Result<datafusion::logical_expr::ColumnarValue> {
        if args.len() != 2 {
            return Err(datafusion::error::DataFusionError::Execution(format!(
                "{} takes two arguments",
                self.name()
            )));
        }

        let sequence = &args[0];
        let pattern = &args[1];

        match (sequence, pattern) {
            (
                ColumnarValue::Array(sequence_arr),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(pattern_scalar))),
            ) => {
                let encoding_map = encoding_values_to_map(pattern_scalar);

                let mut list_builder =
                    ListBuilder::with_capacity(Int16Builder::new(), sequence_arr.len());

                let sequence_arr = as_string_array(sequence_arr)?;

                for sequence in sequence_arr.iter() {
                    if let Some(sequence) = sequence {
                        let builder = list_builder.values();

                        for char in sequence.chars() {
                            if let Some(value) = encoding_map.get(&char) {
                                builder.append_value(*value);
                            } else {
                                builder.append_null();
                            }
                        }
                    } else {
                        list_builder.append_null();
                    }

                    list_builder.append(true);
                }

                let list_array = list_builder.finish();

                Ok(ColumnarValue::Array(Arc::new(list_array)))
            }
            (
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(sequence_scalar))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(pattern_scalar))),
            ) => {
                let encoding_map = encoding_values_to_map(pattern_scalar);

                let mut list_builder = ListBuilder::with_capacity(Int16Builder::new(), 1);

                let builder = list_builder.values();

                for char in sequence_scalar.chars() {
                    if let Some(value) = encoding_map.get(&char) {
                        builder.append_value(*value);
                    } else {
                        builder.append_null();
                    }
                }

                list_builder.append(true);

                let list_array = list_builder.finish();

                Ok(ColumnarValue::Array(Arc::new(list_array)))
            }
            _ => Err(datafusion::error::DataFusionError::Execution(format!(
                "{} takes different types of arguments",
                self.name()
            ))),
        }
    }

    fn return_type(
        &self,
        arg_types: &[arrow::datatypes::DataType],
    ) -> Result<arrow::datatypes::DataType> {
        if arg_types.len() != 2 {
            return Err(datafusion::error::DataFusionError::Execution(format!(
                "{} takes two arguments",
                self.name()
            )));
        }

        let dt = DataType::List(Arc::new(Field::new("item", DataType::Int16, true)));
        Ok(dt)
    }
}
