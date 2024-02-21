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

use arrow::{array::ArrayRef, datatypes::DataType};
use datafusion::{
    common::cast::as_int32_array,
    error::Result,
    execution::context::SessionContext,
    logical_expr::{ScalarUDF, ScalarUDFImpl, Volatility},
};
use noodles::sam::alignment::record::Flags;

fn sam_flag_function(args: &[ArrayRef], record_flag: Flags) -> Result<ArrayRef> {
    if args.len() != 1 {
        return Err(datafusion::error::DataFusionError::Execution(
            "flag scalar takes one argument".to_string(),
        ));
    }

    let sam_flags = as_int32_array(&args[0])?;

    let array = sam_flags
        .iter()
        .map(|sam_flag| {
            sam_flag.map(|f| {
                let f16 = f as u16;
                let flag = Flags::from_bits_truncate(f16);
                flag.contains(record_flag)
            })
        })
        .collect::<arrow::array::BooleanArray>();

    Ok(Arc::new(array))
}

#[derive(Debug)]
pub struct SAMScalarUDF {
    signature: datafusion::logical_expr::Signature,
    flags: Flags,
    name: String,
}

impl SAMScalarUDF {
    fn new(flags: Flags, name: String) -> Self {
        let type_signature = datafusion::logical_expr::TypeSignature::Exact(vec![DataType::Int32]);
        let signature =
            datafusion::logical_expr::Signature::new(type_signature, Volatility::Immutable);

        Self {
            flags,
            signature,
            name,
        }
    }
}

impl ScalarUDFImpl for SAMScalarUDF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &datafusion::logical_expr::Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke(
        &self,
        args: &[datafusion::physical_plan::ColumnarValue],
    ) -> Result<datafusion::physical_plan::ColumnarValue> {
        if args.len() != 1 {
            return Err(datafusion::error::DataFusionError::Execution(format!(
                "{} takes one argument",
                self.name
            )));
        }

        match &args[0] {
            datafusion::physical_plan::ColumnarValue::Array(arr) => {
                let array = sam_flag_function(&[arr.clone()], self.flags)?;
                Ok(datafusion::physical_plan::ColumnarValue::Array(array))
            }
            _ => Err(datafusion::error::DataFusionError::Execution(
                "flag scalar takes an array argument".to_string(),
            )),
        }
    }
}

/// Returns a vector of SAM function UDFs.
pub fn register_udfs(ctx: &SessionContext) {
    let udfs = vec![
        SAMScalarUDF::new(Flags::SEGMENTED, "is_segmented".to_string()),
        SAMScalarUDF::new(Flags::PROPERLY_ALIGNED, "is_properly_aligned".to_string()),
        SAMScalarUDF::new(Flags::UNMAPPED, "is_unmapped".to_string()),
        SAMScalarUDF::new(Flags::MATE_UNMAPPED, "is_mate_unmapped".to_string()),
        SAMScalarUDF::new(
            Flags::REVERSE_COMPLEMENTED,
            "is_reverse_complemented".to_string(),
        ),
        SAMScalarUDF::new(
            Flags::MATE_REVERSE_COMPLEMENTED,
            "is_mate_reverse_complemented".to_string(),
        ),
        SAMScalarUDF::new(Flags::FIRST_SEGMENT, "is_first_segment".to_string()),
        SAMScalarUDF::new(Flags::LAST_SEGMENT, "is_last_segment".to_string()),
        SAMScalarUDF::new(Flags::SECONDARY, "is_secondary".to_string()),
        SAMScalarUDF::new(Flags::QC_FAIL, "is_qc_fail".to_string()),
        SAMScalarUDF::new(Flags::DUPLICATE, "is_duplicate".to_string()),
        SAMScalarUDF::new(Flags::SUPPLEMENTARY, "is_supplementary".to_string()),
    ];

    for udf in udfs {
        let scalar_func = ScalarUDF::from(udf);
        ctx.register_udf(scalar_func);
    }
}
