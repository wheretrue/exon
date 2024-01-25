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
    logical_expr::{ScalarUDF, Volatility},
    physical_plan::functions::make_scalar_function,
    prelude::create_udf,
};
use noodles::sam::alignment::record::Flags;

/// Based on the SAM flags, determine if the read is segmented.
///
/// # Arguments
///
/// * `args` - A slice of ArrayRefs. The first element should be a Int32Array.
///     The Int32Array should contain SAM flags and be a single column.
///
/// # Example
///
/// ```rust
/// use arrow::array::{ArrayRef, BooleanArray, Int32Array};
/// use datafusion::{common::cast::as_boolean_array, error::Result};
/// use std::sync::Arc;
///
/// let sam_flags = Int32Array::from(vec![Some(1), Some(2)]);
/// let array_ref = Arc::new(sam_flags) as ArrayRef;
///
/// let result = exon::udfs::samflags::is_segmented(&[array_ref]).unwrap();
/// let result = as_boolean_array(&result).unwrap();
///
/// let expected = vec![Some(true), Some(false)];
/// let expected = BooleanArray::from(expected);
///
/// result
///     .iter()
///     .zip(expected.iter())
///     .for_each(|(result, expected)| {
///         assert_eq!(result, expected);
///    });
/// ```
pub fn is_segmented(args: &[ArrayRef]) -> Result<ArrayRef> {
    sam_flag_function(args, Flags::SEGMENTED)
}

/// Based on the SAM flags, determine if the read is properly aligned.
pub fn is_properly_aligned(args: &[ArrayRef]) -> Result<ArrayRef> {
    sam_flag_function(args, Flags::PROPERLY_ALIGNED)
}

/// Based on the SAM flags, determine if the read is unmapped.
pub fn is_unmapped(args: &[ArrayRef]) -> Result<ArrayRef> {
    sam_flag_function(args, Flags::UNMAPPED)
}

/// Based on the SAM flags, determine if the mate is unmapped.
pub fn is_mate_unmapped(args: &[ArrayRef]) -> Result<ArrayRef> {
    sam_flag_function(args, Flags::MATE_UNMAPPED)
}

/// Based on the SAM flags, determine if the read is reverse complemented.
pub fn is_reverse_complemented(args: &[ArrayRef]) -> Result<ArrayRef> {
    sam_flag_function(args, Flags::REVERSE_COMPLEMENTED)
}

/// Based on the SAM flags, determine if the mate is reverse complemented.
pub fn is_mate_reverse_complemented(args: &[ArrayRef]) -> Result<ArrayRef> {
    sam_flag_function(args, Flags::MATE_REVERSE_COMPLEMENTED)
}

/// Based on the SAM flags, determine if the read is the first segment.
pub fn is_first_segment(args: &[ArrayRef]) -> Result<ArrayRef> {
    sam_flag_function(args, Flags::FIRST_SEGMENT)
}

/// Based on the SAM flags, determine if the read is the last segment.
pub fn is_last_segment(args: &[ArrayRef]) -> Result<ArrayRef> {
    sam_flag_function(args, Flags::LAST_SEGMENT)
}

/// Based on the SAM flags, determine if the secondary alignment.
pub fn is_secondary(args: &[ArrayRef]) -> Result<ArrayRef> {
    sam_flag_function(args, Flags::SECONDARY)
}

/// Based on the SAM flags, determine if the read is a quality check failure.
pub fn is_qc_fail(args: &[ArrayRef]) -> Result<ArrayRef> {
    sam_flag_function(args, Flags::QC_FAIL)
}

/// Based on the SAM flags, determine if the read is a PCR or optical duplicate.
pub fn is_duplicate(args: &[ArrayRef]) -> Result<ArrayRef> {
    sam_flag_function(args, Flags::DUPLICATE)
}

/// Based on the SAM flags, determine if the read is supplementary.
pub fn is_supplementary(args: &[ArrayRef]) -> Result<ArrayRef> {
    sam_flag_function(args, Flags::SUPPLEMENTARY)
}

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

/// Returns a vector of SAM function UDFs.
pub fn register_udfs() -> Vec<ScalarUDF> {
    vec![
        // create_udf(name, input_types, return_type, volatility, fun)
        create_udf(
            "is_segmented",
            vec![DataType::Int32],
            Arc::new(DataType::Boolean),
            Volatility::Immutable,
            make_scalar_function(is_segmented),
        ),
        create_udf(
            "is_properly_aligned",
            vec![DataType::Int32],
            Arc::new(DataType::Boolean),
            Volatility::Immutable,
            make_scalar_function(is_properly_aligned),
        ),
        create_udf(
            "is_unmapped",
            vec![DataType::Int32],
            Arc::new(DataType::Boolean),
            Volatility::Immutable,
            make_scalar_function(is_unmapped),
        ),
        create_udf(
            "is_mate_unmapped",
            vec![DataType::Int32],
            Arc::new(DataType::Boolean),
            Volatility::Immutable,
            make_scalar_function(is_mate_unmapped),
        ),
        create_udf(
            "is_reverse_complemented",
            vec![DataType::Int32],
            Arc::new(DataType::Boolean),
            Volatility::Immutable,
            make_scalar_function(is_reverse_complemented),
        ),
        create_udf(
            "is_mate_reverse_complemented",
            vec![DataType::Int32],
            Arc::new(DataType::Boolean),
            Volatility::Immutable,
            make_scalar_function(is_mate_reverse_complemented),
        ),
        create_udf(
            "is_first_segment",
            vec![DataType::Int32],
            Arc::new(DataType::Boolean),
            Volatility::Immutable,
            make_scalar_function(is_first_segment),
        ),
        create_udf(
            "is_last_segment",
            vec![DataType::Int32],
            Arc::new(DataType::Boolean),
            Volatility::Immutable,
            make_scalar_function(is_last_segment),
        ),
        create_udf(
            "is_secondary",
            vec![DataType::Int32],
            Arc::new(DataType::Boolean),
            Volatility::Immutable,
            make_scalar_function(is_secondary),
        ),
        create_udf(
            "is_qc_fail",
            vec![DataType::Int32],
            Arc::new(DataType::Boolean),
            Volatility::Immutable,
            make_scalar_function(is_qc_fail),
        ),
        create_udf(
            "is_duplicate",
            vec![DataType::Int32],
            Arc::new(DataType::Boolean),
            Volatility::Immutable,
            make_scalar_function(is_duplicate),
        ),
        create_udf(
            "is_supplementary",
            vec![DataType::Int32],
            Arc::new(DataType::Boolean),
            Volatility::Immutable,
            make_scalar_function(is_supplementary),
        ),
    ]
}
