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

use arrow::array::{ArrayRef, Float32Array};
use datafusion::{common::cast::as_string_array, error::Result};

/// Compute the GC content of a sequence.
///
/// # Arguments
///
/// * `args` - A slice of ArrayRefs. The first element should be a StringArray.
///       The StringArray should contain sequences and be a single column.
///
/// # Example
///
/// ```rust
/// use arrow::array::{ArrayRef, Float32Array, StringArray};
/// use datafusion::{common::cast::as_float32_array, error::Result};
/// use std::sync::Arc;
///
/// let sequence_array = StringArray::from(vec![Some("ATCG"), None]);
/// let array_ref = Arc::new(sequence_array) as ArrayRef;
///
/// let result = exon::udfs::sequence::gc_content(&[array_ref]).unwrap();
/// let result = as_float32_array(&result).unwrap();
///
/// let expected = vec![Some(0.5), None];
/// let expected = Float32Array::from(expected);
///
/// result
///     .iter()
///     .zip(expected.iter())
///     .for_each(|(result, expected)| {
///         assert_eq!(result, expected);
///    });
/// ```
pub fn gc_content(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 1 {
        return Err(datafusion::error::DataFusionError::Execution(
            "reverse_complement takes one argument".to_string(),
        ));
    }

    let sequences = as_string_array(&args[0])?;

    let array = sequences
        .iter()
        .map(|sequence| match sequence {
            Some(sequence) => {
                let gc_count = sequence.chars().filter(|c| *c == 'G' || *c == 'C').count() as f32;
                let total_count = sequence.len() as f32;
                Some(gc_count / total_count)
            }
            None => None,
        })
        .collect::<Float32Array>();

    Ok(Arc::new(array))
}
