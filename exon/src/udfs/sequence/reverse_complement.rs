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

use arrow::array::{ArrayRef, StringArray};
use datafusion::{common::cast::as_string_array, error::Result};

/// Reverse complement a sequence.
///
/// # Arguments
///
/// * `args` - A slice of ArrayRefs. The first element should be a StringArray.
///       The StringArray should contain sequences and be a single column.
///
/// # Example
///
/// ```rust
/// use arrow::array::{ArrayRef, StringArray};
/// use datafusion::{common::cast::as_string_array, error::Result};
/// use std::sync::Arc;
///
/// let sequence_array = StringArray::from(vec![Some("ATCG"), None]);
/// let array_ref = Arc::new(sequence_array) as ArrayRef;
///
/// let result = exon::udfs::sequence::reverse_complement(&[array_ref]).unwrap();
///
/// let string_result = as_string_array(&result).unwrap();
/// let expected_sequence_array = StringArray::from(vec![Some("CGAT"), None]);
///
/// string_result
///   .iter()
///   .zip(expected_sequence_array.iter())
///   .for_each(|(result, expected)| {
///       assert_eq!(result, expected);
///  });
/// ```
pub fn reverse_complement(args: &[ArrayRef]) -> Result<ArrayRef> {
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
                let mut reverse_complement = String::new();
                for base in sequence.chars().rev() {
                    match base {
                        'A' => reverse_complement.push('T'),
                        'T' => reverse_complement.push('A'),
                        'C' => reverse_complement.push('G'),
                        'G' => reverse_complement.push('C'),
                        _ => reverse_complement.push(base),
                    }
                }
                Some(reverse_complement)
            }
            None => None,
        })
        .collect::<StringArray>();

    Ok(Arc::new(array))
}
