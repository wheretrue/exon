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

use arrow::array::{as_string_array, ArrayRef, Float32Builder};

use datafusion::error::Result;

/// Include the generated bindings into a separate module.
#[allow(non_upper_case_globals)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[allow(unused)]
mod wfa {
    include!(concat!(env!("OUT_DIR"), "/bindings_wfa.rs"));
}

pub fn alignment_score(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 2 {
        return Err(datafusion::error::DataFusionError::Execution(
            "align_sequence takes two arguments".to_string(),
        ));
    }

    let sequences = as_string_array(&args[0]);
    let reference = as_string_array(&args[1]);

    let mut score_builder = Float32Builder::new();

    unsafe {
        let mut attributes = wfa::wavefront_aligner_attr_default;
        attributes.heuristic.strategy = wfa::wf_heuristic_strategy_wf_heuristic_none;

        let wf_aligner = wfa::wavefront_aligner_new(&mut attributes);

        for (sequence, reference) in sequences.iter().zip(reference.iter()) {
            match (sequence, reference) {
                (None, _) => {
                    score_builder.append_null();
                }
                (_, None) => {
                    return Err(datafusion::error::DataFusionError::Execution(
                        "align_sequence takes two arguments".to_string(),
                    ));
                }
                (Some(s), Some(r)) => {
                    let s_ptr = std::ffi::CString::new(s)
                        .map_err(|e| {
                            return datafusion::error::DataFusionError::Execution(format!(
                                "CString::new failed: {}",
                                e.to_string()
                            ));
                        })?
                        .as_ptr();

                    let r_ptr = std::ffi::CString::new(r)
                        .map_err(|e| {
                            return datafusion::error::DataFusionError::Execution(format!(
                                "CString::new failed: {}",
                                e.to_string()
                            ));
                        })?
                        .as_ptr();

                    let status = wfa::wavefront_align(
                        wf_aligner,
                        s_ptr,
                        s.len() as i32,
                        r_ptr,
                        r.len() as i32,
                    );

                    if status != 0 {
                        return Err(datafusion::error::DataFusionError::Execution(
                            "wavefront_align failed".to_string(),
                        ));
                    }

                    let cigar = (*wf_aligner).cigar;

                    let score = (*cigar).score;

                    score_builder.append_value(score as f32);
                }
            }
        }

        wfa::wavefront_aligner_delete(wf_aligner)
    }

    Ok(Arc::new(score_builder.finish()))
}
