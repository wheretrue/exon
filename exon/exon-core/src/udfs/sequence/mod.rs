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

mod alignment_score;
mod gc_content;
mod locate_regex;
mod quality_score_list_to_string;
mod quality_score_string_to_list;
mod reverse_complement;
mod trim_polya;

pub(crate) mod motif;

use datafusion::{execution::context::SessionContext, logical_expr::ScalarUDF};

use gc_content::GCContent;
use reverse_complement::ReverseComplement;

use self::{
    alignment_score::AlignmentScore, quality_score_list_to_string::QualityScoreListToString,
    quality_score_string_to_list::QualityScoreStringToList,
};

/// Returns a vector of ScalarUDFs from the sequence module.
pub fn register_udfs(ctx: &SessionContext) {
    let gc_content = GCContent::default();
    let gc_content_scalar = ScalarUDF::from(gc_content);
    ctx.register_udf(gc_content_scalar);

    let alignment_score = AlignmentScore::default();
    let alignment_score_scalar = ScalarUDF::from(alignment_score);
    ctx.register_udf(alignment_score_scalar);

    let quality_to_list = QualityScoreStringToList::default();
    let quality_to_list_scalar = ScalarUDF::from(quality_to_list);
    ctx.register_udf(quality_to_list_scalar);

    let quality_to_string_scaler = QualityScoreListToString::default();
    let quality_to_string_scalar = ScalarUDF::from(quality_to_string_scaler);
    ctx.register_udf(quality_to_string_scalar);

    let trim_polya_scaler = trim_polya::TrimPolyA::default();
    let trim_polya_scalar = ScalarUDF::from(trim_polya_scaler);
    ctx.register_udf(trim_polya_scalar);

    let reverse_complement = ReverseComplement::default();
    let reverse_complement_scalar = ScalarUDF::from(reverse_complement);
    ctx.register_udf(reverse_complement_scalar);

    let locate_regex = locate_regex::LocateRegex::default();
    let locate_regex_udf = ScalarUDF::from(locate_regex);
    ctx.register_udf(locate_regex_udf);
}
