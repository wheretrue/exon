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

mod gc_content;
mod reverse_complement;

use datafusion::{execution::context::SessionContext, logical_expr::ScalarUDF};

use gc_content::GCContent;
use reverse_complement::ReverseComplement;

/// Returns a vector of ScalarUDFs from the sequence module.
pub fn register_udfs(ctx: &SessionContext) {
    let gc_content = GCContent::default();
    let gc_content_scalar = ScalarUDF::from(gc_content);

    ctx.register_udf(gc_content_scalar);

    let reverse_complement = ReverseComplement::default();
    let reverse_complement_scalar = ScalarUDF::from(reverse_complement);

    ctx.register_udf(reverse_complement_scalar);
}
