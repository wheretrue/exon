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

use datafusion::{logical_expr::LogicalPlan, prelude::SessionContext};

use crate::ExonSessionExt;

pub struct ExonSession {
    ctx: SessionContext,
}

impl Default for ExonSession {
    fn default() -> Self {
        Self {
            ctx: SessionContext::new_exon(),
        }
    }
}

impl ExonSession {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn create_physical_plan(
        &self,
        logical_plan: LogicalPlan,
    ) -> datafusion::error::Result<()> {
        Ok(())
    }
}
