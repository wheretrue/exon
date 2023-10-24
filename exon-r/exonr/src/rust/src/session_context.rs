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

use datafusion::prelude::SessionContext;
use exon::ExonSessionExt;

use extendr_api::prelude::*;
use tokio::runtime::Runtime;

use crate::RDataFrame;

pub struct ExonSessionContext {
    ctx: SessionContext,
    runtime: Runtime,
}

impl Default for ExonSessionContext {
    fn default() -> Self {
        Self {
            ctx: SessionContext::new_exon(),
            runtime: Runtime::new().unwrap(),
        }
    }
}

#[extendr]
impl ExonSessionContext {
    fn new() -> Result<Self> {
        Ok(Self::default())
    }

    fn sql(&mut self, query: &str) -> Result<RDataFrame> {
        let df = self.runtime.block_on(self.ctx.sql(query)).map_err(|e| {
            Error::from(format!(
                "Error executing query: {}\n{}",
                query,
                e.to_string()
            ))
        })?;

        let rdf = RDataFrame::from(df);

        Ok(rdf)
    }
}
