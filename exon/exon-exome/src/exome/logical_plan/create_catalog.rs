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

use datafusion::logical_expr::UserDefinedLogicalNodeCore;

use crate::exome::physical_plan::CHANGE_LOGICAL_SCHEMA;

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct CreateCatalog {
    pub name: String,
}

impl UserDefinedLogicalNodeCore for CreateCatalog {
    fn name(&self) -> &str {
        "CreateCatalog"
    }

    fn inputs(&self) -> Vec<&datafusion::logical_expr::LogicalPlan> {
        todo!()
    }

    fn schema(&self) -> &datafusion::common::DFSchemaRef {
        &CHANGE_LOGICAL_SCHEMA
    }

    fn expressions(&self) -> Vec<datafusion::prelude::Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "CreateCatalog")
    }

    fn from_template(
        &self,
        _exprs: &[datafusion::prelude::Expr],
        _inputs: &[datafusion::logical_expr::LogicalPlan],
    ) -> Self {
        self.clone()
    }
}
