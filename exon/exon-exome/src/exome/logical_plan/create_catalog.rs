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

use datafusion::logical_expr::{CreateCatalog, UserDefinedLogicalNodeCore};

use crate::{
    exome::physical_plan::CHANGE_LOGICAL_SCHEMA, exome_extension_planner::DfExtensionNode,
};

const NODE_NAME: &str = "CreateExomeCatalog";

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct CreateExomeCatalog {
    pub name: String,
}

impl UserDefinedLogicalNodeCore for CreateExomeCatalog {
    fn name(&self) -> &str {
        NODE_NAME
    }

    fn inputs(&self) -> Vec<&datafusion::logical_expr::LogicalPlan> {
        vec![]
    }

    fn schema(&self) -> &datafusion::common::DFSchemaRef {
        &CHANGE_LOGICAL_SCHEMA
    }

    fn expressions(&self) -> Vec<datafusion::prelude::Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", NODE_NAME)
    }

    fn from_template(
        &self,
        _exprs: &[datafusion::prelude::Expr],
        _inputs: &[datafusion::logical_expr::LogicalPlan],
    ) -> Self {
        self.clone()
    }
}

impl DfExtensionNode for CreateExomeCatalog {
    const NODE_NAME: &'static str = NODE_NAME;
}

impl From<CreateCatalog> for CreateExomeCatalog {
    fn from(value: CreateCatalog) -> Self {
        CreateExomeCatalog {
            name: value.catalog_name,
        }
    }
}
