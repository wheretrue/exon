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

use datafusion::logical_expr::{CreateCatalogSchema, UserDefinedLogicalNodeCore};

use crate::{
    exome::physical_plan::CHANGE_LOGICAL_SCHEMA, exome_extension_planner::DfExtensionNode,
};

const NODE_NAME: &str = "CreateExomeSchema";

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct CreateExomeSchema {
    pub name: String,
    pub catalog_name: String,
    pub if_not_exists: bool,
}

impl UserDefinedLogicalNodeCore for CreateExomeSchema {
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

impl DfExtensionNode for CreateExomeSchema {
    const NODE_NAME: &'static str = NODE_NAME;
}

impl TryFrom<CreateCatalogSchema> for CreateExomeSchema {
    type Error = ();

    fn try_from(value: CreateCatalogSchema) -> Result<Self, Self::Error> {
        let schema_name = value.schema_name.split('.').collect::<Vec<_>>();

        if schema_name.len() != 2 {
            return Err(());
        }

        Ok(CreateExomeSchema {
            name: schema_name[1].to_string(),
            catalog_name: schema_name[0].to_string(),
            if_not_exists: value.if_not_exists,
        })
    }
}