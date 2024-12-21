// Copyright 2024 WHERE TRUE Technologies.
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

use std::{
    hash::{Hash, Hasher},
    str::FromStr,
    sync::Arc,
};

use arrow::datatypes::{DataType, Field, Schema};
use datafusion::{
    common::{DFSchema, DFSchemaRef},
    datasource::file_format::file_compression_type::FileCompressionType,
    logical_expr::{LogicalPlan, UserDefinedLogicalNodeCore},
    sql::{parser::CopyToSource, sqlparser::ast::Value},
};

use crate::{sql::ExonCopyToStatement, ExonError};

use super::DfExtensionNode;

pub(crate) struct ExonDataSinkLogicalPlanNode {
    pub schema: Arc<DFSchema>,
    pub source: CopyToSource,
    pub target: String,
    pub stored_as: Option<String>,
    pub options: Vec<(String, Value)>,
}

impl std::fmt::Debug for ExonDataSinkLogicalPlanNode {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "ExonDataSinkLogicalPlanNode")
    }
}

impl ExonDataSinkLogicalPlanNode {
    fn new(
        source: CopyToSource,
        target: String,
        stored_as: Option<String>,
        options: Vec<(String, Value)>,
    ) -> Self {
        let schema = Schema::new(vec![Field::new("count", DataType::UInt64, false)]);
        let schema = DFSchema::try_from(schema).unwrap();

        Self {
            schema: Arc::new(schema),
            source,
            target,
            stored_as,
            options,
        }
    }

    fn inner_schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    pub(crate) fn file_compression_type(&self) -> crate::Result<Option<FileCompressionType>> {
        let inferred_type = self
            .options
            .iter()
            .find(|(k, _)| k == "compression")
            .map(|(_, v)| match v {
                Value::SingleQuotedString(s) => {
                    FileCompressionType::from_str(s).map_err(|e| e.into())
                }
                _ => Err(ExonError::ExecutionError(
                    "Invalid compression type".to_string(),
                )),
            });

        inferred_type.transpose()
    }
}

impl From<ExonCopyToStatement> for ExonDataSinkLogicalPlanNode {
    fn from(stmt: ExonCopyToStatement) -> Self {
        let source = stmt.source;
        let target = stmt.target;
        let stored_as = stmt.stored_as;
        let options = stmt.options;

        Self::new(source, target, stored_as, options)
    }
}

impl PartialOrd for ExonDataSinkLogicalPlanNode {
    fn partial_cmp(&self, _other: &Self) -> Option<std::cmp::Ordering> {
        None
    }
}

impl DfExtensionNode for ExonDataSinkLogicalPlanNode {}

impl Hash for ExonDataSinkLogicalPlanNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // self.source.hash(state);
        self.target.hash(state);
        self.stored_as.hash(state);
        self.options.hash(state);
    }
}

impl PartialEq for ExonDataSinkLogicalPlanNode {
    fn eq(&self, other: &Self) -> bool {
        self.source == other.source
            && self.target == other.target
            && self.stored_as == other.stored_as
            && self.options == other.options
    }
}

impl Eq for ExonDataSinkLogicalPlanNode {}

impl UserDefinedLogicalNodeCore for ExonDataSinkLogicalPlanNode {
    fn name(&self) -> &str {
        "ExonDataSinkLogicalPlanNode"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.inner_schema()
    }

    fn expressions(&self) -> Vec<datafusion::prelude::Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "ExonDataSinkLogicalPlanNode")
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<datafusion::prelude::Expr>,
        _inputs: Vec<LogicalPlan>,
    ) -> datafusion::error::Result<Self> {
        Ok(Self::new(
            self.source.clone(),
            self.target.clone(),
            self.stored_as.clone(),
            self.options.clone(),
        ))
    }
}
