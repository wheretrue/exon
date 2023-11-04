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

use datafusion::{
    common::parsers::CompressionTypeVariant,
    logical_expr::{CreateExternalTable, UserDefinedLogicalNodeCore},
};

use crate::{
    exome::physical_plan::CHANGE_LOGICAL_SCHEMA, exome_extension_planner::DfExtensionNode,
};

const NODE_NAME: &str = "CreateExomeTable";

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub struct CreateExomeTable {
    pub name: String,
    pub schema_name: String,
    pub catalog_name: String,

    pub location: String,
    pub file_format: String,
    pub is_listing: bool,
    pub compression_type: String,
    pub partition_cols: Vec<String>,

    pub if_not_exists: bool,
}

impl UserDefinedLogicalNodeCore for CreateExomeTable {
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

impl DfExtensionNode for CreateExomeTable {
    const NODE_NAME: &'static str = NODE_NAME;
}

impl TryFrom<CreateExternalTable> for CreateExomeTable {
    type Error = std::io::Error;

    fn try_from(value: CreateExternalTable) -> Result<Self, Self::Error> {
        let full_name = value.name.table().split('.').collect::<Vec<_>>();

        if full_name.len() != 3 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "Invalid table name: {}. Expected format: catalog.schema.table",
                    value.name.table()
                ),
            ));
        }

        // table_name is catalog_name.schema_name.table_name
        let catalog_name = full_name[0];
        let schema_name = full_name[1];
        let table_name = full_name[2];

        let location = value.location;
        let file_format = value.file_type;

        let partition_cols = value.table_partition_cols;

        // TODO: how to get this?
        let is_listing = true;

        let compression_type = match value.file_compression_type {
            CompressionTypeVariant::GZIP => "GZIP".to_string(),
            CompressionTypeVariant::ZSTD => "ZSTD".to_string(),
            CompressionTypeVariant::UNCOMPRESSED => "UNCOMPRESSED".to_string(),
            _ => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!(
                        "Unsupported file compression type {:?}",
                        value.file_compression_type
                    ),
                ))
            }
        };

        Ok(CreateExomeTable {
            name: table_name.to_string(),
            schema_name: schema_name.to_string(),
            catalog_name: catalog_name.to_string(),
            if_not_exists: value.if_not_exists,
            location,
            file_format,
            is_listing,
            compression_type,
            partition_cols,
        })
    }
}
