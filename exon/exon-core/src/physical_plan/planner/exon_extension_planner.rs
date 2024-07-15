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

use std::{env, str::FromStr, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    datasource::{
        file_format::file_compression_type::FileCompressionType, listing::PartitionedFile,
        physical_plan::FileSinkConfig, DefaultTableSource,
    },
    execution::context::SessionState,
    logical_expr::{LogicalPlan, LogicalPlanBuilder, UserDefinedLogicalNode},
    physical_plan::{insert::DataSinkExec, ExecutionPlan},
    physical_planner::{ExtensionPlanner, PhysicalPlanner},
    sql::{
        parser::{CopyToSource, Statement},
        sqlparser::ast,
        TableReference,
    },
};
use exon_fasta::FASTASchemaBuilder;
use exon_fastq::new_fastq_schema_builder;

use crate::{
    datasources::ExonFileType,
    logical_plan::ExonDataSinkLogicalPlanNode,
    physical_plan::object_store::{parse_url, url_to_object_store_url},
    sinks::SimpleRecordSink,
};

pub struct ExomeExtensionPlanner {}

impl ExomeExtensionPlanner {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for ExomeExtensionPlanner {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ExtensionPlanner for ExomeExtensionPlanner {
    async fn plan_extension(
        &self,
        planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        _physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &SessionState,
    ) -> datafusion::error::Result<Option<Arc<dyn ExecutionPlan>>> {
        let logical_node = node
            .as_any()
            .downcast_ref::<ExonDataSinkLogicalPlanNode>()
            .unwrap();

        let input_plan = match &logical_node.source {
            CopyToSource::Query(q) => {
                session_state
                    .statement_to_plan(Statement::Statement(Box::new(ast::Statement::Query(
                        Box::new(q.clone()),
                    ))))
                    .await?
            }
            CopyToSource::Relation(r) => {
                let catalog = &session_state.config_options().catalog;

                let table_name = r.to_string();
                let table_ref = TableReference::parse_str(&table_name);

                let table = table_ref
                    .clone()
                    .resolve(&catalog.default_catalog, &catalog.default_schema);

                let u = session_state
                    .catalog_list()
                    .catalog(&table.catalog)
                    .unwrap()
                    .schema(&table.schema)
                    .unwrap();

                let table_provider = u.table(&table.table).await?.ok_or(
                    datafusion::error::DataFusionError::Plan(format!(
                        "Table {} not found in schema {}",
                        table.table, table.schema
                    )),
                )?;

                let table_source = Arc::new(DefaultTableSource::new(table_provider));

                let builder = LogicalPlanBuilder::scan(table_ref, table_source, None)?;

                builder.build()?
            }
        };

        let physical_plan = planner
            .create_physical_plan(&input_plan, session_state)
            .await?;

        let url = logical_node.target.clone();

        let url = parse_url(&url)?;
        let authority = match url.host_str() {
            Some(host) => format!("{}://{}", url.scheme(), host),
            None => format!("{}://", url.scheme()),
        };

        let is_local = authority.starts_with("file://");

        let path = if is_local {
            let p = std::path::Path::new(logical_node.target.as_str());

            if p.is_absolute() {
                object_store::path::Path::from_absolute_path(p)?
            } else {
                let current_dir = env::current_dir()?;

                let absolute_path = current_dir.join(p);
                object_store::path::Path::from_absolute_path(absolute_path)?
            }
        } else {
            let path = &url.as_str()[authority.len()..];
            object_store::path::Path::parse(path)?
        };

        let object_store_url = url_to_object_store_url(&parse_url(&logical_node.target)?)?;
        let p_file = PartitionedFile::new(path, 0);

        let stored_as = logical_node.stored_as.as_ref().ok_or_else(|| {
            datafusion::error::DataFusionError::Plan(
                "Stored as option is required for ExonDataSinkLogicalPlanNode".to_string(),
            )
        })?;
        let exon_file_type = ExonFileType::from_str(stored_as)?;

        let schema = match ExonFileType::from_str(stored_as)? {
            ExonFileType::FASTA => FASTASchemaBuilder::default().build().file_schema().unwrap(),
            ExonFileType::FASTQ => new_fastq_schema_builder().build().file_schema().unwrap(),
            _ => {
                return Err(datafusion::error::DataFusionError::Plan(
                    "Invalid file type".to_string(),
                ))
            }
        };

        let file_sink_config = FileSinkConfig {
            object_store_url,
            file_groups: vec![p_file],
            table_paths: vec![],
            output_schema: schema.clone(),
            table_partition_cols: vec![],
            overwrite: false,
            keep_partition_by_columns: false,
        };

        let compression_type = logical_node
            .file_compression_type()?
            .unwrap_or(FileCompressionType::UNCOMPRESSED);

        let sink = Arc::new(SimpleRecordSink::new(
            file_sink_config,
            compression_type,
            exon_file_type,
        ));

        let data_sink = DataSinkExec::new(physical_plan, sink, schema, None);

        Ok(Some(Arc::new(data_sink)))
    }
}
