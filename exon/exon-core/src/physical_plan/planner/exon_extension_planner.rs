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

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::{
    datasource::{listing::PartitionedFile, physical_plan::FileSinkConfig},
    execution::{context::SessionState, object_store::ObjectStoreUrl},
    logical_expr::{LogicalPlan, UserDefinedLogicalNode},
    physical_plan::{insert::DataSinkExec, ExecutionPlan},
    physical_planner::{ExtensionPlanner, PhysicalPlanner},
    sql::parser::{CopyToSource, Statement},
};
use exon_fasta::FASTASchemaBuilder;
use object_store::path::Path;

use crate::{logical_plan::ExonDataSinkLogicalPlanNode, sinks::FASTADataSync};

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

        let copy_to = match &logical_node.source {
            CopyToSource::Query(q) => q,
            _ => unimplemented!(),
        };

        let input_plan = session_state
            .statement_to_plan(Statement::Statement(Box::new(
                sqlparser::ast::Statement::Query(Box::new(copy_to.clone())),
            )))
            .await?;

        let physical_plan = planner
            .create_physical_plan(&input_plan, session_state)
            .await?;

        let url = logical_node.target.clone();

        // let url = Url::parse(url.as_str()).expect("Invalid default catalog location!");
        // let authority = match url.host_str() {
        //     Some(host) => format!("{}://{}", url.scheme(), host),
        //     None => format!("{}://", url.scheme()),
        // };
        // let path = &url.as_str()[authority.len()..];

        // let path = object_store::path::Path::parse(path).expect("Can't parse path");
        // let object_store_url = ObjectStoreUrl::parse(authority.as_str()).unwrap();
        let object_store_url = ObjectStoreUrl::local_filesystem();
        let path = Path::parse(url.as_str()).expect("Can't parse path");

        let p_file = PartitionedFile::new(path, 0);

        let schema = FASTASchemaBuilder::default().build().file_schema().unwrap();

        let file_sink_config = FileSinkConfig {
            object_store_url,
            file_groups: vec![p_file],
            table_paths: vec![],
            output_schema: schema,
            table_partition_cols: vec![],
            overwrite: false,
        };

        let sink = Arc::new(FASTADataSync::new(file_sink_config));
        let sink_schema = FASTASchemaBuilder::default().build().file_schema().unwrap();

        let data_sink = DataSinkExec::new(physical_plan, sink, sink_schema, None);

        Ok(Some(Arc::new(data_sink)))
    }
}
