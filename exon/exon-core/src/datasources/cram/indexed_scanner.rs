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

use std::{any::Any, fmt, sync::Arc};

use arrow::datatypes::SchemaRef;
use datafusion::{
    common::Statistics,
    datasource::physical_plan::{FileScanConfig, FileStream},
    execution::SendableRecordBatchStream,
    physical_plan::{
        metrics::ExecutionPlanMetricsSet, DisplayAs, DisplayFormatType, ExecutionPlan,
        PlanProperties,
    },
};
use exon_cram::CRAMConfig;

use crate::datasources::ExonFileScanConfig;

use super::indexed_file_opener::IndexedCRAMOpener;

#[derive(Debug, Clone)]
pub(super) struct IndexedCRAMScan {
    /// The schema of the data source.
    projected_schema: SchemaRef,

    /// The configuration for the file scan.
    base_config: FileScanConfig,

    /// Metrics for the execution plan.
    metrics: ExecutionPlanMetricsSet,

    /// The FASTA reference to use.
    reference: Option<String>,

    /// The plan properties cache.
    properties: PlanProperties,

    /// The statistics for the scan.
    statistics: Statistics,
}

impl IndexedCRAMScan {
    /// Create a new BAM scan.
    pub fn new(base_config: FileScanConfig, reference: Option<String>) -> Self {
        let (projected_schema, statistics, properties) = base_config.project_with_properties();

        Self {
            base_config,
            projected_schema,
            metrics: ExecutionPlanMetricsSet::new(),
            properties,
            statistics,
            reference,
        }
    }
}

impl DisplayAs for IndexedCRAMScan {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        write!(f, "IndexedCRAMScan: ")?;
        self.base_config.fmt_as(t, f)
    }
}

impl ExecutionPlan for IndexedCRAMScan {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "IndexedCRAMScan"
    }

    fn repartitioned(
        &self,
        target_partitions: usize,
        _config: &datafusion::config::ConfigOptions,
    ) -> datafusion::error::Result<Option<Arc<dyn ExecutionPlan>>> {
        if target_partitions == 1 || self.base_config.file_groups.is_empty() {
            return Ok(None);
        }

        let file_groups = self.base_config.regroup_files_by_size(target_partitions);

        let mut new_plan = self.clone();
        new_plan.base_config.file_groups = file_groups;

        new_plan.properties = new_plan.properties.with_partitioning(
            datafusion::physical_plan::Partitioning::UnknownPartitioning(
                new_plan.base_config.file_groups.len(),
            ),
        );

        Ok(Some(Arc::new(new_plan)))
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.projected_schema)
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::context::TaskContext>,
    ) -> datafusion::error::Result<datafusion::physical_plan::SendableRecordBatchStream> {
        let object_store = context
            .runtime_env()
            .object_store(&self.base_config.object_store_url)?;

        let batch_size = context.session_config().batch_size();

        let config = CRAMConfig::new(
            object_store,
            Arc::clone(&self.projected_schema),
            self.reference.clone(),
        )
        .with_batch_size(batch_size)
        .with_projection(self.base_config.file_projection());

        let opener = IndexedCRAMOpener::new(Arc::new(config));

        let stream = FileStream::new(&self.base_config, partition, opener, &self.metrics)?;

        Ok(Box::pin(stream) as SendableRecordBatchStream)
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn statistics(&self) -> datafusion::error::Result<Statistics> {
        Ok(self.statistics.clone())
    }
}
