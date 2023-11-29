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

use std::{any::Any, fmt, sync::Arc};

use arrow::datatypes::SchemaRef;
use datafusion::{
    datasource::physical_plan::{FileScanConfig, FileStream},
    physical_plan::{
        metrics::ExecutionPlanMetricsSet, DisplayAs, DisplayFormatType, ExecutionPlan,
        Partitioning, SendableRecordBatchStream,
    },
};
use exon_bam::BAMConfig;
use noodles::core::Region;

use crate::datasources::ExonFileScanConfig;

use super::file_opener::BAMOpener;

#[derive(Debug, Clone)]
/// Implements a datafusion `ExecutionPlan` for BAM files.
pub struct BAMScan {
    /// The schema of the data source.
    projected_schema: SchemaRef,

    /// The configuration for the file scan.
    base_config: FileScanConfig,

    /// Metrics for the execution plan.
    metrics: ExecutionPlanMetricsSet,

    /// An optional region filter for the scan.
    region_filter: Option<Region>,
}

impl BAMScan {
    /// Create a new BAM scan.
    pub fn new(base_config: FileScanConfig) -> Self {
        let (projected_schema, ..) = base_config.project();

        Self {
            base_config,
            projected_schema,
            metrics: ExecutionPlanMetricsSet::new(),
            region_filter: None,
        }
    }

    /// Set the region filter for the scan.
    pub fn with_region_filter(mut self, region_filter: Region) -> Self {
        self.region_filter = Some(region_filter);
        self
    }
}

impl DisplayAs for BAMScan {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        write!(f, "BAMScan")
    }
}

impl ExecutionPlan for BAMScan {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn repartitioned(
        &self,
        target_partitions: usize,
        _config: &datafusion::config::ConfigOptions,
    ) -> datafusion::error::Result<Option<Arc<dyn ExecutionPlan>>> {
        if target_partitions == 1 {
            return Ok(None);
        }

        let file_groups = self.base_config.regroup_files_by_size(target_partitions);

        let mut new_plan = self.clone();
        if let Some(repartitioned_file_groups) = file_groups {
            tracing::info!(
                "Repartitioned {} file groups into {}",
                self.base_config.file_groups.len(),
                repartitioned_file_groups.len()
            );
            new_plan.base_config.file_groups = repartitioned_file_groups;
        }

        Ok(Some(Arc::new(new_plan)))
    }

    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }

    fn output_partitioning(&self) -> datafusion::physical_plan::Partitioning {
        Partitioning::UnknownPartitioning(self.base_config.file_groups.len())
    }

    fn output_ordering(&self) -> Option<&[datafusion::physical_expr::PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
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

        let config = BAMConfig::new(object_store, self.base_config.file_schema.clone())
            .with_batch_size(batch_size)
            .with_projection(self.base_config.file_projection());

        let opener = BAMOpener::new(Arc::new(config));

        let stream = FileStream::new(&self.base_config, partition, opener, &self.metrics)?;

        Ok(Box::pin(stream) as SendableRecordBatchStream)
    }
}
