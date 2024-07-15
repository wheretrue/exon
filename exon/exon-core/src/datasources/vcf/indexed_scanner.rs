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

use std::{any::Any, sync::Arc};

use arrow::datatypes::SchemaRef;
use datafusion::{
    common::Statistics,
    datasource::physical_plan::{FileScanConfig, FileStream},
    error::Result,
    physical_plan::{
        metrics::ExecutionPlanMetricsSet, DisplayAs, DisplayFormatType, ExecutionPlan,
        PlanProperties, SendableRecordBatchStream,
    },
};
use exon_vcf::VCFConfig;
use noodles::core::Region;

use crate::datasources::ExonFileScanConfig;

use super::file_opener::indexed_file_opener::IndexedVCFOpener;

#[derive(Debug, Clone)]
/// Implements a datafusion `ExecutionPlan` for VCF files.
pub struct IndexedVCFScanner {
    /// The base configuration for the file scan.
    base_config: FileScanConfig,

    /// The projected schema for the scan.
    projected_schema: SchemaRef,

    /// Metrics for the execution plan.
    metrics: ExecutionPlanMetricsSet,

    /// The region to use for filtering.
    region: Arc<Region>,

    /// The plan properties cache.
    properties: PlanProperties,

    /// The statistics for the scan.
    statistics: Statistics,
}

impl IndexedVCFScanner {
    /// Create a new VCF scan.
    pub fn new(base_config: FileScanConfig, region: Arc<Region>) -> Result<Self> {
        let (projected_schema, statistics, properties) = base_config.project_with_properties();

        Ok(Self {
            base_config,
            projected_schema,
            metrics: ExecutionPlanMetricsSet::new(),
            region: region.clone(),
            properties,
            statistics,
        })
    }

    /// Return the base configuration for the scan.
    pub fn base_config(&self) -> &FileScanConfig {
        &self.base_config
    }
}

impl DisplayAs for IndexedVCFScanner {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let repr = format!("IndexedVCFScanner: {}", self.base_config.file_groups.len());
        write!(f, "{}", repr)
    }
}

impl ExecutionPlan for IndexedVCFScanner {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "IndexedVCFScanner"
    }

    fn repartitioned(
        &self,
        target_partitions: usize,
        _config: &datafusion::config::ConfigOptions,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
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

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn statistics(&self) -> datafusion::error::Result<Statistics> {
        Ok(self.statistics.clone())
    }

    fn schema(&self) -> SchemaRef {
        let schema = self.projected_schema.clone();
        tracing::trace!("VCF schema: {}", schema);
        schema
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

        let config = VCFConfig::new(object_store, self.base_config.file_schema.clone())
            .with_batch_size(batch_size)
            .with_projection(self.base_config().file_projection());

        let opener = IndexedVCFOpener::new(Arc::new(config), self.region.clone());

        let stream = FileStream::new(&self.base_config, partition, opener, &self.metrics)?;
        Ok(Box::pin(stream) as SendableRecordBatchStream)
    }
}
