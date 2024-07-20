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

use std::{any::Any, sync::Arc};

use arrow::datatypes::SchemaRef;
use datafusion::{
    common::Statistics,
    datasource::physical_plan::{FileScanConfig, FileStream},
    physical_plan::{
        metrics::ExecutionPlanMetricsSet, DisplayAs, DisplayFormatType, ExecutionPlan,
        PlanProperties, SendableRecordBatchStream,
    },
};
use exon_bcf::BCFConfig;
use noodles::core::Region;

use crate::datasources::ExonFileScanConfig;

use super::file_opener::BCFOpener;

#[derive(Debug)]
/// Implements a datafusion `ExecutionPlan` for BCF files.
pub struct BCFScan {
    /// The base configuration for the file scan.
    base_config: FileScanConfig,

    /// The projected schema for the scan.
    projected_schema: SchemaRef,

    /// Metrics for the execution plan.
    metrics: ExecutionPlanMetricsSet,

    /// An optional region filter for the scan.
    region_filter: Option<Region>,

    /// The plan properties cache.
    properties: PlanProperties,

    /// The statistics for the scan.
    statistics: Statistics,
}

impl BCFScan {
    /// Create a new BCF scan.
    pub fn new(base_config: FileScanConfig) -> Self {
        let (projected_schema, statistics, properties) = base_config.project_with_properties();

        Self {
            base_config,
            projected_schema,
            metrics: ExecutionPlanMetricsSet::new(),
            region_filter: None,
            properties,
            statistics,
        }
    }

    /// Set the region filter for the scan.
    pub fn with_region_filter(mut self, region_filter: Region) -> Self {
        self.region_filter = Some(region_filter);
        self
    }
}

impl DisplayAs for BCFScan {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let bcf_repr = format!(
            "BCFScanExec: files={:?}, partitions={:?}",
            self.base_config.file_groups,
            self.base_config.file_projection(),
        );

        write!(f, "{}", bcf_repr)
    }
}

impl ExecutionPlan for BCFScan {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "BCFScanExec"
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

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn statistics(&self) -> datafusion::error::Result<Statistics> {
        Ok(self.statistics.clone())
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

        let config = BCFConfig::new(object_store, Arc::clone(&self.base_config.file_schema))
            .with_batch_size(batch_size)
            .with_some_projection(Some(self.base_config.file_projection()));

        let mut opener = BCFOpener::new(Arc::new(config));

        if let Some(region) = &self.region_filter {
            opener = opener.with_region_filter(region.clone());
        }

        let stream = FileStream::new(&self.base_config, partition, opener, &self.metrics)?;

        Ok(Box::pin(stream) as SendableRecordBatchStream)
    }
}
