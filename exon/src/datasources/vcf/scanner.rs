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
    datasource::file_format::file_type::FileCompressionType,
    physical_plan::{
        file_format::{FileScanConfig, FileStream},
        metrics::ExecutionPlanMetricsSet,
        ExecutionPlan, Partitioning, SendableRecordBatchStream, Statistics,
    },
};
use noodles::core::Region;

use super::{config::VCFConfig, file_opener::VCFOpener};

#[derive(Debug)]
/// Implements a datafusion `ExecutionPlan` for VCF files.
pub struct VCFScan {
    /// The base configuration for the file scan.
    base_config: FileScanConfig,
    /// The projected schema for the scan.
    projected_schema: SchemaRef,
    /// The compression type of the file.
    file_compression_type: FileCompressionType,
    /// Metrics for the execution plan.
    metrics: ExecutionPlanMetricsSet,
    /// An optional region filter for the scan.
    region_filter: Option<Region>,
}

impl VCFScan {
    /// Create a new VCF scan.
    pub fn new(base_config: FileScanConfig, file_compression_type: FileCompressionType) -> Self {
        let projected_schema = match &base_config.projection {
            Some(p) => Arc::new(base_config.file_schema.project(p).unwrap()),
            None => base_config.file_schema.clone(),
        };

        Self {
            base_config,
            projected_schema,
            file_compression_type,
            metrics: ExecutionPlanMetricsSet::new(),
            region_filter: None,
        }
    }

    pub fn with_filter(mut self, region_filter: Region) -> Self {
        self.region_filter = Some(region_filter);
        self
    }
}

impl ExecutionPlan for VCFScan {
    fn as_any(&self) -> &dyn Any {
        self
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

        let mut config = VCFConfig::new(object_store, self.base_config.file_schema.clone())
            .with_batch_size(batch_size);
        if let Some(projections) = &self.base_config.projection {
            config = config.with_projection(projections.clone());
        }

        let mut opener = VCFOpener::new(Arc::new(config), self.file_compression_type.clone());

        if let Some(x) = &self.region_filter {
            opener = opener.with_region(x.clone());
        }

        let stream = FileStream::new(&self.base_config, partition, opener, &self.metrics)?;

        Ok(Box::pin(stream) as SendableRecordBatchStream)
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}
