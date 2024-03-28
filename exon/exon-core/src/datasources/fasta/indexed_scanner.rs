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

use crate::datasources::ExonFileScanConfig;
use arrow::datatypes::SchemaRef;
use datafusion::{
    common::Statistics,
    datasource::{
        file_format::file_compression_type::FileCompressionType,
        physical_plan::{FileScanConfig, FileStream},
    },
    error::Result as DataFusionResult,
    execution::SendableRecordBatchStream,
    physical_plan::{
        metrics::ExecutionPlanMetricsSet, DisplayAs, DisplayFormatType, ExecutionPlan,
        PlanProperties,
    },
};
use exon_fasta::FASTAConfig;

use super::indexed_file_opener::IndexedFASTAOpener;

#[derive(Debug, Clone)]
/// Implements a datafusion `ExecutionPlan` for indexed FASTA files.
pub struct IndexedFASTAScanner {
    /// The base configuration for the file scan.
    base_config: FileScanConfig,

    /// The compression type for the file.
    file_compression_type: FileCompressionType,

    /// The projected schema for the scan.
    projected_schema: SchemaRef,

    /// Metrics for the execution plan.
    metrics: ExecutionPlanMetricsSet,

    /// The fasta reader capacity.
    fasta_sequence_buffer_capacity: usize,

    /// The plan properties cache.
    properties: PlanProperties,

    /// The statistics for the scan.
    statistics: Statistics,
}

impl IndexedFASTAScanner {
    pub fn new(
        base_config: FileScanConfig,
        file_compression_type: FileCompressionType,
        fasta_sequence_buffer_capacity: usize,
    ) -> Self {
        let (projected_schema, statistics, properties) = base_config.project_with_properties();

        Self {
            base_config,
            projected_schema,
            file_compression_type,
            metrics: ExecutionPlanMetricsSet::new(),
            fasta_sequence_buffer_capacity,
            properties,
            statistics,
        }
    }
}

impl DisplayAs for IndexedFASTAScanner {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let repr = format!(
            "IndexedFASTAScanner: {}",
            self.base_config.file_groups.len()
        );
        write!(f, "{}", repr)
    }
}

impl ExecutionPlan for IndexedFASTAScanner {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn statistics(&self) -> datafusion::error::Result<Statistics> {
        Ok(self.statistics.clone())
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn repartitioned(
        &self,
        target_partitions: usize,
        _config: &datafusion::config::ConfigOptions,
    ) -> DataFusionResult<Option<Arc<dyn ExecutionPlan>>> {
        let file_groups = self.base_config.regroup_files_by_size(target_partitions);

        match file_groups {
            Some(file_groups) => {
                let mut new_plan = self.clone();
                new_plan.base_config.file_groups = file_groups;

                Ok(Some(Arc::new(new_plan)))
            }
            None => Ok(None),
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::context::TaskContext>,
    ) -> DataFusionResult<datafusion::physical_plan::SendableRecordBatchStream> {
        let object_store = context
            .runtime_env()
            .object_store(&self.base_config.object_store_url)?;

        let config = FASTAConfig::new(object_store, self.base_config.file_schema.clone())
            .with_batch_size(context.session_config().batch_size())
            .with_fasta_sequence_buffer_capacity(self.fasta_sequence_buffer_capacity)
            .with_projection(self.base_config.file_projection());

        let opener = IndexedFASTAOpener::new(Arc::new(config), self.file_compression_type);

        let stream = FileStream::new(&self.base_config, partition, opener, &self.metrics)?;
        Ok(Box::pin(stream) as SendableRecordBatchStream)
    }
}
