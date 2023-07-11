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
    datasource::{
        file_format::file_type::FileCompressionType,
        physical_plan::{FileScanConfig, FileStream},
    },
    physical_plan::{
        metrics::ExecutionPlanMetricsSet, ExecutionPlan, Partitioning, SendableRecordBatchStream,
        Statistics,
    },
};

// file format moted to physcial plan

use super::{config::FCSConfig, file_opener::FCSOpener};

#[derive(Debug)]
/// Implements a datafusion `ExecutionPlan` for FCS files.
pub struct FCSScan {
    /// The base configuration for the file scan.
    base_config: FileScanConfig,
    /// The projected schema for the scan.
    projected_schema: SchemaRef,
    /// The compression type of the file.
    file_compression_type: FileCompressionType,
    /// Metrics for the execution plan.
    metrics: ExecutionPlanMetricsSet,
}

impl FCSScan {
    /// Create a new FCS scan.
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
        }
    }
}

impl ExecutionPlan for FCSScan {
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

        let mut config = FCSConfig::new(object_store, self.base_config.file_schema.clone())
            .with_batch_size(batch_size);
        if let Some(projections) = &self.base_config.projection {
            config = config.with_projection(projections.clone());
        }

        let opener = FCSOpener::new(Arc::new(config), self.file_compression_type.clone());
        let stream = FileStream::new(&self.base_config, partition, opener, &self.metrics)?;

        Ok(Box::pin(stream) as SendableRecordBatchStream)
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{datasources::fcs::file_format::FCSFormat, tests::test_listing_table_url};

    use datafusion::{
        datasource::listing::{ListingOptions, ListingTable, ListingTableConfig},
        prelude::SessionContext,
    };

    #[tokio::test]
    async fn test_fcs_read() {
        let ctx = SessionContext::new();
        let session_state = ctx.state();

        let table_path = test_listing_table_url("fcs");

        let bcf_format = Arc::new(FCSFormat::default());
        let lo = ListingOptions::new(bcf_format.clone()).with_file_extension("fcs");

        let resolved_schema = lo.infer_schema(&session_state, &table_path).await.unwrap();

        assert_eq!(resolved_schema.fields().len(), 10);

        let config = ListingTableConfig::new(table_path)
            .with_listing_options(lo)
            .with_schema(resolved_schema);

        let provider = Arc::new(ListingTable::try_new(config).unwrap());
        let df = ctx.read_table(provider.clone()).unwrap();

        let mut row_cnt = 0;
        let bs = df.collect().await.unwrap();
        for batch in bs {
            row_cnt += batch.num_rows();
        }
        assert_eq!(row_cnt, 108)
    }
}
