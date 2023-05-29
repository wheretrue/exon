use std::{any::Any, sync::Arc};

use arrow::datatypes::SchemaRef;
use datafusion::physical_plan::{
    file_format::{FileScanConfig, FileStream},
    metrics::ExecutionPlanMetricsSet,
    ExecutionPlan, Partitioning, SendableRecordBatchStream, Statistics,
};

use super::{config::BCFConfig, file_opener::BCFOpener};

#[derive(Debug)]
/// Implements a datafusion `ExecutionPlan` for BCF files.
pub struct BCFScan {
    /// The base configuration for the file scan.
    base_config: FileScanConfig,

    /// The projected schema for the scan.
    projected_schema: SchemaRef,

    /// Metrics for the execution plan.
    metrics: ExecutionPlanMetricsSet,
}

impl BCFScan {
    /// Create a new BCF scan.
    pub fn new(base_config: FileScanConfig) -> Self {
        let projected_schema = match &base_config.projection {
            Some(p) => Arc::new(base_config.file_schema.project(&p).unwrap()),
            None => base_config.file_schema.clone(),
        };

        Self {
            base_config,
            projected_schema,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl ExecutionPlan for BCFScan {
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

        let config = BCFConfig::new(object_store, self.base_config.file_schema.clone())
            .with_batch_size(batch_size)
            .with_some_projection(self.base_config.projection.clone());

        let opener = BCFOpener::new(Arc::new(config));

        let stream = FileStream::new(&self.base_config, partition, opener, &self.metrics)?;

        Ok(Box::pin(stream) as SendableRecordBatchStream)
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}
