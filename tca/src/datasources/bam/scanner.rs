use std::{any::Any, sync::Arc};

use arrow::datatypes::SchemaRef;
use datafusion::physical_plan::{
    file_format::{FileScanConfig, FileStream},
    metrics::ExecutionPlanMetricsSet,
    ExecutionPlan, Partitioning, SendableRecordBatchStream, Statistics,
};

use super::{config::BAMConfig, file_opener::BAMOpener};

#[derive(Debug)]
/// Implements a datafusion `ExecutionPlan` for BAM files.
pub struct BAMScan {
    /// The schema of the data source.
    projected_schema: SchemaRef,

    /// The configuration for the file scan.
    base_config: FileScanConfig,

    /// Metrics for the execution plan.
    metrics: ExecutionPlanMetricsSet,
}

impl BAMScan {
    /// Create a new BAM scan.
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

impl ExecutionPlan for BAMScan {
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

        let mut config = BAMConfig::new(object_store, self.base_config.file_schema.clone())
            .with_batch_size(batch_size);

        if let Some(projection) = &self.base_config.projection {
            config = config.with_some_projection(Some(projection.clone()));
        }

        let opener = BAMOpener::new(Arc::new(config));

        let stream = FileStream::new(&self.base_config, partition, opener, &self.metrics)?;

        Ok(Box::pin(stream) as SendableRecordBatchStream)
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}
