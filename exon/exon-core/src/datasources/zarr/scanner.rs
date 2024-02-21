use core::fmt;
use std::{any::Any, sync::Arc};

use arrow::datatypes::SchemaRef;
use datafusion::{
    datasource::physical_plan::{FileScanConfig, FileStream},
    physical_plan::{
        metrics::ExecutionPlanMetricsSet, DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning,
    },
};
use exon_zarr::ZarrConfig;

use super::file_opener::ZarrOpener;

#[derive(Debug, Clone)]
pub struct ZarrScan {
    base_config: FileScanConfig,
    projected_schema: SchemaRef,
    metrics: ExecutionPlanMetricsSet,
}

impl ZarrScan {
    pub fn new(base_config: FileScanConfig) -> Self {
        let (projected_schema, _, _) = base_config.project();

        Self {
            base_config,
            projected_schema,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl DisplayAs for ZarrScan {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        write!(f, "ZarrScan")
    }
}

impl ExecutionPlan for ZarrScan {
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

        let config = ZarrConfig::new(object_store, self.base_config.file_schema.clone());
        let opener = ZarrOpener::new(Arc::new(config));

        let stream = FileStream::new(&self.base_config, partition, opener, &self.metrics)?;

        Ok(Box::pin(stream) as datafusion::physical_plan::SendableRecordBatchStream)
    }
}
