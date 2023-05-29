use std::sync::Arc;

use arrow::ffi_stream::{export_reader_into_raw, FFI_ArrowArrayStream as ArrowArrayStream};
use arrow::{datatypes::SchemaRef, error::ArrowError, record_batch::RecordBatchReader};
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::prelude::DataFrame;
use futures::StreamExt;

#[pin_project::pin_project]
/// A stream of record batches from a DataFrame.
pub struct DataFrameRecordBatchStream {
    #[pin]
    exec_node: SendableRecordBatchStream,

    rt: Arc<tokio::runtime::Runtime>,
}

impl DataFrameRecordBatchStream {
    /// Create a new DataFrameRecordBatchStream.
    pub fn new(exec_node: SendableRecordBatchStream, rt: Arc<tokio::runtime::Runtime>) -> Self {
        Self { exec_node, rt }
    }
}

impl Iterator for DataFrameRecordBatchStream {
    type Item = arrow::error::Result<arrow::record_batch::RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.rt.block_on(self.exec_node.next()) {
            Some(Ok(batch)) => Some(Ok(batch)),
            Some(Err(e)) => Some(Err(ArrowError::ExternalError(Box::new(e)))),
            None => None,
        }
    }
}

impl RecordBatchReader for DataFrameRecordBatchStream {
    fn schema(&self) -> SchemaRef {
        self.exec_node.schema()
    }
}

/// Create a stream of record batches from a DataFrame and export it to a raw pointer.
pub async fn create_dataset_stream_from_table_provider(
    dataframe: DataFrame,
    rt: Arc<tokio::runtime::Runtime>,
    stream_ptr: *mut ArrowArrayStream,
) {
    let stream = dataframe.execute_stream().await.unwrap();
    let dataset_record_batch_stream = DataFrameRecordBatchStream::new(stream, rt);

    unsafe {
        export_reader_into_raw(Box::new(dataset_record_batch_stream), stream_ptr);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::ffi_stream::ArrowArrayStreamReader;
    use arrow::record_batch::RecordBatchReader;
    use datafusion::prelude::SessionContext;

    use crate::context::TCASessionExt;
    use crate::ffi::create_dataset_stream_from_table_provider;
    use crate::ffi::ArrowArrayStream;

    use crate::tests::test_path;

    #[test]
    pub fn test() {
        let rt = Arc::new(tokio::runtime::Runtime::new().unwrap());

        let ctx = SessionContext::new();

        let path = test_path("mzml", "test.mzml");

        let mut stream_ptr = ArrowArrayStream::empty();

        rt.block_on(async {
            let df = ctx.read_mzml(path.to_str().unwrap(), None).await.unwrap();
            create_dataset_stream_from_table_provider(df, rt.clone(), &mut stream_ptr).await;
        });

        let stream_reader = unsafe { ArrowArrayStreamReader::from_raw(&mut stream_ptr).unwrap() };

        let imported_schema = stream_reader.schema();
        assert_eq!(imported_schema.field(0).name(), "id");

        let mut row_cnt = 0;
        for batch in stream_reader {
            let batch = batch.unwrap();

            row_cnt += batch.num_rows();
        }

        assert_eq!(row_cnt, 1);
    }
}
