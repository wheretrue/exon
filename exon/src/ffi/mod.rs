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
) -> Result<(), ArrowError> {
    let stream = dataframe.execute_stream().await?;
    let dataset_record_batch_stream = DataFrameRecordBatchStream::new(stream, rt);

    unsafe {
        export_reader_into_raw(Box::new(dataset_record_batch_stream), stream_ptr);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::ffi_stream::ArrowArrayStreamReader;
    use arrow::record_batch::RecordBatchReader;
    use datafusion::error::DataFusionError;
    use datafusion::prelude::SessionContext;

    use crate::context::ExonSessionExt;
    use crate::ffi::create_dataset_stream_from_table_provider;
    use crate::ffi::ArrowArrayStream;

    use crate::tests::test_path;

    #[test]
    pub fn test() -> Result<(), DataFusionError> {
        let rt = Arc::new(tokio::runtime::Runtime::new().unwrap());

        let ctx = SessionContext::new();

        let path = test_path("fasta", "test.fasta");

        let mut stream_ptr = ArrowArrayStream::empty();

        rt.block_on(async {
            let df = ctx.read_fasta(path.to_str().unwrap(), None).await.unwrap();
            create_dataset_stream_from_table_provider(df, rt.clone(), &mut stream_ptr)
                .await
                .unwrap();
        });

        let stream_reader = unsafe { ArrowArrayStreamReader::from_raw(&mut stream_ptr)? };

        let imported_schema = stream_reader.schema();
        assert_eq!(imported_schema.field(0).name(), "id");

        let mut row_cnt = 0;
        for batch in stream_reader {
            let batch = batch?;

            row_cnt += batch.num_rows();
        }

        assert_eq!(row_cnt, 1);

        Ok(())
    }
}
