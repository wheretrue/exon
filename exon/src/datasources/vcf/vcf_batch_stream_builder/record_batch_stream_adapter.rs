use std::sync::Arc;

use arrow::{
    datatypes::{Schema, SchemaRef},
    error::ArrowError,
    error::Result as ArrowResult,
    record_batch::RecordBatch,
};
use futures::{Stream, StreamExt};

use crate::datasources::vcf::VCFArrayBuilder;

// Adapts a stream of VCF records into a stream of record batches.
pub struct VCFRecordBatchStreamAdapter<R> {
    inner: R,
    batch_size: usize,
    schema_ref: Arc<Schema>,
    // slice of usize to indicate which columns to include in the
    projection: Option<Vec<usize>>,

    _done: bool,
}

impl<R> VCFRecordBatchStreamAdapter<R>
where
    R: Stream<Item = Result<noodles::vcf::Record, ArrowError>> + Unpin + Send,
{
    pub fn new(
        inner: R,
        schema_ref: SchemaRef,
        batch_size: usize,
        projection: Option<Vec<usize>>,
    ) -> Self {
        Self {
            inner,
            schema_ref,
            batch_size,
            projection,
            _done: false,
        }
    }

    pub async fn try_next(&mut self) -> Option<ArrowResult<RecordBatch>> {
        let mut record_batch_builder =
            VCFArrayBuilder::create(self.schema_ref.clone(), self.batch_size).unwrap();

        if self._done {
            return None;
        }

        for _ in 0..self.batch_size {
            if let Some(record) = self.inner.next().await {
                match record {
                    Ok(record) => {
                        record_batch_builder.append(&record);
                    }
                    Err(e) => return Some(Err(e)),
                }
            } else {
                self._done = true;
                break;
            }
        }

        if record_batch_builder.is_empty() {
            return None;
        }

        let batch =
            RecordBatch::try_new(self.schema_ref.clone(), record_batch_builder.finish()).unwrap();

        match &self.projection {
            Some(projection) => {
                let projected_batch = batch.project(projection).unwrap();

                Some(Ok(projected_batch))
            }
            None => Some(Ok(batch)),
        }
    }

    pub fn into_stream(self) -> impl Stream<Item = ArrowResult<RecordBatch>> {
        futures::stream::unfold(self, |mut adapter| async move {
            match adapter.try_next().await {
                Some(Ok(batch)) => Some((Ok(batch), adapter)),
                Some(Err(e)) => Some((Err(e), adapter)),
                None => None,
            }
        })
    }
}
