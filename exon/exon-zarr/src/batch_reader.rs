use std::{path::PathBuf, sync::Arc};

use arrow::array::RecordBatch;
use arrow::error::Result as ArrowResult;
use arrow_zarr::reader::{
    ZarrIterator, ZarrRead, ZarrRecordBatchReader, ZarrRecordBatchReaderBuilder, ZarrStore,
};
use futures::Stream;

use crate::ZarrConfig;

pub struct BatchReader {
    reader: ZarrRecordBatchReader<ZarrStore<PathBuf>>,
    config: Arc<ZarrConfig>,
}

impl BatchReader {
    /// Creates a new batch reader.
    pub fn new(p: PathBuf, config: Arc<ZarrConfig>) -> Self {
        let builder = ZarrRecordBatchReaderBuilder::new(p);
        let reader = builder.build().unwrap();

        Self { config, reader }
    }

    pub fn into_stream(self) -> impl Stream<Item = ArrowResult<RecordBatch>> {
        futures::stream::unfold(self, |mut reader| async move {
            match reader.next() {
                Some(Ok(batch)) => Some((Ok(batch), reader)),
                Some(Err(e)) => Some((Err(e), reader)),
                None => None,
            }
        })
    }
}

impl Iterator for BatchReader {
    type Item = ArrowResult<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.reader.next() {
            Some(Ok(batch)) => Some(Ok(batch)),
            Some(Err(e)) => {
                Some(Err(arrow::error::ArrowError::ExternalError(Box::new(e))))
            }
            None => None,
        }
    }
}
