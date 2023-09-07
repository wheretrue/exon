use std::{pin::Pin, sync::Arc};

use arrow::{error::ArrowError, record_batch::RecordBatch};
use futures::{Stream, StreamExt};
use noodles::bam::lazy::Record;
use tokio::io::AsyncBufRead;

use super::{lazy_array_builder::BAMArrayBuilder, BAMConfig};

// pub struct RecordIterator {
pub struct LazyRecordIterator<R> {
    /// The underlying BAM reader.
    reader: noodles::bam::AsyncReader<R>,
}

impl<R> LazyRecordIterator<R>
where
    R: AsyncBufRead + Unpin + Send,
{
    pub fn new(reader: noodles::bam::AsyncReader<R>) -> Self {
        Self { reader }
    }

    pub async fn read_record(&mut self) -> std::io::Result<Option<noodles::bam::lazy::Record>> {
        let mut record = noodles::bam::lazy::Record::default();

        match self.reader.read_lazy_record(&mut record).await? {
            0 => Ok(None),
            _ => Ok(Some(record)),
        }
    }

    pub fn into_stream(self) -> impl Stream<Item = std::io::Result<Record>> {
        futures::stream::unfold(self, |mut reader| async move {
            match reader.read_record().await {
                Ok(Some(record)) => Some((Ok(record), reader)),
                Ok(None) => None,
                Err(e) => Some((Err(e), reader)),
            }
        })
    }
}

// Given an input stream of records, adapt it to a stream of `RecordBatch`s.
pub struct LazyBatchReader {
    stream: Pin<Box<dyn Stream<Item = std::io::Result<Record>> + Send>>,
    header: Arc<noodles::sam::Header>,
    config: Arc<BAMConfig>,
}

impl LazyBatchReader {
    pub fn new(
        stream: Pin<Box<dyn Stream<Item = std::io::Result<Record>> + Send>>,
        header: Arc<noodles::sam::Header>,
        config: Arc<BAMConfig>,
    ) -> Self {
        Self {
            stream,
            header,
            config,
        }
    }

    async fn read_batch(&mut self) -> Result<Option<RecordBatch>, ArrowError> {
        let mut record_batch = BAMArrayBuilder::try_new(
            self.config.file_schema.clone(),
            self.config.batch_size,
            self.config.projection.clone(),
            self.header.clone(),
        )?;

        for _ in 0..self.config.batch_size {
            let record = match self.stream.next().await {
                Some(Ok(record)) => record,
                Some(Err(e)) => return Err(ArrowError::ExternalError(Box::new(e))),
                None => break,
            };

            record_batch.append(&record)?;
        }

        if record_batch.is_empty() {
            return Ok(None);
        }

        let batch = RecordBatch::try_new(self.config.file_schema.clone(), record_batch.finish())?;

        match &self.config.projection {
            Some(projection) => Ok(Some(batch.project(projection)?)),
            None => Ok(Some(batch)),
        }
    }

    pub fn into_stream(self) -> impl Stream<Item = Result<RecordBatch, ArrowError>> {
        futures::stream::unfold(self, |mut reader| async move {
            match reader.read_batch().await {
                Ok(Some(batch)) => Some((Ok(batch), reader)),
                Ok(None) => None,
                Err(e) => Some((Err(ArrowError::ExternalError(Box::new(e))), reader)),
            }
        })
    }
}
