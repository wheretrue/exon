use std::sync::Arc;

use arrow::error::Result as ArrowResult;
use arrow::{error::ArrowError, record_batch::RecordBatch};
use noodles::fastq;

use tokio::io::AsyncBufRead;

use super::{array_builder::FASTQArrayBuilder, FASTQConfig};

pub struct BatchReader<R> {
    /// The underlying FASTQ reader.
    reader: noodles::fastq::AsyncReader<R>,
    /// The FASTQ configuration.
    config: Arc<FASTQConfig>,
}

impl<R> BatchReader<R>
where
    R: AsyncBufRead + Unpin + Send,
{
    pub fn new(inner: R, config: Arc<FASTQConfig>) -> Self {
        Self {
            reader: noodles::fastq::AsyncReader::new(inner),
            config,
        }
    }

    /// Stream built `RecordBatch`es from the underlying FASTQ reader.
    pub fn into_stream(self) -> impl futures::Stream<Item = Result<RecordBatch, ArrowError>> {
        futures::stream::try_unfold(self, |mut reader| async move {
            match reader.read_batch(reader.config.batch_size).await? {
                Some(batch) => Ok(Some((batch, reader))),
                None => Ok(None),
            }
        })
    }

    async fn read_record(&mut self) -> std::io::Result<Option<noodles::fastq::Record>> {
        let mut record = fastq::Record::default();

        match self.reader.read_record(&mut record).await? {
            0 => Ok(None),
            _ => Ok(Some(record)),
        }
    }

    async fn read_batch(&mut self, batch_size: usize) -> ArrowResult<Option<RecordBatch>> {
        let mut array = FASTQArrayBuilder::create();

        for _ in 0..batch_size {
            match self.read_record().await? {
                Some(record) => array.append(&record)?,
                None => break,
            }
        }

        if array.len() == 0 {
            return Ok(None);
        }

        let batch = RecordBatch::try_new(self.config.file_schema.clone(), array.finish())?;

        match &self.config.projections {
            Some(projection) => {
                let projected_batch = batch.project(projection)?;

                Ok(Some(projected_batch))
            }
            None => Ok(Some(batch)),
        }
    }
}
