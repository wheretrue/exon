use std::sync::Arc;

use arrow::{error::ArrowError, record_batch::RecordBatch};

use futures::Stream;
use tokio::io::{AsyncBufRead, AsyncRead};

use super::{array_builder::SAMArrayBuilder, config::SAMConfig};

/// A batch reader for SAM files.
pub struct BatchReader<R>
where
    R: AsyncRead,
{
    /// The underlying SAM reader.
    reader: noodles::sam::AsyncReader<R>,

    /// The configuration for this reader.
    config: Arc<SAMConfig>,

    /// The header of the SAM file.
    header: noodles::sam::Header,
}

impl<R> BatchReader<R>
where
    R: AsyncBufRead + Unpin + Send + AsyncRead,
{
    pub async fn new(inner: R, config: Arc<SAMConfig>) -> std::io::Result<Self> {
        let mut reader = noodles::sam::AsyncReader::new(inner);

        let header = reader.read_header().await?;

        Ok(Self {
            reader,
            header,
            config,
        })
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

    async fn read_record(&mut self) -> std::io::Result<Option<noodles::sam::alignment::Record>> {
        let mut record = noodles::sam::alignment::Record::default();

        match self.reader.read_record(&self.header, &mut record).await? {
            0 => Ok(None),
            _ => Ok(Some(record)),
        }
    }

    async fn read_batch(&mut self) -> Result<Option<RecordBatch>, ArrowError> {
        let mut record_batch = SAMArrayBuilder::create(self.header.clone());

        for _ in 0..self.config.batch_size {
            match self.read_record().await? {
                Some(record) => record_batch.append(&record).map_err(|e| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("invalid record: {e}"),
                    )
                })?,
                None => break,
            }
        }

        if record_batch.len() == 0 {
            return Ok(None);
        }

        let batch = RecordBatch::try_new(self.config.file_schema.clone(), record_batch.finish())?;

        match self.config.projection {
            Some(ref projection) => Ok(Some(batch.project(&projection)?)),
            None => Ok(Some(batch)),
        }
    }
}
