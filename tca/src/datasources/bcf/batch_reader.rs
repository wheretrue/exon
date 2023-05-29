use std::sync::Arc;

use arrow::{error::ArrowError, record_batch::RecordBatch};

use noodles::bcf::lazy;
use tokio::io::{AsyncBufRead, AsyncRead};

use crate::datasources::vcf::VCFArrayBuilder;

use super::BCFConfig;

pub struct BatchReader<R>
where
    R: AsyncRead,
{
    /// The underlying BCF reader.
    reader: noodles::bcf::AsyncReader<noodles::bgzf::AsyncReader<R>>,

    /// Configuration for how to batch records.
    config: Arc<BCFConfig>,

    /// The VCF header.
    header: noodles::vcf::Header,

    /// The VCF header string maps.
    string_maps: noodles::bcf::header::StringMaps,
}

impl<R> BatchReader<R>
where
    R: AsyncBufRead + Unpin + Send,
{
    pub async fn new(inner: R, config: Arc<BCFConfig>) -> std::io::Result<Self> {
        let mut reader = noodles::bcf::AsyncReader::new(inner);
        reader.read_file_format().await?;

        let header_str = reader.read_header().await?;
        let header = header_str.parse::<noodles::vcf::Header>().map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("invalid header: {e}"),
            )
        })?;

        let string_maps = match header_str.parse() {
            Ok(string_maps) => string_maps,
            Err(e) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("invalid header: {e}"),
                ))
            }
        };

        Ok(Self {
            reader,
            config,
            header,
            string_maps,
        })
    }

    async fn read_record(&mut self) -> std::io::Result<Option<noodles::vcf::Record>> {
        let mut lazy_record = lazy::Record::default();

        match self.reader.read_lazy_record(&mut lazy_record).await? {
            0 => Ok(None),
            _ => {
                let vcf_record =
                    lazy_record.try_into_vcf_record(&self.header, &self.string_maps)?;

                Ok(Some(vcf_record))
            }
        }
    }

    async fn read_batch(&mut self) -> Result<Option<RecordBatch>, ArrowError> {
        let mut record_batch =
            VCFArrayBuilder::create(self.config.file_schema.clone(), self.config.batch_size)?;

        for _ in 0..self.config.batch_size {
            match self.read_record().await? {
                Some(record) => record_batch.append(&record),
                None => break,
            }
        }

        if record_batch.len() == 0 {
            return Ok(None);
        }

        let batch = RecordBatch::try_new(self.config.file_schema.clone(), record_batch.finish())?;

        match &self.config.projection {
            Some(projection) => Ok(Some(batch.project(&projection)?)),
            None => Ok(Some(batch)),
        }
    }

    /// Returns a stream of batches from the underlying BCF reader.
    pub fn into_stream(self) -> impl futures::Stream<Item = Result<RecordBatch, ArrowError>> {
        futures::stream::unfold(self, |mut reader| async move {
            match reader.read_batch().await {
                Ok(Some(batch)) => Some((Ok(batch), reader)),
                Ok(None) => None,
                Err(e) => Some((Err(e), reader)),
            }
        })
    }
}
