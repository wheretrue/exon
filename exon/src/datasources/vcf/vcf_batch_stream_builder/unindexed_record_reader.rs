use std::sync::Arc;

use arrow::{datatypes::SchemaRef, error::ArrowError};
use futures::Stream;
use tokio::io::AsyncBufRead;

use crate::datasources::vcf::VCFSchemaBuilder;

/// A stream of records from an unindexed reader.
pub struct VCFUnindexedRecordStream<R> {
    reader: noodles::vcf::AsyncReader<R>,
    header: noodles::vcf::Header,
}

impl<R> VCFUnindexedRecordStream<R>
where
    R: AsyncBufRead + Unpin + Send,
{
    pub async fn new(inner: R) -> std::io::Result<Self> {
        let mut reader = noodles::vcf::AsyncReader::new(inner);
        let header = reader.read_header().await?;

        Ok(Self { reader, header })
    }

    async fn read_record(&mut self) -> std::io::Result<Option<noodles::vcf::Record>> {
        let mut record = noodles::vcf::Record::default();

        match self.reader.read_record(&self.header, &mut record).await? {
            0 => Ok(None),
            _ => Ok(Some(record)),
        }
    }

    pub fn get_arrow_schema(&self) -> SchemaRef {
        let schema = VCFSchemaBuilder::from(&self.header).build();

        Arc::new(schema)
    }

    pub fn into_record_stream(
        self,
    ) -> impl Stream<Item = Result<noodles::vcf::Record, ArrowError>> {
        futures::stream::unfold(self, |mut reader| async move {
            match reader.read_record().await {
                Ok(Some(record)) => Some((Ok(record), reader)),
                Ok(None) => None,
                Err(e) => Some((Err(ArrowError::ExternalError(Box::new(e))), reader)),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;

    use crate::{
        datasources::vcf::vcf_batch_stream_builder::unindexed_record_reader::VCFUnindexedRecordStream,
        tests::test_path,
    };

    #[tokio::test]
    async fn test_unindexed_record_stream() {
        let path = test_path("vcf", "index.vcf");
        let tokio_file = tokio::fs::File::open(path).await.unwrap();
        let tokio_bufreader = tokio::io::BufReader::new(tokio_file);

        let mut record_stream = VCFUnindexedRecordStream::new(tokio_bufreader)
            .await
            .unwrap()
            .into_record_stream()
            .boxed();

        let mut record_cnt = 0;
        while let Some(record) = record_stream.next().await {
            assert!(record.is_ok());
            record_cnt += 1;
        }
        assert_eq!(record_cnt, 621);
    }
}
