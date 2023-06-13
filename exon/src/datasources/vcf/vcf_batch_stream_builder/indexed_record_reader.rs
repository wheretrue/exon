use std::sync::Arc;

use arrow::{datatypes::SchemaRef, error::ArrowError};
use futures::{Stream, TryStreamExt};
use noodles::core::Region;
use tokio::io::{AsyncRead, AsyncSeek};

use crate::datasources::vcf::VCFSchemaBuilder;

/// A stream of records from an indexed reader.
/// pub struct VCFIndexedRecordStream<R> {
pub struct VCFIndexedRecordStream<R>
where
    R: AsyncRead + AsyncSeek + Unpin + Send,
{
    reader: noodles::vcf::AsyncReader<noodles::bgzf::AsyncReader<R>>,
    index: noodles::csi::Index,
    header: noodles::vcf::Header,
    region: Region,
}

impl<R> VCFIndexedRecordStream<R>
where
    R: AsyncRead + AsyncSeek + Unpin + Send,
{
    pub async fn new(
        inner: R,
        index: noodles::csi::Index,
        region: Region,
    ) -> std::io::Result<Self> {
        let mut reader = noodles::vcf::AsyncReader::new(noodles::bgzf::AsyncReader::new(inner));

        let header = reader.read_header().await?;

        Ok(Self {
            reader,
            index,
            header,
            region,
        })
    }

    pub fn into_record_stream(
        &mut self,
    ) -> impl Stream<Item = Result<noodles::vcf::Record, ArrowError>> + '_ {
        let query = self
            .reader
            .query(&self.header, &self.index, &self.region)
            .unwrap();

        let query_with_arrow = query.map_err(|f| {
            ArrowError::ExternalError(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                f.to_string(),
            )))
        });

        query_with_arrow
    }

    pub fn get_arrow_schema(&self) -> SchemaRef {
        let schema = VCFSchemaBuilder::from(&self.header).build();

        Arc::new(schema)
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;
    use noodles::core::Region;

    use crate::{
        datasources::vcf::vcf_batch_stream_builder::indexed_record_reader::VCFIndexedRecordStream,
        tests::test_path,
    };

    #[tokio::test]
    async fn test_indexed_record_stream() {
        let index_path = test_path("vcf", "index.vcf.gz.tbi");

        let index_file = tokio::fs::File::open(index_path).await.unwrap();
        let mut index_reader = noodles::tabix::AsyncReader::new(index_file);
        let index = index_reader.read_index().await.unwrap();

        let path = test_path("vcf", "index.vcf.gz");
        let tokio_file = tokio::fs::File::open(path).await.unwrap();
        let tokio_bufreader = tokio::io::BufReader::new(tokio_file);

        let region: Region = "1".parse().unwrap();
        let mut record_stream = VCFIndexedRecordStream::new(tokio_bufreader, index, region)
            .await
            .unwrap();

        let mut stream = record_stream.into_record_stream().boxed();

        let mut row_cnt = 0;
        while let Some(record) = stream.next().await {
            assert!(record.is_ok());
            row_cnt += 1;
        }
        assert_eq!(row_cnt, 191);
    }
}
