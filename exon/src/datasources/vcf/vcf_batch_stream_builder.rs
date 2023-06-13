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

use arrow::{error::Result as ArrowResult, record_batch::RecordBatch};

use futures::{Stream, StreamExt};
use noodles::core::Region;
use tokio::io::AsyncBufRead;

use self::{
    record_batch_stream_adapter::VCFRecordBatchStreamAdapter,
    unindexed_record_reader::VCFUnindexedRecordStream,
};

mod indexed_record_reader;
mod record_batch_stream_adapter;
mod unindexed_record_reader;

pub struct VCFRecordBatchStreamBuilder<R> {
    input_stream: R,
    projection: Option<Vec<usize>>,
    batch_size: usize,
    region: Option<Region>,
    index_path: Option<String>,
}

impl<R> VCFRecordBatchStreamBuilder<R>
where
    R: AsyncBufRead + Unpin + Send + 'static,
{
    pub fn new(input_stream: R) -> Self {
        Self {
            input_stream,
            projection: None,
            batch_size: 1024,
            region: None,
            index_path: None,
        }
    }

    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    fn try_build_indexed_stream(&self) {
        // let mut_idx_stream = VCFIndexedRecordStream::new(
    }

    pub async fn try_build_unindexed_stream(self) -> impl Stream<Item = ArrowResult<RecordBatch>> {
        let mut_idx_stream = VCFUnindexedRecordStream::new(self.input_stream)
            .await
            .unwrap();

        let schema = mut_idx_stream.get_arrow_schema();

        let adapter = VCFRecordBatchStreamAdapter::new(
            mut_idx_stream.into_record_stream().boxed(),
            schema.clone(),
            self.batch_size,
            self.projection,
        );

        let stream = adapter.into_stream().boxed();

        stream
    }

    pub fn with_region(mut self, region: Region) -> Self {
        self.region = Some(region);
        self
    }

    pub fn with_index_path(mut self, index_path: String) -> Self {
        self.index_path = Some(index_path);
        self
    }
}

#[cfg(test)]
mod tests {

    use futures::StreamExt;
    use noodles::core::Region;

    use crate::{
        datasources::vcf::vcf_batch_stream_builder::{
            indexed_record_reader::VCFIndexedRecordStream, VCFRecordBatchStreamAdapter,
            VCFRecordBatchStreamBuilder, VCFUnindexedRecordStream,
        },
        tests::test_path,
    };

    #[tokio::test]
    async fn test_indexed_batch_stream() {
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

        let arrow_schema = record_stream.get_arrow_schema();

        let mut stream = record_stream.into_record_stream().boxed();

        let mut batch_stream = VCFRecordBatchStreamAdapter::new(stream, arrow_schema, 2, None)
            .into_stream()
            .boxed();

        let mut record_batch_cnt = 0;
        while let Some(record_batch) = batch_stream.next().await {
            record_batch.unwrap();
            record_batch_cnt += 1;
        }

        assert_eq!(record_batch_cnt, 96);
    }

    #[tokio::test]
    async fn test_unindexed_batch_stream() {
        let path = test_path("vcf", "index.vcf");
        let tokio_file = tokio::fs::File::open(path).await.unwrap();
        let tokio_bufreader = tokio::io::BufReader::new(tokio_file);

        let record_stream = VCFUnindexedRecordStream::new(tokio_bufreader)
            .await
            .unwrap();

        let arrow_schema = record_stream.get_arrow_schema();

        let record_stream = record_stream.into_record_stream().boxed();

        let mut batch_stream =
            VCFRecordBatchStreamAdapter::new(record_stream, arrow_schema, 2, None)
                .into_stream()
                .boxed();

        let mut record_batch_cnt = 0;
        while let Some(record_batch) = batch_stream.next().await {
            assert!(record_batch.is_ok());
            record_batch_cnt += 1;
        }

        assert_eq!(record_batch_cnt, 311);
    }

    #[tokio::test]
    async fn test_unindexed_batch_stream_via_builder() {
        let path = test_path("vcf", "index.vcf");
        let tokio_file = tokio::fs::File::open(path).await.unwrap();
        let tokio_bufreader = tokio::io::BufReader::new(tokio_file);

        let a = VCFRecordBatchStreamBuilder::new(tokio_bufreader).with_batch_size(2);

        let mut stream = a.try_build_unindexed_stream().await;

        let mut record_batch_cnt = 0;
        while let Some(record_batch) = stream.next().await {
            assert!(record_batch.is_ok());
            record_batch_cnt += 1;
        }

        assert_eq!(record_batch_cnt, 311);
    }
}
