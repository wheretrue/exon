// Copyright 2024 WHERE TRUE Technologies.
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

use std::{any::Any, sync::Arc};

use datafusion::{
    datasource::{
        file_format::{file_compression_type::FileCompressionType, write::BatchSerializer},
        physical_plan::FileSinkConfig,
    },
    error::DataFusionError,
    execution::{SendableRecordBatchStream, TaskContext},
    physical_plan::{insert::DataSink, metrics::MetricsSet, DisplayAs, DisplayFormatType},
};
use futures::StreamExt;
use tokio::io::AsyncWriteExt;

use crate::datasources::ExonFileType;

use super::{fasta_serializer::FASTASerializer, fastq_serializer::FASTQSerializer};

pub struct SimpleRecordSink {
    file_compression_type: FileCompressionType,
    file_sink_config: FileSinkConfig,
    exon_file_type: ExonFileType,
}

impl SimpleRecordSink {
    pub fn new(
        file_sink_config: FileSinkConfig,
        file_compression_type: FileCompressionType,
        exon_file_type: ExonFileType,
    ) -> Self {
        Self {
            file_sink_config,
            file_compression_type,
            exon_file_type,
        }
    }
}

use std::fmt::Debug;

impl Debug for SimpleRecordSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FASTADataSync").finish()
    }
}

impl DisplayAs for SimpleRecordSink {
    fn fmt_as(
        &self,
        _display_type: DisplayFormatType,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        write!(f, "ExonDataSync")
    }
}

#[async_trait::async_trait]
impl DataSink for SimpleRecordSink {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn metrics(&self) -> Option<MetricsSet> {
        None
    }

    async fn write_all(
        &self,
        mut data: SendableRecordBatchStream,
        context: &Arc<TaskContext>,
    ) -> Result<u64, DataFusionError> {
        let mut total_bytes = 0;

        let object_store = context
            .runtime_env()
            .object_store(&self.file_sink_config.object_store_url)?;

        let partition_file = &self.file_sink_config.file_groups[0];
        let location = partition_file.path();

        let buf_writer = object_store::buffered::BufWriter::new(object_store, location.clone());
        let mut buf_writer = self
            .file_compression_type
            .convert_async_writer(buf_writer)?;

        let serializer: Arc<dyn BatchSerializer> = match self.exon_file_type {
            ExonFileType::FASTA => Arc::new(FASTASerializer::default()),
            ExonFileType::FASTQ => Arc::new(FASTQSerializer::default()),
            _ => return Err(DataFusionError::Execution("Invalid file type".to_string())),
        };

        while let Some(batch) = data.next().await {
            let batch = batch?;
            let bytes = serializer.serialize(batch, false)?;

            buf_writer.write_all(&bytes).await?;
            buf_writer.flush().await?;

            total_bytes += bytes.len() as u64;
        }

        buf_writer.shutdown().await?;

        Ok(total_bytes)
    }
}

#[cfg(test)]
mod tests {
    use crate::datasources::fasta::table_provider::ListingFASTATableOptions;
    use crate::datasources::ExonFileType;
    use crate::sinks::SimpleRecordSink;
    use crate::ExonSession;

    use std::sync::Arc;

    use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
    use datafusion::datasource::listing::PartitionedFile;
    use datafusion::datasource::physical_plan::FileSinkConfig;
    use datafusion::execution::object_store::ObjectStoreUrl;
    use datafusion::physical_plan::insert::DataSink;

    #[tokio::test]
    async fn test_data_sink() -> Result<(), Box<dyn std::error::Error>> {
        let ctx = ExonSession::new_exon()?;

        let fasta_path = exon_test::test_path("fasta", "test.fa");

        let df = ctx
            .read_fasta(
                fasta_path.to_str().unwrap(),
                ListingFASTATableOptions::default().with_some_file_extension(Some("fa")),
            )
            .await
            .unwrap();

        let stream = df.execute_stream().await.unwrap();
        let output_schema = Arc::clone(&stream.schema());

        let p_file = PartitionedFile::new("/tmp/test.fa", 0);

        let object_store_url = ObjectStoreUrl::local_filesystem();
        let file_sink_config = FileSinkConfig {
            object_store_url,
            file_groups: vec![p_file],
            table_paths: vec![],
            output_schema,
            table_partition_cols: vec![],
            overwrite: false,
            keep_partition_by_columns: false,
        };

        let exon_file_type = ExonFileType::FASTA;
        let sink = SimpleRecordSink::new(
            file_sink_config,
            FileCompressionType::UNCOMPRESSED,
            exon_file_type,
        );

        let total_bytes = sink
            .write_all(stream, &ctx.session.task_ctx())
            .await
            .unwrap();

        assert_eq!(total_bytes, 41);

        Ok(())
    }
}
