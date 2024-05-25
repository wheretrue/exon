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

use std::{any::Any, fmt::Debug, io::Write, sync::Arc};

use bytes::{BufMut, Bytes, BytesMut};
use datafusion::{
    datasource::{file_format::write::BatchSerializer, physical_plan::FileSinkConfig},
    error::DataFusionError,
    execution::{SendableRecordBatchStream, TaskContext},
    physical_plan::{insert::DataSink, metrics::MetricsSet, DisplayAs, DisplayFormatType},
};
use futures::StreamExt;
use noodles::fasta::{
    record::{Definition, Sequence},
    Record,
};

#[derive(Debug)]
struct FASTASerializer {}

impl BatchSerializer for FASTASerializer {
    fn serialize(
        &self,
        batch: arrow::array::RecordBatch,
        initial: bool,
    ) -> datafusion::error::Result<bytes::Bytes> {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("ids should be a string array");

        let descriptions = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("descriptions should be a string array");

        let sequences = batch
            .column(2)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("sequences should be a string array");

        let b = Vec::new();
        let mut fasta_writer = noodles::fasta::writer::Writer::new(b);

        for i in 0..batch.num_rows() {
            let id = ids.value(i);
            let description = descriptions.value(i);
            let sequence = sequences.value(i);

            let definition = Definition::new(id, Some(Vec::from(description)));
            let sequence = Sequence::from(Vec::from(sequence));

            let record = Record::new(definition, sequence);
            fasta_writer.write_record(&record)?;
        }

        // todo benchmark this
        Ok(Bytes::from(fasta_writer.get_ref().to_vec()))
    }
}

impl DebugSerializer for FASTASerializer {}

trait DebugSerializer: BatchSerializer + std::fmt::Debug {}

struct FASTADataSync {
    file_sink_config: FileSinkConfig,
}

impl Debug for FASTADataSync {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FASTADataSync").finish()
    }
}

impl DisplayAs for FASTADataSync {
    fn fmt_as(
        &self,
        _display_type: DisplayFormatType,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        write!(f, "ExonDataSync")
    }
}

#[async_trait::async_trait]
impl DataSink for FASTADataSync {
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

        // let base_output_path = &self.file_sink_config.table_paths[0];
        let partition_file = &self.file_sink_config.file_groups[0];

        let serializer = FASTASerializer {};

        // let writer = object_store.put

        while let Some(batch) = data.next().await {
            let batch = batch?;

            let bytes = serializer.serialize(batch, false)?;
            total_bytes += bytes.len() as u64;

            let location = partition_file.path();
            object_store.put(location, bytes).await?;
        }

        Ok(total_bytes)
    }
}

#[cfg(test)]
mod tests {
    use crate::datasources::fasta::table_provider::ListingFASTATableOptions;
    use crate::sinks::FASTADataSync;
    use crate::ExonSessionExt;

    use datafusion::datasource::listing::PartitionedFile;
    use datafusion::datasource::physical_plan::FileSinkConfig;
    use datafusion::execution::context::SessionContext;
    use datafusion::execution::object_store::ObjectStoreUrl;
    use datafusion::physical_plan::insert::DataSink;

    #[tokio::test]
    async fn test_data_sink() -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new_exon();

        let fasta_path = exon_test::test_path("fasta", "test.fa");

        let df = ctx
            .read_fasta(
                fasta_path.to_str().unwrap(),
                ListingFASTATableOptions::default().with_some_file_extension(Some("fa")),
            )
            .await
            .unwrap();

        let stream = df.execute_stream().await.unwrap();
        let output_schema = stream.schema().clone();

        let p_file = PartitionedFile::new("/tmp/test.fa", 0);

        let object_store_url = ObjectStoreUrl::local_filesystem();
        let file_sink_config = FileSinkConfig {
            object_store_url,
            file_groups: vec![p_file],
            table_paths: vec![],
            output_schema,
            table_partition_cols: vec![],
            overwrite: false,
        };

        let sink = FASTADataSync { file_sink_config };

        let total_bytes = sink.write_all(stream, &ctx.task_ctx()).await.unwrap();

        assert_eq!(total_bytes, 0);

        Ok(())
    }
}
