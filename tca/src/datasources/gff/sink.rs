use std::{
    fmt::Display,
    io::{BufWriter, Cursor, Write},
    sync::Arc,
};

use arrow::{
    array::{Float32Array, Int32Array, Int64Array, MapArray, StringArray},
    record_batch::{RecordBatch, RecordBatchWriter},
};
use async_trait::async_trait;
use bytes::Bytes;
use datafusion::{
    datasource::{
        file_format::{AsyncPutWriter, BatchSerializer, FileWriterMode},
        physical_plan::{FileMeta, FileSinkConfig},
    },
    execution::TaskContext,
    physical_plan::{insert::DataSink, SendableRecordBatchStream},
};
use futures::StreamExt;
use noodles::{
    bam::reader::record, core::Position, gff::Record, sam::record::reference_sequence_name,
};
use object_store::ObjectStore;
use tokio::io::{AsyncWrite, AsyncWriteExt};

struct GFFArrayWriter {
    seqid: StringArray,
    source: StringArray,
    ty: StringArray,
    start: Int64Array,
    end: Int64Array,
    score: Float32Array,
    strand: StringArray,
    phase: StringArray,
    attributes: MapArray,
}

impl GFFArrayWriter {
    pub fn get(&self, row_idx: usize) -> Record {
        let reference_sequence_name = self.seqid.value(row_idx);

        let start_position = Position::try_from(self.start.value(row_idx) as usize).unwrap();
        let end_position = Position::try_from(self.end.value(row_idx) as usize).unwrap();

        let mut record_builder = Record::builder()
            .set_reference_sequence_name(reference_sequence_name.to_string())
            .set_source(self.source.value(row_idx).to_string())
            .set_type(self.ty.value(row_idx).to_string())
            .set_start(start_position)
            .set_end(end_position);

        record_builder.build()
    }
}

impl TryFrom<RecordBatch> for GFFArrayWriter {
    type Error = arrow::error::ArrowError;

    fn try_from(value: RecordBatch) -> Result<Self, Self::Error> {
        let seqid_column = value
            .column_by_name("seqid")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        let source_column = value
            .column_by_name("source")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        let ty_column = value
            .column_by_name("type")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        let start_column = value
            .column_by_name("start")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        let end_column = value
            .column_by_name("end")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        let score_column = value
            .column_by_name("score")
            .unwrap()
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();

        let strand_column = value
            .column_by_name("strand")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        let phase_column = value
            .column_by_name("phase")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        let attributes_column = value
            .column_by_name("attributes")
            .unwrap()
            .as_any()
            .downcast_ref::<MapArray>()
            .unwrap();

        Ok(GFFArrayWriter {
            seqid: seqid_column.clone(),
            source: source_column.clone(),
            ty: ty_column.clone(),
            start: start_column.clone(),
            end: end_column.clone(),
            score: score_column.clone(),
            strand: strand_column.clone(),
            phase: phase_column.clone(),
            attributes: attributes_column.clone(),
        })
    }
}

// https://github.com/apache/arrow-rs/blob/a88dc75100dd682ca873850a434e476b7d7f8404/arrow-csv/src/writer.rs#L82
pub struct GFFWriter<W: Write> {
    writer: BufWriter<W>,
}

impl<W> GFFWriter<W>
where
    W: Write,
{
    pub fn new(writer: W) -> Self {
        Self {
            writer: BufWriter::new(writer),
        }
    }
}

impl<W> RecordBatchWriter for GFFWriter<W>
where
    W: Write,
{
    fn write(&mut self, batch: &RecordBatch) -> Result<(), arrow::error::ArrowError> {
        let n_records = batch.num_rows();

        let gff_array_writer = GFFArrayWriter::try_from(batch.clone())?;

        for row_idx in 0..n_records {
            let record = gff_array_writer.get(row_idx);

            let gff_line = record.to_string() + "\n";

            self.writer.write_all(gff_line.as_bytes())?;
        }
        self.writer.flush()?;

        Ok(())
    }

    fn close(self) -> Result<(), arrow::error::ArrowError> {
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct GFFWriterBuilder {}

impl GFFWriterBuilder {
    pub fn new() -> Self {
        Self {}
    }

    pub fn build<W: Write>(&self, writer: W) -> datafusion::error::Result<GFFWriter<W>> {
        Ok(GFFWriter::new(writer))
    }
}

pub struct GFFSerializer {
    builder: GFFWriterBuilder,
    buffer: Vec<u8>,
}

impl GFFSerializer {
    pub fn new() -> Self {
        Self {
            builder: GFFWriterBuilder::new(),
            buffer: Vec::with_capacity(4096),
        }
    }
}

#[async_trait]
impl BatchSerializer for GFFSerializer {
    async fn serialize(&mut self, batch: RecordBatch) -> datafusion::error::Result<Bytes> {
        let builder = self.builder.clone();

        let mut writer = builder.build(&mut self.buffer)?;

        writer.write(&batch)?;
        drop(writer);

        Ok(Bytes::from(self.buffer.drain(..).collect::<Vec<u8>>()))
    }
}

#[derive(Debug)]
pub struct GFFSink {
    config: FileSinkConfig,
}

impl Display for GFFSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}

impl GFFSink {
    pub fn new(config: FileSinkConfig) -> Self {
        Self { config }
    }

    async fn create_writer(
        &self,
        file_meta: FileMeta,
        object_store: Arc<dyn ObjectStore>,
    ) -> datafusion::error::Result<Box<dyn AsyncWrite + Send + Unpin>> {
        let object = &file_meta.object_meta;

        eprintln!("Creating writer for {:?}", object.location);

        match self.config.writer_mode {
            FileWriterMode::Append => Err(datafusion::error::DataFusionError::NotImplemented(
                "GFF does not support append mode".to_string(),
            )),
            FileWriterMode::PutMultipart => {
                Err(datafusion::error::DataFusionError::NotImplemented(
                    "GFF does not support multipart mode".to_string(),
                ))
            }
            FileWriterMode::Put => {
                let writer = Box::new(AsyncPutWriter::new(object.clone(), object_store));
                // Handle compression here?
                Ok(writer)
            }
        }
    }
}

#[async_trait]
impl DataSink for GFFSink {
    async fn write_all(
        &self,
        mut data: SendableRecordBatchStream,
        context: &Arc<TaskContext>,
    ) -> datafusion::error::Result<u64> {
        let num_partitions = self.config.file_groups.len();
        if num_partitions != 1 {
            return Err(datafusion::error::DataFusionError::NotImplemented(
                "GFF only supports writing to a single file".to_string(),
            ));
        }

        eprintln!("Writing GFF file...");

        let object_store = context
            .runtime_env()
            .object_store(&self.config.object_store_url)?;

        let file = &self.config.file_groups[0];
        eprintln!("Writing to {:?}", file);

        let mut serializer = GFFSerializer::new();
        // let mut writer = self
        //     .create_writer(file.object_meta.clone().into(), object_store.clone())
        //     .await?;

        let location = file.object_meta.location.clone();

        let mut idx = 0;
        let mut row_count = 0;

        while let Some(maybe_batch) = data.next().await {
            idx = (idx + 1) % num_partitions;

            let batch = maybe_batch?;

            row_count += batch.num_rows();

            let bytes = serializer.serialize(batch).await?;
            let n_bytes = bytes.len();
            eprintln!("Writing {} bytes", n_bytes);

            object_store.put(&location, bytes).await?;
        }

        Ok(row_count as u64)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::{
        datasource::{
            file_format::BatchSerializer,
            listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
        },
        prelude::SessionContext,
    };

    use crate::{
        context::TCASessionExt,
        datasources::gff::{sink::GFFSerializer, GFFFormat},
        tests::test_path,
    };

    #[tokio::test]
    async fn test_serializer() -> datafusion::error::Result<()> {
        let ctx = SessionContext::new();

        let path = test_path("gff", "test.gff");
        let gff_df = ctx.read_gff(path.to_str().unwrap(), None).await?;
        let gff_df = gff_df.limit(0, Some(2))?;

        let gff_batches = gff_df.collect().await?;

        let mut serializer = GFFSerializer::new();

        let bytes = serializer.serialize(gff_batches[0].clone()).await?;
        assert_eq!(
            "sq0\tcaat\tgene\t8\t13\t.\t.\t.\t.\nsq0\tcaat\tgene\t8\t13\t.\t.\t.\t.\n",
            String::from_utf8(bytes.into()).unwrap()
        );

        Ok(())
    }
}
