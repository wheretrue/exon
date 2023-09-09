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

use std::{any::Any, sync::Arc};

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::{
    common::FileCompressionType,
    datasource::{file_format::FileFormat, physical_plan::FileScanConfig},
    error::DataFusionError,
    execution::context::SessionState,
    physical_plan::{ExecutionPlan, PhysicalExpr, Statistics},
};
use futures::TryStreamExt;
use noodles::{bgzf, core::Region, csi::index::reference_sequence::bin::Chunk, vcf};
use object_store::{path::Path, ObjectMeta, ObjectStore};
use tokio_util::io::StreamReader;

use crate::physical_plan::{
    chrom_physical_expr::ChromPhysicalExpr, region_physical_expr::RegionPhysicalExpr,
};

use super::{scanner::VCFScan, schema_builder::VCFSchemaBuilder};

#[derive(Debug)]
/// Implements a datafusion `FileFormat` for VCF files.
pub struct VCFFormat {
    /// The compression type of the file.
    file_compression_type: FileCompressionType,

    /// A region to filter on, if known.
    region_filter: Option<Region>,
}

impl VCFFormat {
    /// Create a new VCFFormat.
    pub fn new(file_compression_type: FileCompressionType) -> Self {
        Self {
            file_compression_type,
            region_filter: None,
        }
    }

    /// Set the region to filter on.
    pub fn with_region_filter(mut self, region_filter: Region) -> Self {
        self.region_filter = Some(region_filter);
        self
    }
}

impl Default for VCFFormat {
    fn default() -> Self {
        Self {
            file_compression_type: FileCompressionType::UNCOMPRESSED,
            region_filter: None,
        }
    }
}

#[async_trait]
impl FileFormat for VCFFormat {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn infer_schema(
        &self,
        state: &SessionState,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> datafusion::error::Result<SchemaRef> {
        let get_result = store.get(&objects[0].location).await?;

        let stream_reader = Box::pin(get_result.into_stream().map_err(DataFusionError::from));
        let stream_reader = StreamReader::new(stream_reader);

        let exon_settings = state
            .config()
            .get_extension::<crate::config::ExonConfigExtension>();

        let parse_vcf_info = exon_settings
            .as_ref()
            .map(|s| s.parse_vcf_info)
            .unwrap_or(false);

        let parse_vcf_format = exon_settings
            .as_ref()
            .map(|s| s.parse_vcf_format)
            .unwrap_or(false);

        let mut schema_builder = match self.file_compression_type {
            FileCompressionType::GZIP => {
                let bgzf_reader = bgzf::AsyncReader::new(stream_reader);
                let mut vcf_reader = vcf::AsyncReader::new(bgzf_reader);

                let header = vcf_reader.read_header().await?;

                VCFSchemaBuilder::default()
                    .with_header(header)
                    .with_parse_info(parse_vcf_info)
                    .with_parse_formats(parse_vcf_format)
            }
            FileCompressionType::UNCOMPRESSED => {
                let mut vcf_reader = vcf::AsyncReader::new(stream_reader);

                let header = vcf_reader.read_header().await?;

                VCFSchemaBuilder::default()
                    .with_header(header)
                    .with_parse_info(parse_vcf_info)
                    .with_parse_formats(parse_vcf_format)
            }
            _ => {
                return Err(DataFusionError::Execution(
                    "Unsupported file compression type".to_string(),
                ))
            }
        };

        let schema = schema_builder.build()?;

        Ok(Arc::new(schema))
    }

    async fn infer_stats(
        &self,
        _state: &SessionState,
        _store: &Arc<dyn ObjectStore>,
        _table_schema: SchemaRef,
        _object: &ObjectMeta,
    ) -> datafusion::error::Result<Statistics> {
        Ok(Statistics::default())
    }

    async fn create_physical_plan(
        &self,
        _state: &SessionState,
        conf: FileScanConfig,
        filters: Option<&Arc<dyn PhysicalExpr>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        // if we got filters, we need to check if they are region filters
        if let Some(filters) = filters {
            // Downcast the filters to a Region filter if possible.
            if let Some(region_filter) = filters.as_any().downcast_ref::<RegionPhysicalExpr>() {
                let region = region_filter.region()?;
                let scan = VCFScan::new(conf, self.file_compression_type)?.with_filter(region);

                return Ok(Arc::new(scan));
            }

            // Downcast the filters to a Chrom filter if possible.
            if let Some(chrom_filter) = filters.as_any().downcast_ref::<ChromPhysicalExpr>() {
                let chrom = chrom_filter.chrom();
                let region = chrom.parse().map_err(|e| {
                    DataFusionError::Execution(format!("Invalid chrom: {:?}: {}", chrom, e))
                })?;
                let scan = VCFScan::new(conf, self.file_compression_type)?.with_filter(region);

                return Ok(Arc::new(scan));
            }
        }

        let scan = VCFScan::new(conf, self.file_compression_type)?;

        Ok(Arc::new(scan))
    }
}

pub async fn get_byte_range_for_file(
    object_store: Arc<dyn ObjectStore>,
    object_meta: &ObjectMeta,
    region: &Region,
) -> std::io::Result<Vec<Chunk>> {
    let tbi_path = object_meta.location.clone().to_string() + ".tbi";
    let tbi_path = Path::from(tbi_path);

    let index_bytes = object_store.get(&tbi_path).await?.bytes().await?;

    let cursor = std::io::Cursor::new(index_bytes);
    let index = noodles::tabix::Reader::new(cursor).read_index().unwrap();

    let (id, _) = resolve_region(&index, region).unwrap();
    let chunks = index.query(id, region.interval())?;

    Ok(chunks)
}

fn resolve_region(
    index: &noodles::csi::Index,
    region: &Region,
) -> std::io::Result<(usize, String)> {
    let header = index.header().ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "missing tabix header")
    })?;

    let i = header
        .reference_sequence_names()
        .get_index_of(region.name())
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "region reference sequence does not exist in reference sequences: {region:?}"
                ),
            )
        })?;

    Ok((i, region.name().into()))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{
        datasources::vcf::{
            table_provider::{ListingVCFTable, VCFListingTableConfig},
            ListingVCFTableOptions, VCFScan,
        },
        tests::test_path,
        ExonSessionExt,
    };

    use super::VCFFormat;
    use datafusion::{
        common::FileCompressionType,
        datasource::listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
        physical_plan::filter::FilterExec,
        prelude::SessionContext,
    };

    #[tokio::test]
    async fn test_region_pushdown() {
        let ctx = SessionContext::new_exon();
        let session_state = ctx.state();
        let table_path = test_path("vcf", "index.vcf.gz");

        let table_path = ListingTableUrl::parse(table_path.to_str().unwrap()).unwrap();

        let vcf_table_options = ListingVCFTableOptions::new(FileCompressionType::GZIP);

        let resolved_schema = vcf_table_options
            .infer_schema(&session_state, &table_path)
            .await
            .unwrap();

        let config = VCFListingTableConfig::new(table_path).with_options(vcf_table_options);

        let provider = Arc::new(ListingVCFTable::try_new(config, resolved_schema).unwrap());
        ctx.register_table("vcf_file", provider).unwrap();

        let sql_statements = vec![
            "SELECT * FROM vcf_file WHERE chrom = '1' AND pos = 100000;",
            "SELECT * FROM vcf_file WHERE chrom = '1' AND pos BETWEEN 100000 AND 200000;",
            "SELECT * FROM vcf_file WHERE chrom = '1'",
        ];

        for sql_statement in sql_statements {
            let df = ctx.sql(sql_statement).await.unwrap();

            let physical_plan = ctx
                .state()
                .create_physical_plan(df.logical_plan())
                .await
                .unwrap();

            if let Some(scan) = physical_plan.as_any().downcast_ref::<FilterExec>() {
                // Check the input is a VCF scan...
                if let Some(scan) = scan.input().as_any().downcast_ref::<VCFScan>() {
                    // ... and that it has a region filter.
                    assert!(scan.region_filter().is_some());
                } else {
                    panic!(
                        "expected VCFScan for {} in {:#?}",
                        sql_statement, physical_plan
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn test_vcf_parsing_string() {
        let ctx = SessionContext::new_exon();

        let table_path = test_path("vcf", "index.vcf");

        let sql = "SET exon.parse_vcf_info = true;";
        ctx.sql(sql).await.unwrap();

        let sql = "SET exon.parse_vcf_format = true;";
        ctx.sql(sql).await.unwrap();

        let sql = format!(
            "CREATE EXTERNAL TABLE vcf_file STORED AS VCF LOCATION '{}';",
            table_path.to_str().unwrap(),
        );
        ctx.sql(&sql).await.unwrap();

        let sql = "SELECT * FROM vcf_file WHERE chrom = '1' AND pos = 100000;";
        let df = ctx.sql(sql).await.unwrap();

        // Check that the last two columns are strings.
        let schema = df.schema();

        assert_eq!(schema.field(7).data_type().to_string(), "Utf8");
        assert_eq!(schema.field(8).data_type().to_string(), "Utf8");
    }

    #[tokio::test]
    async fn test_uncompressed_read() {
        let ctx = SessionContext::new_exon();
        let session_state = ctx.state();

        let table_path = ListingTableUrl::parse("test-data").unwrap();

        let vcf_format = Arc::new(VCFFormat::default());
        let lo = ListingOptions::new(vcf_format.clone()).with_file_extension("vcf");

        let resolved_schema = lo.infer_schema(&session_state, &table_path).await.unwrap();

        assert_eq!(resolved_schema.fields().len(), 9);
        assert_eq!(resolved_schema.field(0).name(), "chrom");

        let config = ListingTableConfig::new(table_path)
            .with_listing_options(lo)
            .with_schema(resolved_schema);

        let provider = Arc::new(ListingTable::try_new(config).unwrap());

        ctx.register_table("vcf_file", provider).unwrap();

        let df = ctx
            .sql("SELECT chrom, pos, id FROM vcf_file")
            .await
            .unwrap();

        let mut row_cnt = 0;
        let bs = df.collect().await.unwrap();
        for batch in bs {
            row_cnt += batch.num_rows();
        }
        assert_eq!(row_cnt, 621)
    }

    #[tokio::test]
    async fn test_compressed_read_with_region() {
        let ctx = SessionContext::new_exon();
        let session_state = ctx.state();

        let table_path = ListingTableUrl::parse("test-data").unwrap();

        let vcf_format = Arc::new(VCFFormat::new(FileCompressionType::GZIP));
        let lo = ListingOptions::new(vcf_format.clone()).with_file_extension("vcf.gz");

        let resolved_schema = lo.infer_schema(&session_state, &table_path).await.unwrap();

        assert_eq!(resolved_schema.fields().len(), 9);
        assert_eq!(resolved_schema.field(0).name(), "chrom");

        let config = ListingTableConfig::new(table_path)
            .with_listing_options(lo)
            .with_schema(resolved_schema);

        let provider = Arc::new(ListingTable::try_new(config).unwrap());
        ctx.register_table("vcf_file", provider).unwrap();

        let df = ctx
            .sql("SELECT chrom, pos FROM vcf_file WHERE chrom = 1")
            .await
            .unwrap();

        let mut row_cnt = 0;
        let bs = df.collect().await.unwrap();
        for batch in bs {
            row_cnt += batch.num_rows();
        }

        assert_eq!(row_cnt, 191)
    }
}
