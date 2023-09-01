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
    datasource::{
        file_format::FileFormat,
        listing::{FileRange, PartitionedFile},
        physical_plan::FileScanConfig,
    },
    error::DataFusionError,
    execution::context::SessionState,
    physical_plan::{expressions::BinaryExpr, ExecutionPlan, PhysicalExpr, Statistics},
};
use futures::TryStreamExt;
use noodles::{bgzf, core::Region, vcf};
use object_store::{path::Path, ObjectMeta, ObjectStore};
use tokio_util::io::StreamReader;

use crate::{
    physical_optimizer::region_between_rewriter::transform_interval_expression,
    physical_plan::region_physical_expr::RegionPhysicalExpr,
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
        _state: &SessionState,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> datafusion::error::Result<SchemaRef> {
        let get_result = store.get(&objects[0].location).await?;

        let stream_reader = Box::pin(get_result.into_stream().map_err(DataFusionError::from));
        let stream_reader = StreamReader::new(stream_reader);

        let schema_builder = match self.file_compression_type {
            FileCompressionType::GZIP => {
                let bgzf_reader = bgzf::AsyncReader::new(stream_reader);
                let mut vcf_reader = vcf::AsyncReader::new(bgzf_reader);

                let header = vcf_reader.read_header().await?;

                VCFSchemaBuilder::from(header)
            }
            FileCompressionType::UNCOMPRESSED => {
                let mut vcf_reader = vcf::AsyncReader::new(stream_reader);

                let header = vcf_reader.read_header().await?;

                VCFSchemaBuilder::from(header)
            }
            _ => {
                return Err(DataFusionError::Execution(
                    "Unsupported file compression type".to_string(),
                ))
            }
        };

        let schema = schema_builder.build();

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
        state: &SessionState,
        conf: FileScanConfig,
        filters: Option<&Arc<dyn PhysicalExpr>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        // Make 5 passes through the filters and to try to optimize into a region filter.

        let new_filters = match filters {
            Some(filter) => match filter.as_any().downcast_ref::<BinaryExpr>() {
                Some(be) => transform_interval_expression(be),
                None => None,
            },
            _ => None,
        };

        let new_filters = match new_filters {
            Some(f) => match f.as_any().downcast_ref::<BinaryExpr>() {
                Some(be) => Some(RegionPhysicalExpr::try_from(be.clone())?),
                None => None,
            },
            _ => None,
        };

        eprintln!("Got new filters: {:#?}", new_filters);

        if let Some(region_filter) = new_filters {
            let mut new_conf = conf.clone();

            let object_store = state.runtime_env().object_store(&conf.object_store_url)?;

            let new_groups = add_region_bytes_to_file_groups(
                object_store,
                &conf.file_groups,
                &region_filter.region(),
            )
            .await;

            new_conf.file_groups = new_groups;

            let mut scan = VCFScan::new(new_conf, self.file_compression_type)?;
            scan = scan.with_filter(region_filter.region().clone());

            return Ok(Arc::new(scan));
        } else {
            let scan = VCFScan::new(conf, self.file_compression_type)?;
            return Ok(Arc::new(scan));
        }
    }
}

pub async fn add_region_bytes_to_file_groups(
    object_store: Arc<dyn ObjectStore>,
    file_groups: &Vec<Vec<PartitionedFile>>,
    region: &Region,
) -> Vec<Vec<PartitionedFile>> {
    let mut new_list = Vec::new();

    // iterate through the nested list of files
    for file_group in file_groups {
        // iterate through the files in the file group
        for file in file_group {
            let tbi_path = file.object_meta.location.clone().to_string() + ".tbi";
            let tbi_path = Path::from(tbi_path);

            let index_bytes = object_store.get(&tbi_path).await.unwrap();
            let index_bytes = index_bytes.bytes().await.unwrap();

            let cursor = std::io::Cursor::new(index_bytes);
            let index = noodles::tabix::Reader::new(cursor).read_index().unwrap();

            let (id, _) = resolve_region(&index, region).unwrap();
            let chunks = index.query(id, region.interval()).unwrap();

            for chunk in chunks {
                let start = chunk.start().compressed();
                let end = chunk.end().compressed();

                let mut new_file = file.clone();
                new_file.range = Some(FileRange {
                    start: start as i64,
                    end: end as i64,
                });

                eprintln!("new file: {:?}", new_file);

                new_list.push(new_file);
            }
        }
    }

    vec![new_list]
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

    use crate::{datasources::vcf::VCFScan, tests::test_path, ExonSessionExt};

    use super::VCFFormat;
    use aws_config::default_provider::region;
    use datafusion::{
        common::FileCompressionType,
        datasource::listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
        prelude::SessionContext,
    };

    #[tokio::test]
    async fn test_region_pushdown() {
        let ctx = SessionContext::new_exon();

        let table_path = test_path("vcf", "index.vcf");

        let sql = format!(
            "CREATE EXTERNAL TABLE vcf_file STORED AS VCF LOCATION '{}';",
            table_path.to_str().unwrap(),
        );
        ctx.sql(&sql).await.unwrap();

        let sql = "SELECT * FROM vcf_file WHERE chrom = '1' AND pos = 10000;";

        let df = ctx.sql(sql).await.unwrap();

        let physical_plan = ctx
            .state()
            .create_physical_plan(df.logical_plan())
            .await
            .unwrap();

        let scan = physical_plan.as_any().downcast_ref::<VCFScan>();
        assert!(scan.is_some());
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

        let table_path = ListingTableUrl::parse(
            "/Users/thauck/wheretrue/github.com/wheretrue/exon/exon-benchmarks/data",
        )
        .unwrap();

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
            .sql(
                "SELECT chrom, pos FROM vcf_file WHERE chrom = 'chr1' and pos BETWEEN 1 and 10000000",
            )
            .await
            .unwrap();

        // let ph_plan = df.create_physical_plan().await.unwrap();
        // eprintln!("Got physical plan: {:#?}", ph_plan);

        let mut row_cnt = 0;
        let bs = df.collect().await.unwrap();
        for batch in bs {
            row_cnt += batch.num_rows();
        }
        assert!(true);
        assert_eq!(row_cnt, 191)
    }
}
