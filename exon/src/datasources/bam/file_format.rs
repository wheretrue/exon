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
use noodles::{
    bgzf::VirtualPosition,
    core::Region,
    csi::index::reference_sequence,
    sam::{header::ReferenceSequences, Header},
};
use object_store::{path::Path, GetResult, ObjectMeta, ObjectStore};
use tokio_util::io::StreamReader;

use crate::physical_plan::reference_physical_expr::ReferencePhysicalExpr;

use super::{
    array_builder::schema,
    indexed::{
        file_opener::{FileHeader, HeaderCache},
        scanner::IndexedBAMScan,
    },
    unindexed::scanner::UnIndexedBAMScan,
};

#[derive(Debug, Default)]
/// Implements a datafusion `FileFormat` for BAM files.
pub struct BAMFormat {
    /// An optional region filter for the scan.
    region_filter: Option<Region>,
}

impl BAMFormat {
    /// Create a new BAM format.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the region filter for the scan.
    pub fn with_region_filter(mut self, region_filter: Region) -> Self {
        self.region_filter = Some(region_filter);
        self
    }
}

async fn get_header_and_size(
    object_store: Arc<dyn ObjectStore>,
    location: &Path,
) -> Result<(Arc<Header>, VirtualPosition), DataFusionError> {
    let get_result = object_store.get(location).await?;

    match get_result {
        GetResult::File(file, _) => {
            let mut reader = noodles::bam::Reader::new(file);

            let header = reader.read_header()?;

            let vp = reader.virtual_position();

            Ok((Arc::new(header), vp))
        }
        GetResult::Stream(s) => {
            let stream_reader = Box::pin(s.map_err(DataFusionError::from));

            let stream_reader = StreamReader::new(stream_reader);
            let mut reader = noodles::bam::AsyncReader::new(stream_reader);

            let header = reader.read_header().await?;
            let header: Header = header.parse().map_err(|e| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("invalid header: {e}"),
                )
            })?;

            let vp = reader.virtual_position();

            Ok((Arc::new(header), vp))
        }
    }
}

pub(crate) fn resolve_region(
    reference_sequences: &ReferenceSequences,
    region: &Region,
) -> std::io::Result<usize> {
    reference_sequences
        .get_index_of(region.name())
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "region reference sequence does not exist in reference sequences: {region:?}"
                ),
            )
        })
}

#[async_trait]
impl FileFormat for BAMFormat {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn infer_schema(
        &self,
        _state: &SessionState,
        _store: &Arc<dyn ObjectStore>,
        _objects: &[ObjectMeta],
    ) -> datafusion::error::Result<SchemaRef> {
        let schema = schema();

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
        let new_filters = match filters {
            Some(filter) => match filter.as_any().downcast_ref::<BinaryExpr>() {
                Some(be) => be,
                None => {
                    let scan = UnIndexedBAMScan::try_new(conf)?;
                    return Ok(Arc::new(scan));
                }
            },
            _ => {
                let scan = UnIndexedBAMScan::try_new(conf)?;
                return Ok(Arc::new(scan));
            }
        };

        let reference_physical_expr = ReferencePhysicalExpr::try_from(new_filters.clone()).unwrap();
        // let region = reference_physical_expr.region();
        let region: Region = "20:1-100000000".parse().unwrap();

        let object_store = state.runtime_env().object_store(&conf.object_store_url)?;

        // Store a hashmap from file location to the tuple of header and vp
        let mut header_cache = HeaderCache::new();

        // These will hold the new partitioned files, which should include ranges from the index
        let mut new_partitioned_files = Vec::new();

        // Get the header for every file along with the vp of the first record
        for f in conf.file_groups.iter().flatten() {
            let location = f.object_meta.location.clone();
            let index_location = Path::from(location.to_string() + ".bai");

            let (header, vp) = get_header_and_size(object_store.clone(), &location).await?;

            header_cache.insert(
                location,
                FileHeader {
                    header: header.clone(),
                    offset: vp,
                },
            );

            let bytes = object_store.get(&index_location).await?.bytes().await?;
            let cursor = std::io::Cursor::new(bytes);

            let mut index = noodles::bam::bai::Reader::new(cursor);
            index.read_header()?;

            let index = index.read_index()?;

            let reference_sequences = header.reference_sequences().clone();
            let reference_sequence_id = resolve_region(&reference_sequences, &region)?;

            let chunks = index.query(reference_sequence_id, region.interval())?;

            for chunk in chunks {
                let start = chunk.start().compressed();
                let end = chunk.end().compressed();

                let mut new_file = f.clone();
                new_file.range = Some(FileRange {
                    start: start as i64,
                    end: end as i64,
                });
                eprintln!("new_file: {:#?}", new_file);

                new_partitioned_files.push(new_file);
            }
        }

        let mut new_conf = conf.clone();
        new_conf.file_groups = vec![new_partitioned_files];

        let scanner = IndexedBAMScan::try_new(new_conf, Arc::new(header_cache))?;

        Ok(Arc::new(scanner))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{tests::test_path, ExonSessionExt};

    use super::BAMFormat;
    use datafusion::{
        datasource::listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
        prelude::SessionContext,
    };
    use noodles::core::Region;

    #[tokio::test]
    async fn test_region_pushdown() {
        let ctx = SessionContext::new_exon();

        // let table_path = test_path("bam", "test.bam").to_str().unwrap()
        // let table_path = "/Users/thauck/wheretrue/github.com/wheretrue/exon/exon/test-data/datasources/bam/test.bam";
        let table_path = "/Users/thauck/wheretrue/github.com/wheretrue/exon/exon-benchmarks/data/HG00096.chrom20.ILLUMINA.bwa.GBR.low_coverage.20120522.bam";

        let sql = format!(
            "CREATE EXTERNAL TABLE bam_file STORED AS BAM LOCATION '{}';",
            table_path,
        );
        ctx.sql(&sql).await.unwrap();

        let sql_statements = vec!["SELECT * FROM bam_file WHERE reference = '20'"];

        for sql_statement in sql_statements {
            let df = ctx.sql(sql_statement).await.unwrap();

            let batches = df.collect().await.unwrap();

            // let physical_plan = ctx
            //     .state()
            //     .create_physical_plan(df.logical_plan())
            //     .await
            //     .unwrap();

            // eprintln!("physical_plan: {:#?}", physical_plan);

            // if let Some(scan) = physical_plan.as_any().downcast_ref::<FilterExec>() {
            //     scan.input().as_any().downcast_ref::<BAMScan>().unwrap();
            // } else {
            //     panic!(
            //         "expected FilterExec for {} in {:#?}",
            //         sql_statement, physical_plan
            //     );
            // }
        }
    }

    #[tokio::test]
    async fn test_read_bam() {
        let ctx = SessionContext::new();
        let session_state = ctx.state();

        let table_path = ListingTableUrl::parse("test-data").unwrap();

        let bam_format = Arc::new(BAMFormat::default());
        let lo = ListingOptions::new(bam_format.clone()).with_file_extension("bam");

        let resolved_schema = lo.infer_schema(&session_state, &table_path).await.unwrap();

        let config = ListingTableConfig::new(table_path)
            .with_listing_options(lo)
            .with_schema(resolved_schema);

        let provider = Arc::new(ListingTable::try_new(config).unwrap());
        let df = ctx.read_table(provider.clone()).unwrap();

        let mut row_cnt = 0;
        let bs = df.collect().await.unwrap();
        for batch in bs {
            row_cnt += batch.num_rows();
        }
        assert_eq!(row_cnt, 61)
    }

    #[tokio::test]
    async fn test_read_with_index() {
        let ctx = SessionContext::new();
        let session_state = ctx.state();

        let table_path = ListingTableUrl::parse("test-data").unwrap();

        let region: Region = "chr1:1-12209153".parse().unwrap();
        let fasta_format = Arc::new(BAMFormat::default().with_region_filter(region));

        let lo = ListingOptions::new(fasta_format.clone()).with_file_extension("bam");

        let resolved_schema = lo.infer_schema(&session_state, &table_path).await.unwrap();

        let config = ListingTableConfig::new(table_path)
            .with_listing_options(lo)
            .with_schema(resolved_schema);

        let provider = Arc::new(ListingTable::try_new(config).unwrap());
        let df = ctx.read_table(provider.clone()).unwrap();

        let mut row_cnt = 0;
        let bs = df.collect().await.unwrap();
        for batch in bs {
            row_cnt += batch.num_rows();
        }
        assert_eq!(row_cnt, 55)
    }
}
