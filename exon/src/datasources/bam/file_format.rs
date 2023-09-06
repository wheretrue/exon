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
        file_format::FileFormat, listing::PartitionedFile, physical_plan::FileScanConfig,
    },
    execution::context::SessionState,
    physical_plan::{expressions::BinaryExpr, ExecutionPlan, PhysicalExpr, Statistics},
};
use noodles::core::Region;
use object_store::{ObjectMeta, ObjectStore};

use crate::{
    datasources::vcf::add_csi_ranges_to_file_groups,
    physical_plan::reference_physical_expr::ReferencePhysicalExpr,
};

use super::{array_builder::schema, scanner::BAMScan};

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
                    let scan = BAMScan::try_new(conf)?;
                    return Ok(Arc::new(scan));
                }
            },
            _ => {
                let scan = BAMScan::try_new(conf)?;
                return Ok(Arc::new(scan));
            }
        };

        if let Ok(reference_expr) = ReferencePhysicalExpr::try_from(new_filters.clone()) {
            let region = reference_expr.region();

            let mut new_conf = conf.clone();

            let object_store = state.runtime_env().object_store(&conf.object_store_url)?;

            if let Ok(new_groups) =
                add_csi_ranges_to_file_groups(object_store, &conf.file_groups, &region, ".bai")
                    .await
            {
                new_conf.file_groups = new_groups;
            }

            let mut scan = BAMScan::try_new(new_conf)?;
            scan = scan.with_region_filter(region);

            return Ok(Arc::new(scan));
        }

        let mut scan = BAMScan::try_new(conf)?;

        if let Some(region_filter) = &self.region_filter {
            scan = scan.with_region_filter(region_filter.clone());
        }

        Ok(Arc::new(scan))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{datasources::bam::BAMScan, tests::test_path, ExonSessionExt};

    use super::BAMFormat;
    use datafusion::{
        datasource::listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
        physical_plan::filter::FilterExec,
        prelude::SessionContext,
    };
    use noodles::core::Region;

    #[tokio::test]
    async fn test_region_pushdown() {
        let ctx = SessionContext::new_exon();

        // let table_path = test_path("bam", "test.bam").to_str().unwrap()
        let table_path = "/Users/thauck/wheretrue/github.com/wheretrue/exon/exon/test-data/datasources/bam/test.bam";
        eprintln!("table_path: {:?}", table_path);

        let sql = format!(
            "CREATE EXTERNAL TABLE bam_file STORED AS BAM LOCATION '{}';",
            table_path,
        );
        ctx.sql(&sql).await.unwrap();

        let sql_statements = vec!["SELECT * FROM bam_file WHERE reference = 'chr1'"];

        for sql_statement in sql_statements {
            let df = ctx.sql(sql_statement).await.unwrap();

            let physical_plan = ctx
                .state()
                .create_physical_plan(df.logical_plan())
                .await
                .unwrap();

            if let Some(scan) = physical_plan.as_any().downcast_ref::<FilterExec>() {
                scan.input().as_any().downcast_ref::<BAMScan>().unwrap();
            } else {
                panic!(
                    "expected FilterExec for {} in {:#?}",
                    sql_statement, physical_plan
                );
            }
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
