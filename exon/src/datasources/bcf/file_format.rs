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
    datasource::file_format::FileFormat,
    error::DataFusionError,
    execution::context::SessionState,
    physical_plan::{file_format::FileScanConfig, ExecutionPlan, PhysicalExpr, Statistics},
};
use futures::TryStreamExt;
use noodles::bcf;
use object_store::{ObjectMeta, ObjectStore};
use tokio_util::io::StreamReader;

use crate::datasources::vcf::VCFSchemaBuilder;

use super::scanner::BCFScan;

// use super::{config::schema, scanner::BCFScan};

#[derive(Debug, Default)]
/// Implements a datafusion `FileFormat` for BCF files.
pub struct BCFFormat {}

#[async_trait]
impl FileFormat for BCFFormat {
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

        let mut bcf_reader = bcf::AsyncReader::new(stream_reader);
        bcf_reader.read_file_format().await?;

        let header_str = bcf_reader.read_header().await?;
        let header = header_str
            .parse::<noodles::vcf::Header>()
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;

        let schema_builder = VCFSchemaBuilder::from(header.clone());

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
        _state: &SessionState,
        conf: FileScanConfig,
        _filters: Option<&Arc<dyn PhysicalExpr>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let scan = BCFScan::new(conf);
        Ok(Arc::new(scan))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::tests::test_listing_table_url;

    use super::BCFFormat;
    use datafusion::{
        datasource::listing::{ListingOptions, ListingTable, ListingTableConfig},
        prelude::SessionContext,
    };

    #[tokio::test]
    async fn test_bcf_read() {
        let ctx = SessionContext::new();
        let session_state = ctx.state();

        let table_path = test_listing_table_url("bcf");

        let bcf_format = Arc::new(BCFFormat::default());
        let lo = ListingOptions::new(bcf_format.clone());

        let resolved_schema = lo.infer_schema(&session_state, &table_path).await.unwrap();

        assert_eq!(resolved_schema.fields().len(), 9);
        assert_eq!(resolved_schema.field(0).name(), "chrom");

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
        assert_eq!(row_cnt, 621)
    }
}
