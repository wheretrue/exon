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

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::{
    datasource::{
        file_format::{file_type::FileCompressionType, FileFormat},
        physical_plan::FileScanConfig,
    },
    execution::context::SessionState,
    physical_plan::{ExecutionPlan, PhysicalExpr, Statistics},
};
use object_store::{ObjectMeta, ObjectStore};

use super::scanner::GFFScan;

#[derive(Debug)]
/// Implements a datafusion `FileFormat` for GFF files.
pub struct GFFFormat {
    /// The compression type of the file.
    file_compression_type: FileCompressionType,
}

impl Default for GFFFormat {
    fn default() -> Self {
        Self {
            file_compression_type: FileCompressionType::UNCOMPRESSED,
        }
    }
}

pub fn schema() -> SchemaRef {
    let attribute_key_field = Field::new("keys", DataType::Utf8, false);

    // attribute_value_field is a list of strings
    let value_field = Field::new("item", DataType::Utf8, true);
    let attribute_value_field = Field::new("values", DataType::List(Arc::new(value_field)), true);

    let inner = Schema::new(vec![
        Field::new("seqname", DataType::Utf8, false),
        Field::new("source", DataType::Utf8, true),
        Field::new("type", DataType::Utf8, false),
        Field::new("start", DataType::Int64, false),
        Field::new("end", DataType::Int64, false),
        Field::new("score", DataType::Float32, true),
        Field::new("strand", DataType::Utf8, false),
        Field::new("phase", DataType::Utf8, true),
        Field::new_map(
            "attributes",
            "entries",
            attribute_key_field,
            attribute_value_field,
            false,
            true,
        ),
    ]);

    inner.into()
}

/// The `FileFormat` for GFF files.
impl GFFFormat {
    /// Create a new GFFFormat.
    pub fn new(file_compression_type: FileCompressionType) -> Self {
        Self {
            file_compression_type,
        }
    }

    /// Return the schema for GFF files.
    pub fn schema(&self) -> datafusion::error::Result<SchemaRef> {
        Ok(schema())
    }
}

#[async_trait]
impl FileFormat for GFFFormat {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn infer_schema(
        &self,
        _state: &SessionState,
        _store: &Arc<dyn ObjectStore>,
        _objects: &[ObjectMeta],
    ) -> datafusion::error::Result<SchemaRef> {
        self.schema()
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
        let scan = GFFScan::new(conf.clone(), self.file_compression_type.clone());
        Ok(Arc::new(scan))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::GFFFormat;
    use datafusion::{
        datasource::listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
        prelude::SessionContext,
    };

    #[tokio::test]
    async fn test_listing() {
        let ctx = SessionContext::new();
        let session_state = ctx.state();

        let table_path = ListingTableUrl::parse("test-data/datasources/gff").unwrap();

        let gff_format = Arc::new(GFFFormat::default());
        let lo = ListingOptions::new(gff_format.clone()).with_file_extension("gff");

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
        assert_eq!(row_cnt, 5000)
    }
}
