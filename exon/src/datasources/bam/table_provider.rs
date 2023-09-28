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

use std::{any::Any, str::FromStr, sync::Arc};

use arrow::datatypes::{Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::{
    datasource::{
        listing::{ListingTableConfig, ListingTableUrl},
        physical_plan::FileScanConfig,
        TableProvider,
    },
    error::{DataFusionError, Result},
    execution::context::SessionState,
    logical_expr::{Operator, TableProviderFilterPushDown, TableType},
    physical_plan::{empty::EmptyExec, ExecutionPlan},
    prelude::Expr,
};
use noodles::core::{region::Interval, Region};

/// A builder for a Region.
pub struct RegionBuilder {
    reference: Option<String>,
    start: Option<i32>,
    end: Option<i32>,
}

impl RegionBuilder {
    /// Create a new RegionBuilder
    pub fn new() -> Self {
        RegionBuilder {
            reference: None,
            start: None,
            end: None,
        }
    }

    /// Add the expression to the builder
    pub fn add_from_expr(mut self, expr: &Expr) -> Self {
        match expr {
            Expr::BinaryExpr(be) => {
                let left = be.left.as_ref();
                let right = be.right.as_ref();
                let op = be.op;

                match (left, right, op) {
                    (Expr::Column(c), Expr::Literal(l), Operator::Eq)
                        if c.name.as_str() == "reference" =>
                    {
                        self.reference = Some(l.to_string());
                    }
                    (Expr::Column(c), Expr::Literal(l), Operator::Gt)
                        if c.name.as_str() == "end" =>
                    {
                        self.end = l.to_string().parse::<i32>().ok();
                    }
                    (Expr::Column(c), Expr::Literal(l), Operator::Lt)
                        if c.name.as_str() == "start" =>
                    {
                        self.start = l.to_string().parse::<i32>().ok();
                    }
                    _ => {}
                }
            }
            _ => {}
        }

        self
    }

    /// Add a slice of expressions to the builder
    pub fn add_from_exprs(mut self, exprs: &[Expr]) -> Self {
        for expr in exprs {
            self = self.add_from_expr(expr);
        }

        self
    }

    /// Build the region
    pub fn build(self) -> Option<Region> {
        match (self.reference, self.start, self.end) {
            (Some(name), Some(start), Some(end)) => {
                let interval = Interval::from_str(&format!("{}..{}", end, start)).ok()?;
                Some(Region::new(name, interval))
            }
            (Some(name), None, None) => Region::from_str(name.as_str()).ok(),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
/// Configuration for a VCF listing table
pub struct ListingBAMTableConfig {
    inner: ListingTableConfig,

    options: Option<ListingBAMTableOptions>,
}

impl ListingBAMTableConfig {
    /// Create a new VCF listing table configuration
    pub fn new(table_path: ListingTableUrl) -> Self {
        Self {
            inner: ListingTableConfig::new(table_path),
            options: None,
        }
    }

    /// Set the options for the VCF listing table
    pub fn with_options(self, options: ListingBAMTableOptions) -> Self {
        Self {
            options: Some(options),
            ..self
        }
    }
}

use crate::{
    datasources::{indexed_file_utils::IndexedFile, sam::schema},
    physical_plan::file_scan_config_builder::FileScanConfigBuilder,
};

use super::{indexed_scanner::IndexedBAMScan, BAMScan};

#[derive(Debug, Clone)]
/// Listing options for a BAM table
pub struct ListingBAMTableOptions {
    file_extension: String,

    indexed: bool,
}

impl Default for ListingBAMTableOptions {
    fn default() -> Self {
        Self {
            file_extension: String::from("bam"),
            indexed: false,
        }
    }
}

impl ListingBAMTableOptions {
    /// Infer the schema for the table
    pub async fn infer_schema(&self) -> datafusion::error::Result<SchemaRef> {
        let schema = schema();

        Ok(Arc::new(schema))
    }

    /// Update the indexed flag
    pub fn with_indexed(mut self, indexed: bool) -> Self {
        self.indexed = indexed;
        self
    }

    async fn create_physical_plan_with_region(
        &self,
        conf: FileScanConfig,
        region: Arc<Region>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let scan = IndexedBAMScan::new(conf, region);
        Ok(Arc::new(scan))
    }

    async fn create_physical_plan(
        &self,
        conf: FileScanConfig,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let scan = BAMScan::new(conf);
        Ok(Arc::new(scan))
    }
}

#[derive(Debug, Clone)]
/// A BAM listing table
pub struct ListingBAMTable {
    table_paths: Vec<ListingTableUrl>,

    table_schema: SchemaRef,

    options: ListingBAMTableOptions,
}

impl ListingBAMTable {
    /// Create a new BAM listing table
    pub fn try_new(config: ListingBAMTableConfig, table_schema: Arc<Schema>) -> Result<Self> {
        Ok(Self {
            table_paths: config.inner.table_paths,
            table_schema,
            options: config
                .options
                .ok_or_else(|| DataFusionError::Internal(String::from("Options must be set")))?,
        })
    }
}

#[async_trait]
impl TableProvider for ListingBAMTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.table_schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|f| {
                // if f is a binary expression it might be able to be pushed down.

                let be = match f {
                    Expr::BinaryExpr(be) => be,
                    _ => return TableProviderFilterPushDown::Unsupported,
                };

                // Get the left, right and operator
                let left = be.left.as_ref();
                let right = be.right.as_ref();
                let op = be.op;

                match (left, right, op) {
                    // If the left is a column and the right is a literal we can push down
                    // the filter
                    (Expr::Column(c), Expr::Literal(_), Operator::Eq)
                        if c.name.as_str() == "reference" =>
                    {
                        TableProviderFilterPushDown::Inexact
                    }
                    (Expr::Column(c), Expr::Literal(_), Operator::Gt)
                        if c.name.as_str() == "end" =>
                    {
                        TableProviderFilterPushDown::Inexact
                    }
                    (Expr::Column(c), Expr::Literal(_), Operator::Lt)
                        if c.name.as_str() == "start" =>
                    {
                        TableProviderFilterPushDown::Inexact
                    }
                    // If the left is a column and the right is a literal we can push down
                    _ => TableProviderFilterPushDown::Unsupported,
                }
            })
            .collect())
    }

    async fn scan(
        &self,
        state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let object_store_url = if let Some(url) = self.table_paths.get(0) {
            url.object_store()
        } else {
            return Ok(Arc::new(EmptyExec::new(false, Arc::new(Schema::empty()))));
        };

        let object_store = state.runtime_env().object_store(object_store_url.clone())?;

        let region = RegionBuilder::new().add_from_exprs(filters).build();
        if region.is_none() && self.options.indexed {
            return Err(DataFusionError::Execution(
                "Indexed BAM requires a region filter".to_string(),
            ));
        }

        match region {
            None => {
                let partitioned_file_lists = vec![
                    crate::physical_plan::object_store::list_files_for_scan(
                        object_store,
                        self.table_paths.clone(),
                        &self.options.file_extension,
                    )
                    .await?,
                ];

                let file_scan_config = FileScanConfigBuilder::new(
                    object_store_url.clone(),
                    Arc::clone(&self.table_schema),
                    partitioned_file_lists,
                )
                .projection_option(projection.cloned())
                .limit_option(limit)
                .build();

                let plan = self.options.create_physical_plan(file_scan_config).await?;

                Ok(plan)
            }
            Some(region) => {
                let regions = vec![region.clone()];
                let partitioned_file_lists = IndexedFile::Bam
                    .list_files_for_scan(self.table_paths.clone(), object_store, &regions)
                    .await?;

                if partitioned_file_lists.is_empty() || partitioned_file_lists[0].is_empty() {
                    return Ok(Arc::new(EmptyExec::new(
                        false,
                        Arc::clone(&self.table_schema),
                    )));
                }

                let file_scan_config = FileScanConfigBuilder::new(
                    object_store_url.clone(),
                    Arc::clone(&self.table_schema),
                    partitioned_file_lists,
                )
                .projection_option(projection.cloned())
                .limit_option(limit)
                .build();

                let plan = self
                    .options
                    .create_physical_plan_with_region(file_scan_config, Arc::new(region))
                    .await?;

                Ok(plan)
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use datafusion::{common::FileCompressionType, prelude::SessionContext};

    use crate::{
        datasources::{ExonFileType, ExonListingTableFactory},
        tests::test_listing_table_url,
        ExonSessionExt,
    };

    #[tokio::test]
    async fn test_read_bam() -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let session_state = ctx.state();

        let table_path = test_listing_table_url("bam");

        let exon_listing_table_factory = ExonListingTableFactory::new();

        let table = exon_listing_table_factory
            .create_from_file_type(
                &session_state,
                ExonFileType::BAM,
                FileCompressionType::UNCOMPRESSED,
                table_path.to_string(),
            )
            .await?;

        let df = ctx.read_table(table).unwrap();

        let mut row_cnt = 0;
        let bs = df.collect().await.unwrap();
        for batch in bs {
            row_cnt += batch.num_rows();
        }

        assert_eq!(row_cnt, 61);

        Ok(())
    }

    #[tokio::test]
    async fn test_read_with_index() -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new_exon();

        let table_path = test_listing_table_url("bam");

        let create_external_table_sql = format!(
            "CREATE EXTERNAL TABLE bam_file STORED AS INDEXED_BAM LOCATION '{}';",
            table_path
        );
        ctx.sql(&create_external_table_sql).await?;

        let select_sql = "SELECT * FROM bam_file WHERE reference = 'chr1';";
        let df = ctx.sql(&select_sql).await?;
        let cnt = df.count().await?;

        assert_eq!(cnt, 61);

        Ok(())
    }
}
