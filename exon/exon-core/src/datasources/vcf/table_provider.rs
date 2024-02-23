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

use arrow::datatypes::{Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::{
    datasource::{
        file_format::file_compression_type::FileCompressionType,
        listing::{ListingTableConfig, ListingTableUrl},
        physical_plan::FileScanConfig,
        TableProvider,
    },
    error::{DataFusionError, Result},
    execution::context::SessionState,
    logical_expr::{expr::ScalarFunction, TableProviderFilterPushDown, TableType},
    physical_plan::{empty::EmptyExec, ExecutionPlan},
    prelude::Expr,
};
use exon_common::TableSchema;
use futures::{StreamExt, TryStreamExt};
use noodles::{bgzf, core::Region, vcf};
use object_store::{ObjectMeta, ObjectStore};
use tokio_util::io::StreamReader;

use crate::{
    config::ExonConfigExtension,
    datasources::{
        hive_partition::filter_matches_partition_cols,
        indexed_file_utils::{augment_partitioned_file_with_byte_range, IndexedFile},
        ExonFileType,
    },
    physical_plan::{
        file_scan_config_builder::FileScanConfigBuilder, infer_region,
        object_store::pruned_partition_list,
    },
};

use super::{indexed_scanner::IndexedVCFScanner, VCFScan, VCFSchemaBuilder};

#[derive(Debug, Clone)]
/// Configuration for a VCF listing table
pub struct ListingVCFTableConfig {
    inner: ListingTableConfig,

    options: Option<ListingVCFTableOptions>,
}

impl ListingVCFTableConfig {
    /// Create a new VCF listing table configuration
    pub fn new(table_path: ListingTableUrl) -> Self {
        Self {
            inner: ListingTableConfig::new(table_path),
            options: None,
        }
    }

    /// Set the options for the VCF listing table
    pub fn with_options(self, options: ListingVCFTableOptions) -> Self {
        Self {
            options: Some(options),
            ..self
        }
    }
}

#[derive(Debug, Clone)]
/// Options specific to the VCF file format
pub struct ListingVCFTableOptions {
    /// The extension of the files to read
    file_extension: String,

    /// True if the file must be indexed
    indexed: bool,

    /// A region to filter the records
    region: Option<Region>,

    /// The file compression type
    file_compression_type: FileCompressionType,

    /// A list of table partition columns
    table_partition_cols: Vec<Field>,
}

impl ListingVCFTableOptions {
    /// Create a new set of options
    pub fn new(file_compression_type: FileCompressionType, indexed: bool) -> Self {
        let file_extension = ExonFileType::VCF.get_file_extension(file_compression_type);

        Self {
            file_extension,
            file_compression_type,
            indexed,
            table_partition_cols: Vec::new(),
            region: None,
        }
    }

    /// Set the region
    pub fn with_region(self, region: Option<Region>) -> Self {
        Self {
            region,
            indexed: true,
            ..self
        }
    }

    /// Set the table partition columns
    pub fn with_table_partition_cols(self, table_partition_cols: Vec<Field>) -> Self {
        Self {
            table_partition_cols,
            ..self
        }
    }

    async fn infer_schema_from_object_meta(
        &self,
        state: &SessionState,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> datafusion::error::Result<TableSchema> {
        if objects.is_empty() {
            return Err(DataFusionError::Execution(
                "No objects found in the table path".to_string(),
            ));
        }

        let get_result = store.get(&objects[0].location).await?;

        let stream_reader = Box::pin(get_result.into_stream().map_err(DataFusionError::from));
        let stream_reader = StreamReader::new(stream_reader);

        let exon_settings = state
            .config()
            .options()
            .extensions
            .get::<ExonConfigExtension>()
            .ok_or(DataFusionError::Execution(
                "Exon settings must be configured.".to_string(),
            ))?;

        let mut builder = VCFSchemaBuilder::default()
            .with_parse_info(exon_settings.vcf_parse_info)
            .with_parse_formats(exon_settings.vcf_parse_formats)
            .with_partition_fields(self.table_partition_cols.clone());

        let header = match self.file_compression_type {
            FileCompressionType::GZIP => {
                let bgzf_reader = bgzf::AsyncReader::new(stream_reader);
                let mut vcf_reader = vcf::AsyncReader::new(bgzf_reader);

                vcf_reader.read_header().await?
            }
            FileCompressionType::UNCOMPRESSED => {
                let mut vcf_reader = vcf::AsyncReader::new(stream_reader);
                vcf_reader.read_header().await?
            }
            _ => {
                return Err(DataFusionError::Execution(
                    "Unsupported file compression type".to_string(),
                ))
            }
        };

        builder = builder.with_header(header);

        let table_schema = builder.build()?;

        Ok(table_schema)
    }

    /// Infer the schema of the files in the table
    pub async fn infer_schema<'a>(
        &'a self,
        state: &SessionState,
        table_path: &'a ListingTableUrl,
    ) -> Result<TableSchema> {
        let store = state.runtime_env().object_store(table_path)?;

        let files = exon_common::object_store_files_from_table_path(
            &store,
            table_path.as_ref(),
            table_path.prefix(),
            self.file_extension.as_str(),
            None,
        )
        .await;

        // collect the files as a slice
        let files = files
            .try_collect::<Vec<_>>()
            .await
            .map_err(|e| DataFusionError::Execution(format!("Unable to get path info: {}", e)))?;

        self.infer_schema_from_object_meta(state, &store, &files)
            .await
    }

    async fn create_physical_plan_with_region(
        &self,
        conf: FileScanConfig,
        region: Arc<Region>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let scan = IndexedVCFScanner::new(conf, region)?;

        Ok(Arc::new(scan))
    }

    async fn create_physical_plan(
        &self,
        conf: FileScanConfig,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let scan = VCFScan::new(conf, self.file_compression_type)?;

        Ok(Arc::new(scan))
    }
}

#[derive(Debug, Clone)]
/// A VCF listing table
pub struct ListingVCFTable {
    table_paths: Vec<ListingTableUrl>,

    table_schema: TableSchema,

    options: ListingVCFTableOptions,
}

impl ListingVCFTable {
    /// Create a new VCF listing table
    pub fn try_new(config: ListingVCFTableConfig, table_schema: TableSchema) -> Result<Self> {
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
impl TableProvider for ListingVCFTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.table_schema.table_schema())
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        tracing::info!(
            "vcf table provider supports_filters_pushdown: {:?}",
            filters
        );

        Ok(filters
            .iter()
            .map(|f| {
                if let Expr::ScalarFunction(s) = f {
                    if s.name() == "vcf_region_filter" && (s.args.len() == 2 || s.args.len() == 3) {
                        return TableProviderFilterPushDown::Exact;
                    }
                }

                filter_matches_partition_cols(f, &self.options.table_partition_cols)
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
        let object_store_url = if let Some(url) = self.table_paths.first() {
            url.object_store()
        } else {
            return Ok(Arc::new(EmptyExec::new(Arc::new(Schema::empty()))));
        };

        let object_store = state.runtime_env().object_store(object_store_url.clone())?;

        let regions = filters
            .iter()
            .filter_map(|f| {
                if let Expr::ScalarFunction(s) = f {
                    infer_region::infer_region_from_udf(s, "vcf_region_filter")
                } else {
                    None
                }
            })
            .collect::<Vec<Region>>();

        let regions = if self.options.indexed {
            if !regions.is_empty() {
                regions
            } else {
                match self.options.region.clone() {
                    Some(region) => vec![region],
                    None => regions,
                }
            }
        } else {
            regions
        };

        if regions.len() > 1 {
            return Err(DataFusionError::NotImplemented(
                "Multiple regions are not supported yet".to_string(),
            ));
        }

        if regions.is_empty() && self.options.indexed {
            return Err(DataFusionError::Plan(
                "INDEXED_VCF table requires a region filter. See the UDF 'vcf_region_filter'."
                    .to_string(),
            ));
        }

        if regions.is_empty() {
            let file_list = pruned_partition_list(
                state,
                &object_store,
                &self.table_paths[0],
                filters,
                self.options.file_extension.as_str(),
                &self.options.table_partition_cols,
            )
            .await?
            .try_collect::<Vec<_>>()
            .await?;

            let file_schema = self.table_schema.file_schema()?;
            let file_scan_config =
                FileScanConfigBuilder::new(object_store_url.clone(), file_schema, vec![file_list])
                    .projection_option(projection.cloned())
                    .limit_option(limit)
                    .table_partition_cols(self.options.table_partition_cols.clone())
                    .build();

            let table = self.options.create_physical_plan(file_scan_config).await?;

            return Ok(table);
        }

        let mut file_list = pruned_partition_list(
            state,
            &object_store,
            &self.table_paths[0],
            filters,
            self.options.file_extension.as_str(),
            &self.options.table_partition_cols,
        )
        .await?;

        let mut file_partitions = Vec::new();

        while let Some(f) = file_list.next().await {
            let f = f?;

            for region in &regions {
                let file_byte_range = augment_partitioned_file_with_byte_range(
                    object_store.clone(),
                    &f,
                    region,
                    &IndexedFile::Vcf,
                )
                .await?;

                file_partitions.extend(file_byte_range);
            }
        }

        let file_schema = self.table_schema.file_schema()?;
        let file_scan_config = FileScanConfigBuilder::new(
            object_store_url.clone(),
            file_schema,
            vec![file_partitions],
        )
        .projection_option(projection.cloned())
        .limit_option(limit)
        .table_partition_cols(self.options.table_partition_cols.clone())
        .build();

        let table = self
            .options
            .create_physical_plan_with_region(file_scan_config, Arc::new(regions[0].clone()))
            .await?;

        return Ok(table);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{
        datasources::{vcf::IndexedVCFScanner, ExonListingTableFactory},
        tests::setup_tracing,
        ExonSessionExt,
    };

    use arrow::datatypes::{DataType, Field, Fields};
    use datafusion::{
        datasource::file_format::file_compression_type::FileCompressionType,
        physical_plan::{coalesce_partitions::CoalescePartitionsExec, filter::FilterExec},
        prelude::SessionContext,
    };
    use exon_test::test_path;

    #[cfg(feature = "fixtures")]
    #[tokio::test]
    async fn test_chr17_queries() -> Result<(), Box<dyn std::error::Error>> {
        use crate::tests::{setup_tracing, test_fixture_table_url};

        setup_tracing();

        let path = test_fixture_table_url("chr17/")?;

        let ctx = SessionContext::new_exon();
        ctx.sql(
            format!(
                "CREATE EXTERNAL TABLE vcf_file STORED AS VCF LOCATION '{}';",
                path.to_string().as_str()
            )
            .as_str(),
        )
        .await?;

        let sql = "SELECT chrom, pos FROM vcf_file LIMIT 5;";
        let df = ctx.sql(sql).await?;

        // Get the first batch
        let mut batches = df.collect().await?;
        let batch = batches.remove(0);

        assert_eq!(batch.num_rows(), 5);
        assert_eq!(batch.num_columns(), 2);

        Ok(())
    }

    #[tokio::test]
    #[cfg(feature = "fixtures")]
    async fn test_chr17_positions() -> Result<(), Box<dyn std::error::Error>> {
        use crate::tests::test_fixture_table_url;

        let path = test_fixture_table_url(
            "chr17/ALL.chr17.integrated_phase1_v3.20101123.snps_indels_svs.genotypes.vcf.gz",
        )?;

        let ctx = SessionContext::new_exon();

        ctx.sql(
            format!(
                "CREATE EXTERNAL TABLE vcf_file STORED AS INDEXED_VCF COMPRESSION TYPE GZIP LOCATION '{}';",
                path.to_string().as_str()
            )
            .as_str(),
        )
        .await?;

        let sql_commands = vec![
            "SELECT chrom, pos FROM vcf_file WHERE vcf_region_filter('17:1-1000', chrom, pos);",
            "SELECT chrom, pos FROM vcf_file WHERE vcf_region_filter('17:1000-1000000', chrom, pos);",
            "SELECT chrom, pos FROM vcf_file WHERE vcf_region_filter('17:1234-1424000', chrom, pos);",
            "SELECT chrom, pos FROM vcf_file WHERE vcf_region_filter('17:1000000-1424000', chrom, pos);",
        ];

        for sql in sql_commands {
            let df = ctx.sql(sql).await?;

            // Get the first batch
            let mut batches = df.collect().await?;
            let batch = batches.remove(0);

            assert!(batch.num_rows() > 0);
            assert_eq!(batch.num_columns(), 2);
        }

        Ok(())
    }

    #[cfg(feature = "fixtures")]
    #[tokio::test]
    async fn test_region_query_with_additional_predicates() -> Result<(), Box<dyn std::error::Error>>
    {
        setup_tracing();
        let path = crate::tests::test_fixture_table_url(
            "chr17/ALL.chr17.integrated_phase1_v3.20101123.snps_indels_svs.genotypes.vcf.gz",
        )?;

        let ctx = SessionContext::new_exon();
        ctx.sql(
            format!(
                "CREATE EXTERNAL TABLE vcf_file STORED AS INDEXED_VCF COMPRESSION TYPE GZIP LOCATION '{}';",
                path.to_string().as_str()
            )
            .as_str(),
        )
        .await?;

        let sql =
            "SELECT chrom FROM vcf_file WHERE vcf_region_filter('17:1000-1000000', chrom, pos) AND qual != 100;";
        let df = ctx.sql(sql).await?;

        let cnt_where_qual_neq_100 = df.count().await?;
        assert!(cnt_where_qual_neq_100 > 0);

        let cnt_total = ctx
            .sql(
                "SELECT chrom FROM vcf_file WHERE vcf_region_filter('17:1000-1000000', chrom, pos)",
            )
            .await?
            .count()
            .await?;

        assert!(cnt_where_qual_neq_100 < cnt_total);

        Ok(())
    }

    #[tokio::test]
    async fn test_region_pushdown() -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new_exon();
        let table_path = test_path("vcf", "index.vcf.gz");
        let table_path = table_path.to_str().ok_or("Invalid path")?;

        let sql = format!(
            "CREATE EXTERNAL TABLE vcf_file STORED AS VCF COMPRESSION TYPE GZIP LOCATION '{}';",
            table_path
        );
        ctx.sql(&sql).await?;

        let sql_statements = vec![
            "SELECT * FROM vcf_file WHERE vcf_region_filter('1:9999921', chrom, pos);",
            "SELECT * FROM vcf_file WHERE vcf_region_filter('1:9999921-9999922', chrom, pos);",
            "SELECT * FROM vcf_file WHERE vcf_region_filter('1', chrom);",
        ];

        for sql_statement in sql_statements {
            let df = ctx.sql(sql_statement).await?;

            let physical_plan = ctx.state().create_physical_plan(df.logical_plan()).await?;

            if let Some(scan) = physical_plan.as_any().downcast_ref::<FilterExec>() {
                let scan = scan
                    .input()
                    .as_any()
                    .downcast_ref::<CoalescePartitionsExec>()
                    .ok_or("Invalid partition")?;

                let scan = scan.input().as_any().downcast_ref::<IndexedVCFScanner>();
                assert!(scan.is_some());
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_vcf_parsing_string() -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new_exon();

        let table_path = test_path("vcf", "index.vcf");

        let sql = "SET exon.vcf_parse_info = true;";
        ctx.sql(sql).await?;

        let sql = "SET exon.vcf_parse_formats = true;";
        ctx.sql(sql).await?;

        let sql = format!(
            "CREATE EXTERNAL TABLE vcf_file STORED AS VCF LOCATION '{}';",
            table_path.to_str().ok_or("Invalid path")?
        );
        ctx.sql(&sql).await?;

        let sql = "SELECT * FROM vcf_file WHERE chrom = '1' AND pos = 100000;";
        let df = ctx.sql(sql).await?;

        // Check that the last two columns are strings.
        let schema = df.schema();

        let infos_fields = Fields::from(vec![
            Field::new("INDEL", DataType::Boolean, true),
            Field::new("IDV", DataType::Int32, true),
            Field::new("IMF", DataType::Float32, true),
            Field::new("DP", DataType::Int32, true),
            Field::new("VDB", DataType::Float32, true),
            Field::new("RPB", DataType::Float32, true),
            Field::new("MQB", DataType::Float32, true),
            Field::new("BQB", DataType::Float32, true),
            Field::new("MQSB", DataType::Float32, true),
            Field::new("SGB", DataType::Float32, true),
            Field::new("MQ0F", DataType::Float32, true),
            Field::new(
                "I16",
                DataType::List(Arc::new(Field::new("item", DataType::Float32, true))),
                true,
            ),
            Field::new(
                "QS",
                DataType::List(Arc::new(Field::new("item", DataType::Float32, true))),
                true,
            ),
        ]);
        assert_eq!(schema.field(7).data_type(), &DataType::Struct(infos_fields));

        let inner_item_fields = vec![Field::new(
            "PL",
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            true,
        )];

        let inner_struct = DataType::Struct(Fields::from(inner_item_fields));
        let inner_list = DataType::List(Arc::new(Field::new("item", inner_struct, true)));
        assert_eq!(schema.field(8).data_type(), &inner_list);

        Ok(())
    }

    #[tokio::test]
    async fn test_uncompressed_read() -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();

        let ctx = SessionContext::new_exon();
        let table_path = test_path("vcf", "index.vcf");
        let table_path = table_path.to_str().ok_or("Invalid path")?;

        let table = ExonListingTableFactory::new()
            .create_from_file_type(
                &ctx.state(),
                crate::datasources::ExonFileType::VCF,
                FileCompressionType::UNCOMPRESSED,
                table_path.to_string(),
                Vec::new(),
            )
            .await?;

        ctx.register_table("vcf_file", table)?;

        let df = ctx.sql("SELECT chrom, pos FROM vcf_file").await?;

        let mut row_cnt = 0;
        let bs = df.collect().await?;
        for batch in bs {
            row_cnt += batch.num_rows();
        }
        assert_eq!(row_cnt, 621);

        Ok(())
    }

    #[tokio::test]
    async fn test_with_biobear_file() -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new_exon();

        let table_path = test_path("biobear-vcf", "vcf_file.vcf.gz");
        let table_path = table_path.to_str().ok_or("Invalid path")?;

        ctx.register_vcf_file("vcf_file", table_path).await?;

        let sql = "SELECT * FROM vcf_file";
        let df = ctx.sql(sql).await?;

        let batches = df.collect().await?;
        let mut cnt = 0;
        for batch in batches {
            assert!(batch.num_rows() > 0);

            // Check the schema is of the correct size.
            assert_eq!(batch.schema().fields().len(), 9);

            cnt += batch.num_rows();
        }

        assert_eq!(cnt, 15);

        Ok(())
    }

    #[tokio::test]
    async fn test_with_biobear_empty_query() -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new_exon();

        let table_path = test_path("biobear-vcf", "vcf_file.vcf.gz");
        let table_path = table_path.to_str().ok_or("Invalid path")?;

        ctx.register_vcf_file("vcf_file", table_path).await?;

        let sql = "SELECT * FROM vcf_file WHERE chrom = '1000'";
        let df = ctx.sql(sql).await?;

        let cnt = df.count().await?;

        assert_eq!(cnt, 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_with_biobear_chrom_1() -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new_exon();

        let table_path = test_path("biobear-vcf", "vcf_file.vcf.gz");
        let table_path = table_path.to_str().ok_or("Invalid path")?;

        ctx.register_vcf_file("vcf_file", table_path).await?;

        let sql = "SELECT * FROM vcf_file WHERE chrom = '1'";
        let df = ctx.sql(sql).await?;

        let batches = df.collect().await?;
        let mut cnt = 0;
        for batch in batches {
            assert!(batch.num_rows() > 0);

            // Check the schema is of the correct size.
            assert_eq!(batch.schema().fields().len(), 9);

            cnt += batch.num_rows();
        }

        assert_eq!(cnt, 11);

        Ok(())
    }

    #[tokio::test]
    async fn test_compressed_read_with_region() -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new_exon();
        let table_path = test_path("bigger-index", "test.vcf.gz");
        let table_path = table_path.to_str().ok_or("Invalid path")?;

        ctx.register_vcf_file("vcf_file", table_path).await?;

        let df = ctx
            .sql("SELECT chrom, pos FROM vcf_file WHERE chrom = 'chr1' AND pos BETWEEN 3388920 AND 3388930")
            .await?;

        let mut row_cnt = 0;
        let bs = df.collect().await?;
        for batch in bs {
            row_cnt += batch.num_rows();

            assert_eq!(batch.schema().field(0).name(), "chrom");
            assert_eq!(batch.schema().field(1).name(), "pos");

            assert_eq!(batch.schema().fields().len(), 2);
        }

        assert_eq!(row_cnt, 1);

        Ok(())
    }
}
