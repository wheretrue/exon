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

use arrow::datatypes::{Field, SchemaRef};
use async_trait::async_trait;
use datafusion::{
    datasource::{
        file_format::file_compression_type::FileCompressionType, listing::ListingTableUrl,
        physical_plan::FileScanConfig, TableProvider,
    },
    error::{DataFusionError, Result},
    execution::context::SessionState,
    logical_expr::{TableProviderFilterPushDown, TableType},
    physical_plan::ExecutionPlan,
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
        exon_listing_table_options::{
            ExonIndexedListingOptions, ExonListingConfig, ExonListingOptions,
        },
        hive_partition::filter_matches_partition_cols,
        indexed_file::indexed_bgzf_file::{
            augment_partitioned_file_with_byte_range, IndexedBGZFFile,
        },
        ExonFileType,
    },
    error::Result as ExonResult,
    physical_plan::{
        file_scan_config_builder::FileScanConfigBuilder, infer_region,
        object_store::pruned_partition_list,
    },
};

use super::{indexed_scanner::IndexedVCFScanner, VCFScan, VCFSchemaBuilder};

#[derive(Debug, Clone)]
/// Options specific to the VCF file format
pub struct ListingVCFTableOptions {
    /// The extension of the files to read
    file_extension: String,

    /// True if the file must be indexed
    indexed: bool,

    /// A region to filter the records
    regions: Vec<Region>,

    /// The file compression type
    file_compression_type: FileCompressionType,

    /// A list of table partition columns
    table_partition_cols: Vec<Field>,
}

impl Default for ListingVCFTableOptions {
    fn default() -> Self {
        Self {
            file_extension: ExonFileType::VCF.get_file_extension(FileCompressionType::UNCOMPRESSED),
            indexed: false,
            regions: Vec::new(),
            file_compression_type: FileCompressionType::UNCOMPRESSED,
            table_partition_cols: Vec::new(),
        }
    }
}

#[async_trait]
impl ExonListingOptions for ListingVCFTableOptions {
    fn table_partition_cols(&self) -> &[Field] {
        &self.table_partition_cols
    }

    fn file_extension(&self) -> &str {
        &self.file_extension
    }

    fn file_compression_type(&self) -> FileCompressionType {
        self.file_compression_type
    }

    async fn create_physical_plan(
        &self,
        conf: FileScanConfig,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let scan = VCFScan::new(conf, self.file_compression_type)?;

        Ok(Arc::new(scan))
    }
}

#[async_trait]
impl ExonIndexedListingOptions for ListingVCFTableOptions {
    fn indexed(&self) -> bool {
        self.indexed
    }

    fn regions(&self) -> &[Region] {
        &self.regions
    }

    async fn create_physical_plan_with_regions(
        &self,
        conf: FileScanConfig,
        region: Vec<Region>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let scan = IndexedVCFScanner::new(conf, Arc::new(region[0].clone()))?;

        Ok(Arc::new(scan))
    }
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
            regions: Vec::new(),
        }
    }

    /// Set the region
    pub fn with_regions(self, regions: Vec<Region>) -> Self {
        Self {
            regions,
            indexed: true,
            ..self
        }
    }

    /// Set the file extension
    pub fn with_file_extension(self, file_extension: String) -> Self {
        Self {
            file_extension,
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
}

#[derive(Debug, Clone)]
/// A VCF listing table
pub struct ListingVCFTable<T> {
    table_schema: TableSchema,

    config: ExonListingConfig<T>,
}

impl<T> ListingVCFTable<T> {
    /// Create a new VCF listing table
    pub fn new(config: ExonListingConfig<T>, table_schema: TableSchema) -> Self {
        Self {
            table_schema,
            config,
        }
    }
}

#[async_trait]
impl<T: ExonIndexedListingOptions + 'static> TableProvider for ListingVCFTable<T> {
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
        tracing::trace!(
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

                filter_matches_partition_cols(f, self.config.options.table_partition_cols())
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
        let url = self
            .config
            .inner
            .table_paths
            .first()
            .ok_or(DataFusionError::Execution(
                "No table paths found in the configuration".to_string(),
            ))?;

        let object_store = state.runtime_env().object_store(url.object_store())?;

        let mut regions = filters
            .iter()
            .map(|f| {
                if let Expr::ScalarFunction(s) = f {
                    let r = infer_region::infer_region_from_udf(s, "vcf_region_filter")?;
                    Ok(r)
                } else {
                    Ok(None)
                }
            })
            .collect::<ExonResult<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();

        // add the regions from the configuration
        let config_regions = self.config.options.regions().to_vec();
        regions.extend(config_regions);

        if regions.len() > 1 {
            return Err(DataFusionError::NotImplemented(
                "Multiple regions are not supported yet".to_string(),
            ));
        }

        if regions.is_empty() && self.config.options.indexed() {
            return Err(DataFusionError::Plan(
                "INDEXED_VCF table requires a region filter. See the UDF 'vcf_region_filter'."
                    .to_string(),
            ));
        }

        if regions.is_empty() {
            let file_list = pruned_partition_list(
                state,
                &object_store,
                url,
                filters,
                self.config.options.file_extension(),
                self.config.options.table_partition_cols(),
            )
            .await?
            .try_collect::<Vec<_>>()
            .await?;

            let file_schema = self.table_schema.file_schema()?;
            let file_scan_config =
                FileScanConfigBuilder::new(url.object_store(), file_schema, vec![file_list])
                    .projection_option(projection.cloned())
                    .limit_option(limit)
                    .table_partition_cols(self.config.options.table_partition_cols().to_vec())
                    .build();

            let table = self
                .config
                .options
                .create_physical_plan(file_scan_config)
                .await?;

            return Ok(table);
        }

        let mut file_list = pruned_partition_list(
            state,
            &object_store,
            self.config.inner.table_paths.first().unwrap(),
            filters,
            self.config.options.file_extension(),
            self.config.options.table_partition_cols(),
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
                    &IndexedBGZFFile::Vcf,
                )
                .await?;

                file_partitions.extend(file_byte_range);
            }
        }

        let file_schema = self.table_schema.file_schema()?;
        let file_scan_config =
            FileScanConfigBuilder::new(url.object_store(), file_schema, vec![file_partitions])
                .projection_option(projection.cloned())
                .limit_option(limit)
                .table_partition_cols(self.config.options.table_partition_cols().to_vec())
                .build();

        let table = self
            .config
            .options
            .create_physical_plan_with_regions(file_scan_config, regions.to_vec())
            .await?;

        return Ok(table);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{datasources::vcf::IndexedVCFScanner, ExonSessionExt};

    use arrow::datatypes::{DataType, Field, Fields};
    use datafusion::{
        physical_plan::{coalesce_partitions::CoalescePartitionsExec, filter::FilterExec},
        prelude::SessionContext,
    };
    use exon_test::test_path;

    #[cfg(feature = "fixtures")]
    #[tokio::test]
    async fn test_chr17_queries() -> Result<(), Box<dyn std::error::Error>> {
        use crate::tests::test_fixture_table_url;

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

        let inner_item_fields = vec![
            Field::new("GT", DataType::Utf8, true),
            Field::new(
                "PL",
                DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                true,
            ),
            Field::new("PG", DataType::Int32, true),
        ];

        let inner_struct = DataType::Struct(Fields::from(inner_item_fields));
        let inner_list = DataType::List(Arc::new(Field::new("item", inner_struct, true)));
        assert_eq!(schema.field(8).data_type(), &inner_list);

        Ok(())
    }
}
