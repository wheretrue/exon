// Copyright 2024 WHERE TRUE Technologies.
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

use std::{any::Any, collections::HashMap, sync::Arc};

use arrow::datatypes::{Field, SchemaRef};
use async_trait::async_trait;
use datafusion::{
    common::Statistics,
    datasource::{
        listing::{ListingTableConfig, ListingTableUrl},
        physical_plan::FileScanConfig,
        TableProvider,
    },
    error::{DataFusionError, Result as DataFusionResult},
    execution::context::SessionState,
    logical_expr::{Expr, TableProviderFilterPushDown, TableType},
    physical_plan::ExecutionPlan,
};
use exon_common::TableSchema;
use exon_cram::ObjectStoreFastaRepositoryAdapter;
use exon_sam::SAMSchemaBuilder;
use futures::{StreamExt, TryStreamExt};
use noodles::{core::Region, sam::Header};
use object_store::{ObjectMeta, ObjectStore};
use tokio_util::io::StreamReader;

use crate::{
    datasources::hive_partition::filter_matches_partition_cols,
    error::{ExonError, Result},
    physical_plan::{
        file_scan_config_builder::FileScanConfigBuilder, infer_region,
        object_store::pruned_partition_list,
    },
};

use super::{
    index::augment_file_with_crai_record_chunks, indexed_scanner::IndexedCRAMScan,
    scanner::CRAMScan,
};

const CRAM_EXTENSION: &str = "cram";

#[derive(Debug, Clone)]
/// Configuration for a CRAM listing table.
pub struct ListingCRAMTableConfig {
    /// The options for the generic listing table.
    inner: ListingTableConfig,

    /// The options for the CRAM listing table.
    options: ListingCRAMTableOptions,
}

impl ListingCRAMTableConfig {
    /// Create a new CRAM listing table configuration.
    pub fn new(table_path: ListingTableUrl, options: ListingCRAMTableOptions) -> Self {
        Self {
            inner: ListingTableConfig::new(table_path),
            options,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct ListingCRAMTableOptions {
    /// The partition columns for the table.
    table_partition_cols: Vec<Field>,

    /// FASTA Reference
    fasta_reference: Option<String>,

    /// Whether to use the tag as struct.
    tag_as_struct: bool,

    /// If the underlying CRAM file is indexed.
    indexed: bool,

    /// The region filter for the table.
    region: Option<Region>,
}

impl TryFrom<&HashMap<String, String>> for ListingCRAMTableOptions {
    type Error = ExonError;

    fn try_from(options: &HashMap<String, String>) -> Result<Self, ExonError> {
        let fasta_reference = options.get("fasta_reference").map(|s| s.to_string());

        let indexed = options.get("indexed").map(|s| s == "true").unwrap_or(false);

        Ok(Self::default()
            .with_fasta_reference(fasta_reference)
            .with_indexed(indexed))
    }
}

impl ListingCRAMTableOptions {
    /// Set the FASTA reference.
    pub fn with_fasta_reference(mut self, fasta_reference: Option<String>) -> Self {
        self.fasta_reference = fasta_reference;
        self
    }

    /// Set the indexed option.
    pub fn with_indexed(mut self, indexed: bool) -> Self {
        self.indexed = indexed;
        self
    }

    /// Set the the tag_as_struct option.
    pub fn with_tag_as_struct(mut self, tag_as_struct: bool) -> Self {
        self.tag_as_struct = tag_as_struct;
        self
    }

    /// Set the partition columns for the table.
    pub fn with_table_partition_cols(mut self, table_partition_cols: Vec<Field>) -> Self {
        self.table_partition_cols = table_partition_cols;
        self
    }

    /// Infer the schema from the file.
    async fn infer_schema_from_object_meta(
        &self,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> Result<TableSchema> {
        if objects.is_empty() {
            return Err(ExonError::ExecutionError("No objects found".to_string()));
        }

        if !self.tag_as_struct {
            let builder = SAMSchemaBuilder::default()
                .with_partition_fields(self.table_partition_cols.clone());
            let table_schema = builder.build();

            return Ok(table_schema);
        }

        let get_result = store.get(&objects[0].location).await?;

        let stream_reader = Box::pin(get_result.into_stream().map_err(ExonError::from));
        let stream_reader = StreamReader::new(stream_reader);

        let mut cram_reader = noodles::cram::AsyncReader::new(stream_reader);

        cram_reader.read_file_definition().await?;
        let header = cram_reader.read_file_header().await?;
        let header: Header = header
            .to_owned()
            .parse()
            .map_err(|_| DataFusionError::Execution("Unable to parse header".to_string()))?;

        let mut schema_builder = SAMSchemaBuilder::default();

        let reference_sequence_repository = match &self.fasta_reference {
            Some(reference) => {
                let object_store_adapter = ObjectStoreFastaRepositoryAdapter::try_new(
                    store.clone(),
                    reference.to_string(),
                )
                .await?;

                noodles::fasta::Repository::new(object_store_adapter)
            }
            None => noodles::fasta::Repository::default(),
        };

        if let Some(Ok(record)) = cram_reader
            .records(&reference_sequence_repository, &header)
            .next()
            .await
        {
            schema_builder = schema_builder.with_tags_data_type_from_data(record.data())?;
        } else {
            return Err(ExonError::ExecutionError(
                "No records found in CRAM file".to_string(),
            ));
        }

        schema_builder = schema_builder.with_partition_fields(self.table_partition_cols.clone());

        Ok(schema_builder.build())
    }

    pub async fn infer_schema<'a>(
        &self,
        state: &SessionState,
        table_path: &'a ListingTableUrl,
    ) -> Result<TableSchema> {
        let store = state.runtime_env().object_store(table_path)?;

        let files = exon_common::object_store_files_from_table_path(
            &store,
            table_path.as_ref(),
            table_path.prefix(),
            CRAM_EXTENSION,
            None,
        )
        .await;

        // collect the files as a slice
        let files = files
            .try_collect::<Vec<_>>()
            .await
            .map_err(|e| DataFusionError::Execution(format!("Unable to get path info: {}", e)))?;

        self.infer_schema_from_object_meta(&store, &files).await
    }

    async fn create_physical_plan_with_region(
        &self,
        conf: FileScanConfig,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let scan = IndexedCRAMScan::new(conf, self.fasta_reference.clone());

        Ok(Arc::new(scan))
    }

    async fn create_physical_plan(
        &self,
        conf: FileScanConfig,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let scan = CRAMScan::new(conf, self.fasta_reference.clone());

        Ok(Arc::new(scan))
    }
}

#[derive(Debug, Clone)]
pub struct ListingCRAMTable {
    /// The paths to the listing table.
    table_paths: Vec<ListingTableUrl>,

    /// The table schema.
    table_schema: TableSchema,

    /// The options for the listing table.
    options: ListingCRAMTableOptions,
}

impl ListingCRAMTable {
    /// Create a new CRAM listing table.
    pub fn try_new(config: ListingCRAMTableConfig, table_schema: TableSchema) -> Result<Self> {
        Ok(Self {
            table_paths: config.inner.table_paths,
            table_schema,
            options: config.options,
        })
    }
}

#[async_trait]
impl TableProvider for ListingCRAMTable {
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
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        tracing::trace!(
            "cram table provider supports_filters_pushdown: {:?}",
            filters
        );

        Ok(filters
            .iter()
            .map(|f| {
                if let Expr::ScalarFunction(s) = f {
                    if s.name() == "cram_region_filter" && (s.args.len() == 2 || s.args.len() == 3)
                    {
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
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let object_store_url = self.table_paths[0].object_store();
        let object_store = state.runtime_env().object_store(object_store_url.clone())?;

        if !self.options.indexed {
            let file_list = pruned_partition_list(
                state,
                &object_store,
                &self.table_paths[0],
                filters,
                CRAM_EXTENSION,
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

        let regions = filters
            .iter()
            .filter_map(|f| {
                if let Expr::ScalarFunction(s) = f {
                    infer_region::infer_region_from_udf(s, "cram_region_filter")
                } else {
                    None
                }
            })
            .collect::<Vec<Region>>();

        let regions = if self.options.indexed {
            if regions.len() == 1 {
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

        if regions.is_empty() && self.options.indexed {
            return Err(DataFusionError::Plan(
                "An indexed CRAM table type requires a region filter. See the 'cram_region_filter' function.".to_string(),
            ));
        }

        if regions.len() > 1 {
            return Err(DataFusionError::Plan(
                "Only one region filter is supported".to_string(),
            ));
        }

        let mut file_list = pruned_partition_list(
            state,
            &object_store,
            &self.table_paths[0],
            filters,
            CRAM_EXTENSION,
            &self.options.table_partition_cols,
        )
        .await?;

        let mut file_partition_with_ranges = Vec::new();
        let region = regions[0].clone();

        while let Some(f) = file_list.next().await {
            let f = f?;

            let s = object_store.get(&f.object_meta.location).await?;

            let s = s.into_stream().map_err(DataFusionError::from);
            let stream_reader = Box::pin(s);
            let stream_reader = StreamReader::new(stream_reader);

            let mut cram_reader = noodles::cram::AsyncReader::new(stream_reader);
            cram_reader.read_file_definition().await?;

            let header = cram_reader.read_file_header().await?;
            let header: Header = header.parse().map_err(|_| {
                DataFusionError::Execution("Failed to parse CRAM header".to_string())
            })?;

            let file_byte_range =
                augment_file_with_crai_record_chunks(object_store.clone(), &header, &f, &region)
                    .await?;

            file_partition_with_ranges.extend(file_byte_range);
        }

        let file_scan_config = FileScanConfig {
            object_store_url: object_store_url.clone(),
            file_schema: self.table_schema.file_schema()?,
            file_groups: vec![file_partition_with_ranges],
            statistics: Statistics::new_unknown(self.table_schema.file_schema()?.as_ref()),
            projection: projection.cloned(),
            limit,
            output_ordering: Vec::new(),
            table_partition_cols: self.options.table_partition_cols.clone(),
        };

        let table = self
            .options
            .create_physical_plan_with_region(file_scan_config)
            .await?;

        return Ok(table);
    }
}
