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

use std::{collections::HashMap, str::FromStr, sync::Arc};

use arrow::datatypes::{DataType, Field, SchemaRef};
use async_trait::async_trait;
use datafusion::{
    datasource::{
        file_format::file_compression_type::FileCompressionType, listing::ListingTableUrl,
        provider::TableProviderFactory, TableProvider,
    },
    execution::context::SessionState,
    logical_expr::CreateExternalTable,
};
use url::Url;

use crate::{config::extract_config_from_state, datasources::ExonFileType, ExonRuntimeEnvExt};

use super::{
    bam::table_provider::{ListingBAMTable, ListingBAMTableOptions},
    bcf::table_provider::{ListingBCFTable, ListingBCFTableOptions},
    bed::table_provider::{ListingBEDTable, ListingBEDTableOptions},
    bigwig,
    cram::table_provider::{ListingCRAMTableConfig, ListingCRAMTableOptions},
    exon_listing_table_options::ExonListingConfig,
    fasta::table_provider::{ListingFASTATable, ListingFASTATableOptions},
    fastq::table_provider::{ListingFASTQTable, ListingFASTQTableOptions},
    gff::table_provider::{ListingGFFTable, ListingGFFTableOptions},
    gtf::table_provider::{ListingGTFTable, ListingGTFTableOptions},
    hmmdomtab::table_provider::{ListingHMMDomTabTable, ListingHMMDomTabTableOptions},
    sam::table_provider::{ListingSAMTable, ListingSAMTableOptions},
    vcf::{ListingVCFTable, ListingVCFTableOptions},
};

#[cfg(feature = "fcs")]
use super::fcs::table_provider::{ListingFCSTable, ListingFCSTableConfig, ListingFCSTableOptions};

#[cfg(feature = "mzml")]
use super::mzml::table_provider::{ListingMzMLTable, ListingMzMLTableOptions};

#[cfg(feature = "genbank")]
use super::genbank::table_provider::{ListingGenbankTable, ListingGenbankTableOptions};

const FILE_EXTENSION_OPTION: &str = "file_extension";
const INDEXED_OPTION: &str = "indexed";
const INDEXED_TRUE_VALUE: &str = "true";

/// A `ListingTableFactory` that adapts Exon FileFormats to `TableProvider`s.
#[derive(Debug, Clone, Default)]
pub struct ExonListingTableFactory {}

impl ExonListingTableFactory {
    /// Create a new `ListingTableFactory`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a new table provider from a file type.
    pub async fn create_from_file_type(
        &self,
        state: &SessionState,
        file_type: ExonFileType,
        file_compression_type: FileCompressionType,
        location: String,
        table_partition_cols: Vec<Field>,
        options: &HashMap<String, String>,
    ) -> datafusion::common::Result<Arc<dyn TableProvider>> {
        let table_path = ListingTableUrl::parse(&location)?;

        let exon_config_extension = extract_config_from_state(state)?;

        match file_type {
            ExonFileType::BAM => {
                let options = ListingBAMTableOptions::default()
                    .with_table_partition_cols(table_partition_cols)
                    .with_tag_as_struct(exon_config_extension.bam_parse_tags);

                let table_schema = options.infer_schema(state, &table_path).await?;

                let config = ExonListingConfig::new_with_options(table_path, options);
                let table = ListingBAMTable::new(config, table_schema);

                Ok(Arc::new(table))
            }
            ExonFileType::BED => {
                let options = ListingBEDTableOptions::new(file_compression_type)
                    .with_table_partition_cols(table_partition_cols);

                let table_schema = options.infer_schema()?;

                let config = ExonListingConfig::new_with_options(table_path, options);
                let table = ListingBEDTable::new(config, table_schema);

                Ok(Arc::new(table))
            }
            ExonFileType::SAM => {
                let options = ListingSAMTableOptions::default()
                    .with_table_partition_cols(table_partition_cols)
                    .with_tag_as_struct(exon_config_extension.sam_parse_tags);

                let table_schema = options.infer_schema(state, &table_path).await?;

                let config = ExonListingConfig::new_with_options(table_path, options);
                let table = ListingSAMTable::new(config, table_schema);

                Ok(Arc::new(table))
            }
            ExonFileType::IndexedGFF => {
                if file_compression_type != FileCompressionType::GZIP {
                    return Err(datafusion::error::DataFusionError::Execution(
                        "INDEXED_GFF files must be compressed with gzip".to_string(),
                    ));
                }

                let options = ListingGFFTableOptions::new(file_compression_type)
                    .with_indexed(true)
                    .with_table_partition_cols(table_partition_cols);

                let file_schema = options.infer_schema().await?;

                let config = ExonListingConfig::new_with_options(table_path, options);
                let table = ListingGFFTable::new(config, file_schema);

                Ok(Arc::new(table))
            }
            ExonFileType::GFF => {
                let options = ListingGFFTableOptions::new(file_compression_type)
                    .with_indexed(
                        options.get(INDEXED_OPTION) == Some(&INDEXED_TRUE_VALUE.to_string()),
                    )
                    .with_file_extension(options.get(FILE_EXTENSION_OPTION).cloned())
                    .with_table_partition_cols(table_partition_cols);

                let file_schema = options.infer_schema().await?;

                let config = ExonListingConfig::new_with_options(table_path, options);
                let table = ListingGFFTable::new(config, file_schema);

                Ok(Arc::new(table))
            }
            #[cfg(feature = "mzml")]
            ExonFileType::MZML => {
                let options = ListingMzMLTableOptions::new(file_compression_type)
                    .with_table_partition_cols(table_partition_cols);
                let schema = options.infer_schema().await?;

                let config = ExonListingConfig::new_with_options(table_path, options);
                let table = ListingMzMLTable::new(config, schema);

                Ok(Arc::new(table))
            }
            ExonFileType::HMMDOMTAB => {
                let options = ListingHMMDomTabTableOptions::new(file_compression_type)
                    .with_table_partition_cols(table_partition_cols);
                let table_schema = options.infer_schema().await?;

                let config = ExonListingConfig::new_with_options(table_path, options);
                let table = ListingHMMDomTabTable::new(config, table_schema);

                Ok(Arc::new(table))
            }
            ExonFileType::GTF => {
                let options = ListingGTFTableOptions::new(file_compression_type)
                    .with_table_partition_cols(table_partition_cols);
                let table_schema = options.infer_schema();

                let config = ExonListingConfig::new_with_options(table_path, options);
                let table = ListingGTFTable::new(config, table_schema);

                Ok(Arc::new(table))
            }
            #[cfg(feature = "genbank")]
            ExonFileType::GENBANK => {
                let options = ListingGenbankTableOptions::new(file_compression_type);
                let schema = options.infer_schema().await?;

                let config = ExonListingConfig::new_with_options(table_path, options);
                let table = ListingGenbankTable::new(config, schema);

                Ok(Arc::new(table))
            }
            ExonFileType::BCF => {
                let options = ListingBCFTableOptions::default()
                    .with_table_partition_cols(table_partition_cols);
                let table_schema = options.infer_schema(state, &table_path).await?;

                let config = ExonListingConfig::new_with_options(table_path, options);
                let table = ListingBCFTable::new(config, table_schema);

                Ok(Arc::new(table))
            }
            ExonFileType::VCF => {
                let vcf_options = ListingVCFTableOptions::new(file_compression_type, false)
                    .with_table_partition_cols(table_partition_cols);
                let table_schema = vcf_options.infer_schema(state, &table_path).await?;

                let config = ExonListingConfig::new_with_options(table_path, vcf_options);

                let table = ListingVCFTable::new(config, table_schema);
                Ok(Arc::new(table))
            }
            ExonFileType::IndexedVCF => {
                let vcf_options = ListingVCFTableOptions::new(file_compression_type, true)
                    .with_table_partition_cols(table_partition_cols);

                let table_schema = vcf_options.infer_schema(state, &table_path).await?;

                let config = ExonListingConfig::new_with_options(table_path, vcf_options);

                let table = ListingVCFTable::new(config, table_schema);
                Ok(Arc::new(table))
            }
            ExonFileType::IndexedBAM => {
                let options = ListingBAMTableOptions::default()
                    .with_indexed(true)
                    .with_table_partition_cols(table_partition_cols)
                    .with_tag_as_struct(exon_config_extension.bam_parse_tags);

                let table_schema = options.infer_schema(state, &table_path).await?;

                let config = ExonListingConfig::new_with_options(table_path, options);

                let table = ListingBAMTable::new(config, table_schema);
                Ok(Arc::new(table))
            }
            ExonFileType::FASTA | ExonFileType::FA | ExonFileType::FAA | ExonFileType::FNA => {
                let extension = options.get(FILE_EXTENSION_OPTION).map(|s| s.as_str());

                let table_options = ListingFASTATableOptions::new(file_compression_type)
                    .with_table_partition_cols(table_partition_cols)
                    .with_some_file_extension(extension);

                let schema = table_options.infer_schema(state).await?;

                let config = ExonListingConfig::new_with_options(table_path, table_options);
                let table = ListingFASTATable::try_new(config, schema)?;

                Ok(Arc::new(table))
            }
            ExonFileType::FASTQ | ExonFileType::FQ => {
                let extension = options.get(FILE_EXTENSION_OPTION).map(|s| s.as_str());

                let options = ListingFASTQTableOptions::new(file_compression_type)
                    .with_table_partition_cols(table_partition_cols)
                    .with_some_file_extension(extension);

                let schema = options.infer_schema();

                let config = ExonListingConfig::new_with_options(table_path, options);
                let table = ListingFASTQTable::try_new(config, schema)?;

                Ok(Arc::new(table))
            }
            ExonFileType::CRAM => {
                let options = ListingCRAMTableOptions::try_from(options)?
                    .with_table_partition_cols(table_partition_cols)
                    .with_tag_as_struct(exon_config_extension.cram_parse_tags);

                let table_schema = options.infer_schema(state, &table_path).await?;

                let config = ListingCRAMTableConfig::new(table_path, options);

                let table = crate::datasources::cram::table_provider::ListingCRAMTable::try_new(
                    config,
                    table_schema,
                )?;

                Ok(Arc::new(table))
            }
            ExonFileType::BigWigValue => {
                let options = super::bigwig::value::ListingTableOptions::new()
                    .with_table_partition_cols(table_partition_cols);

                let table_schema = options.infer_schema()?;

                let config = ExonListingConfig::new_with_options(table_path, options);
                let table = bigwig::value::ListingTable::new(config, table_schema);

                Ok(Arc::new(table))
            }
            ExonFileType::BigWigZoom => {
                let reduction_level = options
                    .get("reduction_level")
                    .ok_or(datafusion::error::DataFusionError::Execution(
                        "BigWigZoom files must have a reduction level".to_string(),
                    ))?
                    .parse::<u32>()
                    .map_err(|e| {
                        datafusion::error::DataFusionError::Execution(format!(
                            "Failed to parse reduction level: {}",
                            e
                        ))
                    })?;

                let options = bigwig::zoom::ListingTableOptions::new(reduction_level)
                    .with_table_partition_cols(table_partition_cols);

                let table_schema = options.infer_schema()?;

                let config = bigwig::zoom::ListingTableConfig::new(table_path, options);
                let table = bigwig::zoom::ListingTable::try_new(config, table_schema)?;

                Ok(Arc::new(table))
            }
            #[cfg(feature = "fcs")]
            ExonFileType::FCS => {
                let options = ListingFCSTableOptions::new(file_compression_type)
                    .with_table_partition_cols(table_partition_cols);

                let table_schema = options.infer_schema(state, &table_path).await?;

                let config = ListingFCSTableConfig::new(table_path).with_options(options);
                let table = ListingFCSTable::try_new(config, table_schema)?;

                Ok(Arc::new(table))
            }
        }
    }
}

#[async_trait]
impl TableProviderFactory for ExonListingTableFactory {
    async fn create(
        &self,
        state: &SessionState,
        cmd: &CreateExternalTable,
    ) -> datafusion::common::Result<Arc<dyn TableProvider>> {
        let file_compression_type: FileCompressionType = cmd.file_compression_type.into();

        let schema: SchemaRef = Arc::new(cmd.schema.as_ref().to_owned().into());
        let table_partition_cols = cmd
            .table_partition_cols
            .iter()
            .map(|col| match schema.field_with_name(col) {
                Ok(f) => Ok(f.clone()),
                Err(_) => Ok(Field::new(col, DataType::Utf8, true)),
            })
            .collect::<datafusion::common::Result<Vec<_>>>()?
            .into_iter()
            .collect::<Vec<_>>();

        let file_type = ExonFileType::from_str(&cmd.file_type)?;

        // Attempt to register the object store if it hasn't been registered yet.
        let table_path = ListingTableUrl::parse(&cmd.location)?;
        let url: &Url = table_path.as_ref();

        state
            .runtime_env()
            .exon_register_object_store_url(url)
            .await?;

        let options = &cmd.options;

        self.create_from_file_type(
            state,
            file_type,
            file_compression_type,
            cmd.location.clone(),
            table_partition_cols,
            options,
        )
        .await
    }
}

#[cfg(not(target_os = "windows"))]
#[cfg(test)]
mod tests {
    use std::{path::PathBuf, sync::Arc};

    use datafusion::{
        catalog::{listing_schema::ListingSchemaProvider, CatalogProvider, MemoryCatalogProvider},
        prelude::SessionContext,
    };
    use object_store::local::LocalFileSystem;

    use crate::{datasources::ExonListingTableFactory, ExonSessionExt};

    #[tokio::test]
    async fn test_in_catalog() -> Result<(), Box<dyn std::error::Error>> {
        let mem_catalog: MemoryCatalogProvider = MemoryCatalogProvider::new();
        let object_store = Arc::new(LocalFileSystem::new());

        let cargo_manifest_path = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR")?);

        let schema = ListingSchemaProvider::new(
            "file://localhost".to_string(),
            cargo_manifest_path
                .join("test-data")
                .join("datasources")
                .join("sam")
                .to_str()
                .ok_or("Failed to create path")?
                .into(),
            Arc::new(ExonListingTableFactory::default()),
            object_store,
            "SAM".to_string(),
            false,
        );

        mem_catalog.register_schema("exon", Arc::new(schema))?;

        // let session_config = SessionConfig::from_env()?;
        // let runtime_env = create_runtime_env()?;
        // let ctx = SessionContext::new_with_config_rt(session_config.clone(), Arc::new(runtime_env));
        let ctx = SessionContext::new_exon();

        ctx.register_catalog("exon", Arc::new(mem_catalog));
        ctx.refresh_catalogs().await?;

        let gotten_catalog = ctx.catalog("exon").ok_or("No catalog found")?;
        let schema_names = gotten_catalog.schema_names();
        assert_eq!(schema_names, vec!["exon"]);

        let new_schema = gotten_catalog.schema("exon").ok_or("No schema found")?;
        let tables = new_schema.table_names();
        assert_eq!(tables, vec!["test"]);

        Ok(())
    }
}
