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

use std::{str::FromStr, sync::Arc};

use arrow::datatypes::{DataType, SchemaRef};
use async_trait::async_trait;
use datafusion::{
    common::FileCompressionType,
    datasource::{listing::ListingTableUrl, provider::TableProviderFactory, TableProvider},
    execution::context::SessionState,
    logical_expr::CreateExternalTable,
};

use crate::datasources::ExonFileType;

use super::{
    bam::table_provider::{ListingBAMTable, ListingBAMTableConfig, ListingBAMTableOptions},
    bcf::table_provider::{ListingBCFTable, ListingBCFTableConfig, ListingBCFTableOptions},
    bed::table_provider::{ListingBEDTable, ListingBEDTableConfig, ListingBEDTableOptions},
    fasta::table_provider::{ListingFASTATable, ListingFASTATableConfig, ListingFASTATableOptions},
    fastq::table_provider::{ListingFASTQTable, ListingFASTQTableConfig, ListingFASTQTableOptions},
    gff::table_provider::{ListingGFFTable, ListingGFFTableConfig, ListingGFFTableOptions},
    gtf::table_provider::{ListingGTFTable, ListingGTFTableConfig, ListingGTFTableOptions},
    hmmdomtab::table_provider::{
        ListingHMMDomTabTable, ListingHMMDomTabTableConfig, ListingHMMDomTabTableOptions,
    },
    sam::table_provider::{ListingSAMTable, ListingSAMTableConfig, ListingSAMTableOptions},
    vcf::{ListingVCFTable, ListingVCFTableOptions, VCFListingTableConfig},
};

#[cfg(feature = "fcs")]
use super::fcs::table_provider::{ListingFCSTable, ListingFCSTableConfig, ListingFCSTableOptions};

#[cfg(feature = "mzml")]
use super::mzml::table_provider::{
    ListingMzMLTable, ListingMzMLTableConfig, ListingMzMLTableOptions,
};

#[cfg(feature = "genbank")]
use super::genbank::table_provider::{
    ListingGenbankTable, ListingGenbankTableConfig, ListingGenbankTableOptions,
};

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
        table_partition_cols: Vec<(String, DataType)>,
    ) -> datafusion::common::Result<Arc<dyn TableProvider>> {
        let table_path = ListingTableUrl::parse(&location)?;

        match file_type {
            ExonFileType::SAM => {
                let options = ListingSAMTableOptions::default();
                let schema = options.infer_schema().await?;

                let config = ListingSAMTableConfig::new(table_path).with_options(options);
                let table = ListingSAMTable::try_new(config, schema)?;

                Ok(Arc::new(table))
            }
            #[cfg(feature = "mzml")]
            ExonFileType::MZML => {
                let options = ListingMzMLTableOptions::new(file_compression_type);
                let schema = options.infer_schema().await?;

                let config = ListingMzMLTableConfig::new(table_path).with_options(options);
                let table = ListingMzMLTable::try_new(config, schema)?;

                Ok(Arc::new(table))
            }
            ExonFileType::HMMDOMTAB => {
                let options = ListingHMMDomTabTableOptions::new(file_compression_type);
                let schema = options.infer_schema().await?;

                let config = ListingHMMDomTabTableConfig::new(table_path).with_options(options);
                let table = ListingHMMDomTabTable::try_new(config, schema)?;

                Ok(Arc::new(table))
            }
            ExonFileType::BAM => {
                let options = ListingBAMTableOptions::default();
                let schema = options.infer_schema(state, &table_path).await?;

                let config = ListingBAMTableConfig::new(table_path).with_options(options);
                let table = ListingBAMTable::try_new(config, schema)?;

                Ok(Arc::new(table))
            }
            ExonFileType::BED => {
                let options = ListingBEDTableOptions::new(file_compression_type);
                let schema = options.infer_schema().await?;

                let config = ListingBEDTableConfig::new(table_path).with_options(options);
                let table = ListingBEDTable::try_new(config, schema)?;

                Ok(Arc::new(table))
            }
            ExonFileType::GTF => {
                let options = ListingGTFTableOptions::new(file_compression_type);
                let schema = options.infer_schema().await?;

                let config = ListingGTFTableConfig::new(table_path).with_options(options);
                let table = ListingGTFTable::try_new(config, schema)?;

                Ok(Arc::new(table))
            }
            #[cfg(feature = "genbank")]
            ExonFileType::GENBANK => {
                let options = ListingGenbankTableOptions::new(file_compression_type);
                let schema = options.infer_schema().await?;

                let config = ListingGenbankTableConfig::new(table_path).with_options(options);
                let table = ListingGenbankTable::try_new(config, schema)?;

                Ok(Arc::new(table))
            }
            ExonFileType::BCF => {
                let options = ListingBCFTableOptions::default();
                let schema = options.infer_schema(state, &table_path).await?;

                let config = ListingBCFTableConfig::new(table_path).with_options(options);
                let table = ListingBCFTable::try_new(config, schema)?;

                Ok(Arc::new(table))
            }
            ExonFileType::VCF => {
                let vcf_options = ListingVCFTableOptions::new(file_compression_type, false);
                let schema = vcf_options.infer_schema(state, &table_path).await?;

                let config = VCFListingTableConfig::new(table_path).with_options(vcf_options);

                let table = ListingVCFTable::try_new(config, schema)?;
                Ok(Arc::new(table))
            }
            ExonFileType::IndexedVCF => {
                let vcf_options = ListingVCFTableOptions::new(file_compression_type, true);
                let schema = vcf_options.infer_schema(state, &table_path).await?;

                let config = VCFListingTableConfig::new(table_path).with_options(vcf_options);

                let table = ListingVCFTable::try_new(config, schema)?;
                Ok(Arc::new(table))
            }
            ExonFileType::IndexedBAM => {
                let bam_options = ListingBAMTableOptions::default().with_indexed(true);
                let schema = bam_options.infer_schema(state, &table_path).await?;

                let config = ListingBAMTableConfig::new(table_path).with_options(bam_options);

                let table = ListingBAMTable::try_new(config, schema)?;
                Ok(Arc::new(table))
            }
            ExonFileType::FASTA => {
                let options = ListingFASTATableOptions::new(file_compression_type)
                    .with_table_partition_cols(table_partition_cols);
                let schema = options.infer_schema().await?;

                let config = ListingFASTATableConfig::new(table_path).with_options(options);
                let table = ListingFASTATable::try_new(config, schema)?;

                Ok(Arc::new(table))
            }
            ExonFileType::FASTQ => {
                let options = ListingFASTQTableOptions::new(file_compression_type)
                    .with_table_partition_cols(table_partition_cols);
                let schema = options.infer_schema().await?;

                let config = ListingFASTQTableConfig::new(table_path).with_options(options);
                let table = ListingFASTQTable::try_new(config, schema)?;

                Ok(Arc::new(table))
            }
            ExonFileType::GFF => {
                let options = ListingGFFTableOptions::new(file_compression_type)
                    .with_table_partition_cols(table_partition_cols);
                let file_schema = options.infer_schema().await?;

                let config = ListingGFFTableConfig::new(table_path).with_options(options);
                let table = ListingGFFTable::try_new(config, file_schema)?;

                Ok(Arc::new(table))
            }
            #[cfg(feature = "fcs")]
            ExonFileType::FCS => {
                let options = ListingFCSTableOptions::new(file_compression_type);
                let schema = options.infer_schema(state, &table_path).await?;

                let config = ListingFCSTableConfig::new(table_path).with_options(options);
                let table = ListingFCSTable::try_new(config, schema)?;

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
                Ok(field) => Ok((field.name().clone(), field.data_type().clone())),
                Err(_) => Ok((col.clone(), DataType::Utf8)),
            })
            .collect::<datafusion::common::Result<Vec<_>>>()?
            .into_iter()
            .collect::<Vec<_>>();

        let file_type = ExonFileType::from_str(&cmd.file_type).map_err(|_| {
            datafusion::error::DataFusionError::Execution(format!(
                "Unsupported file type: {}",
                &cmd.file_type,
            ))
        })?;

        self.create_from_file_type(
            state,
            file_type,
            file_compression_type,
            cmd.location.clone(),
            table_partition_cols,
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
        error::DataFusionError,
        execution::runtime_env::{RuntimeConfig, RuntimeEnv},
        prelude::{SessionConfig, SessionContext},
    };
    use object_store::local::LocalFileSystem;

    use crate::datasources::ExonListingTableFactory;

    fn create_runtime_env() -> Result<RuntimeEnv, DataFusionError> {
        let rn_config = RuntimeConfig::new();
        RuntimeEnv::new(rn_config)
    }

    // Don't include this test on windows
    #[tokio::test]
    async fn test_in_catalog() {
        let mem_catalog: MemoryCatalogProvider = MemoryCatalogProvider::new();
        let object_store = Arc::new(LocalFileSystem::new());

        let cargo_manifest_path = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap());

        let schema = ListingSchemaProvider::new(
            "file://localhost".to_string(),
            cargo_manifest_path
                .join("test-data")
                .join("datasources")
                .join("sam")
                .to_str()
                .unwrap()
                .into(),
            Arc::new(ExonListingTableFactory::default()),
            object_store,
            "SAM".to_string(),
            false,
        );

        mem_catalog
            .register_schema("exon", Arc::new(schema))
            .unwrap();

        let session_config = SessionConfig::from_env().unwrap();
        let runtime_env = create_runtime_env().unwrap();
        let ctx = SessionContext::with_config_rt(session_config.clone(), Arc::new(runtime_env));

        ctx.register_catalog("exon", Arc::new(mem_catalog));
        ctx.refresh_catalogs().await.unwrap();

        let gotten_catalog = ctx.catalog("exon").unwrap();
        let schema_names = gotten_catalog.schema_names();
        assert_eq!(schema_names, vec!["exon"]);

        let new_schema = gotten_catalog.schema("exon").unwrap();
        let tables = new_schema.table_names();
        assert_eq!(tables, vec!["test"]);
    }
}
