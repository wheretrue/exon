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

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::{
    datasource::{
        file_format::file_compression_type::FileCompressionType, listing::ListingTableUrl,
        TableProvider,
    },
    error::{DataFusionError, Result},
    execution::{context::SessionState, runtime_env::RuntimeEnv},
    prelude::{DataFrame, SessionConfig, SessionContext},
};
use noodles::core::Region;

use crate::{
    datasources::{
        bcf::table_provider::{ListingBCFTable, ListingBCFTableConfig, ListingBCFTableOptions},
        vcf::{ListingVCFTable, ListingVCFTableOptions, VCFListingTableConfig},
        ExonFileType, ExonListingTableFactory,
    },
    new_exon_config,
    physical_optimizer::file_repartitioner::ExonRoundRobin,
    udfs::{
        bam_region_filter::register_bam_region_filter_udf,
        vcf::vcf_region_filter::register_vcf_region_filter_udf,
    },
};

/// Extension trait for [`SessionContext`] that adds Exon-specific functionality.
///
/// # Example
///
/// ```rust
/// use exon::ExonSessionExt;
///
/// use datafusion::prelude::*;
/// use datafusion::error::Result;
/// use datafusion::common::FileCompressionType;
///
/// # #[tokio::main]
/// # async fn main() -> Result<()> {
/// let ctx = SessionContext::new();
///
/// let file_compression = FileCompressionType::ZSTD;
/// let df = ctx.read_fasta("test-data/datasources/fasta/test.fasta.zstd", Some(file_compression)).await?;
///
/// assert_eq!(df.schema().fields().len(), 3);
/// assert_eq!(df.schema().field(0).name(), "id");
/// assert_eq!(df.schema().field(1).name(), "description");
/// assert_eq!(df.schema().field(2).name(), "sequence");
///
/// let results = df.collect().await?;
/// assert_eq!(results.len(), 1);  // 1 batch, small dataset
///
/// # Ok(())
/// # }
/// ```
#[async_trait]
pub trait ExonSessionExt {
    /// Reads a Exon table from the given path of a certain type and optional compression type.
    async fn read_exon_table(
        &self,
        table_path: &str,
        file_type: ExonFileType,
        file_compression_type: Option<FileCompressionType>,
    ) -> Result<DataFrame, DataFusionError>;

    /// Create a new Exon based [`SessionContext`].
    fn new_exon() -> SessionContext {
        let exon_config = new_exon_config();

        Self::with_config_exon(exon_config)
    }

    /// Create a new Exon based [`SessionContext`] with the given config.
    fn with_config_exon(config: SessionConfig) -> SessionContext {
        let runtime = Arc::new(RuntimeEnv::default());
        Self::with_config_rt_exon(config, runtime)
    }

    /// Create a new Exon based [`SessionContext`] with the given config and runtime.
    fn with_config_rt_exon(config: SessionConfig, runtime: Arc<RuntimeEnv>) -> SessionContext {
        let round_robin_optimizer = ExonRoundRobin::default();

        let mut state = SessionState::new_with_config_rt(config, runtime)
            .with_physical_optimizer_rules(vec![Arc::new(round_robin_optimizer)]);

        let sources = vec![
            "BAM",
            "BCF",
            "BED",
            "FASTA",
            "FASTQ",
            "GENBANK",
            "GFF",
            "GTF",
            "HMMDOMTAB",
            "VCF",
            "INDEXED_VCF",
            "INDEXED_BAM",
            "SAM",
            #[cfg(feature = "mzml")]
            "MZML",
            #[cfg(feature = "fcs")]
            "FCS",
        ];

        for source in sources {
            state
                .table_factories_mut()
                .insert(source.into(), Arc::new(ExonListingTableFactory::default()));
        }

        let ctx = SessionContext::new_with_state(state);

        // Register the mass spec UDFs
        #[cfg(feature = "mzml")]
        for mass_spec_udf in crate::udfs::massspec::register_udfs() {
            ctx.register_udf(mass_spec_udf);
        }

        // Register the sequence UDFs
        for sequence_udf in crate::udfs::sequence::register_udfs() {
            ctx.register_udf(sequence_udf);
        }

        // Register the sam flag UDFs
        for sam_udf in crate::udfs::samflags::register_udfs() {
            ctx.register_udf(sam_udf);
        }

        // Register the VCF UDFs
        for vcf_udf in crate::udfs::vcf::register_vcf_udfs() {
            ctx.register_udf(vcf_udf);
        }

        // Register BAM region filter UDF
        register_bam_region_filter_udf(&ctx);

        // Register VCF region filter UDF
        register_vcf_region_filter_udf(&ctx);

        ctx
    }

    /// Register a Exon table from the given path of a certain type and optional compression type.
    async fn register_exon_table(
        &self,
        name: &str,
        table_path: &str,
        file_type: &str,
    ) -> Result<(), DataFusionError>;

    /// Read a FASTA file.
    async fn read_fasta(
        &self,
        table_path: &str,
        file_compression_type: Option<FileCompressionType>,
    ) -> Result<DataFrame, DataFusionError> {
        return self
            .read_exon_table(table_path, ExonFileType::FASTA, file_compression_type)
            .await;
    }

    /// Read a BAM file.
    async fn read_bam(
        &self,
        table_path: &str,
        file_compression_type: Option<FileCompressionType>,
    ) -> Result<DataFrame, DataFusionError> {
        return self
            .read_exon_table(table_path, ExonFileType::BAM, file_compression_type)
            .await;
    }

    /// Infer the file type and compression, then read the file.
    async fn read_inferred_exon_table(
        &self,
        table_path: &str,
    ) -> Result<DataFrame, DataFusionError>;

    /// Read a SAM file.
    async fn read_sam(
        &self,
        table_path: &str,
        file_compression_type: Option<FileCompressionType>,
    ) -> Result<DataFrame, DataFusionError> {
        return self
            .read_exon_table(table_path, ExonFileType::SAM, file_compression_type)
            .await;
    }

    /// Read a FASTQ file.
    async fn read_fastq(
        &self,
        table_path: &str,
        file_compression_type: Option<FileCompressionType>,
    ) -> Result<DataFrame, DataFusionError> {
        return self
            .read_exon_table(table_path, ExonFileType::FASTQ, file_compression_type)
            .await;
    }

    /// Read a VCF file.
    async fn read_vcf(
        &self,
        table_path: &str,
        file_compression_type: Option<FileCompressionType>,
    ) -> Result<DataFrame, DataFusionError> {
        return self
            .read_exon_table(table_path, ExonFileType::VCF, file_compression_type)
            .await;
    }

    /// Read a BCF file.
    async fn read_bcf(
        &self,
        table_path: &str,
        file_compression_type: Option<FileCompressionType>,
    ) -> Result<DataFrame, DataFusionError> {
        return self
            .read_exon_table(table_path, ExonFileType::BCF, file_compression_type)
            .await;
    }

    /// Read a GFF file.
    async fn read_gff(
        &self,
        table_path: &str,
        file_compression_type: Option<FileCompressionType>,
    ) -> Result<DataFrame, DataFusionError> {
        return self
            .read_exon_table(table_path, ExonFileType::GFF, file_compression_type)
            .await;
    }

    /// Read a GTF file.
    async fn read_gtf(
        &self,
        table_path: &str,
        file_compression_type: Option<FileCompressionType>,
    ) -> Result<DataFrame, DataFusionError> {
        self.read_exon_table(table_path, ExonFileType::GTF, file_compression_type)
            .await
    }

    /// Read a BED file.
    async fn read_bed(
        &self,
        table_path: &str,
        file_compression_type: Option<FileCompressionType>,
    ) -> Result<DataFrame, DataFusionError> {
        return self
            .read_exon_table(table_path, ExonFileType::BED, file_compression_type)
            .await;
    }

    /// Read a GENBANK file.
    #[cfg(feature = "genbank")]
    async fn read_genbank(
        &self,
        table_path: &str,
        file_compression_type: Option<FileCompressionType>,
    ) -> Result<DataFrame, DataFusionError> {
        return self
            .read_exon_table(table_path, ExonFileType::GENBANK, file_compression_type)
            .await;
    }

    /// Read a HMMER DOMTAB file.
    async fn read_hmm_dom_tab(
        &self,
        table_path: &str,
        file_compression_type: Option<FileCompressionType>,
    ) -> Result<DataFrame, DataFusionError> {
        return self
            .read_exon_table(table_path, ExonFileType::HMMDOMTAB, file_compression_type)
            .await;
    }

    /// Read a MZML file.
    #[cfg(feature = "mzml")]
    async fn read_mzml(
        &self,
        table_path: &str,
        file_compression_type: Option<FileCompressionType>,
    ) -> Result<DataFrame, DataFusionError> {
        return self
            .read_exon_table(table_path, ExonFileType::MZML, file_compression_type)
            .await;
    }

    /// Register a VCF file.
    async fn register_vcf_file(
        &self,
        table_name: &str,
        table_path: &str,
    ) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError>;

    /// Query a BCF file.
    ///
    /// File must be indexed and index file must be in the same directory as the BCF file.
    async fn query_bcf_file(
        &self,
        table_path: &str,
        query: &str,
    ) -> Result<DataFrame, DataFusionError>;

    /// Query a BAM file.
    ///
    /// File must be indexed and index file must be in the same directory as the BAM file.
    // async fn query_bam_file(
    //     &self,
    //     table_path: &str,
    //     query: &str,
    // ) -> Result<DataFrame, DataFusionError>;

    /// Query a gzipped indexed VCF file.
    ///
    /// File must be indexed and index file must be in the same directory as the VCF file.
    async fn query_vcf_file(
        &self,
        table_path: &str,
        query: &str,
    ) -> Result<DataFrame, DataFusionError>;
}

#[async_trait]
impl ExonSessionExt for SessionContext {
    async fn read_exon_table(
        &self,
        table_path: &str,
        file_type: ExonFileType,
        file_compression_type: Option<FileCompressionType>,
    ) -> Result<DataFrame, DataFusionError> {
        let session_state = self.state();

        let file_compression_type =
            file_compression_type.unwrap_or(FileCompressionType::UNCOMPRESSED);

        let factory = ExonListingTableFactory::default();

        let table = factory
            .create_from_file_type(
                &session_state,
                file_type,
                file_compression_type,
                table_path.to_string(),
                Vec::new(),
            )
            .await?;

        self.read_table(table)
    }

    async fn register_exon_table(
        &self,
        name: &str,
        table_path: &str,
        file_type: &str,
    ) -> Result<(), DataFusionError> {
        let sql_statement = format!(
            "CREATE EXTERNAL TABLE {} STORED AS {} LOCATION '{}'",
            name, file_type, table_path
        );

        self.sql(&sql_statement).await?;

        Ok(())
    }

    async fn read_inferred_exon_table(
        &self,
        table_path: &str,
    ) -> Result<DataFrame, DataFusionError> {
        let session_state = self.state();

        let (file_type, file_compress_type) =
            crate::datasources::infer_file_type_and_compression(table_path)?;

        let table = ExonListingTableFactory::default()
            .create_from_file_type(
                &session_state,
                file_type,
                file_compress_type,
                table_path.to_string(),
                Vec::new(),
            )
            .await?;

        self.read_table(table)
    }

    async fn register_vcf_file(
        &self,
        table_name: &str,
        table_path: &str,
    ) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        let vcf_table_options = ListingVCFTableOptions::new(FileCompressionType::GZIP, false);

        let table_path = ListingTableUrl::parse(table_path)?;

        let table_schema = vcf_table_options
            .infer_schema(&self.state(), &table_path)
            .await?;

        let config = VCFListingTableConfig::new(table_path).with_options(vcf_table_options);

        let provider = Arc::new(ListingVCFTable::try_new(config, table_schema)?);
        self.register_table(table_name, provider)
    }

    async fn query_vcf_file(
        &self,
        table_path: &str,
        query: &str,
    ) -> Result<DataFrame, DataFusionError> {
        let region: Region = query.parse().map_err(|e| {
            DataFusionError::Execution(format!(
                "Failed to parse query '{}' as region: {}",
                query, e
            ))
        })?;

        self.register_vcf_file("vcf_file", table_path).await?;

        let name = region.name();
        let interval = region.interval();
        let lower_bound = interval.start();
        let upper_bound = interval.end();

        let sql = match (lower_bound, upper_bound) {
            (Some(lb), Some(ub)) => format!(
                "SELECT * FROM vcf_file WHERE chrom = '{}' AND pos BETWEEN {} AND {}",
                name, lb, ub
            ),
            (Some(lb), None) => {
                format!(
                    "SELECT * FROM vcf_file WHERE chrom = '{}' AND pos >= {}",
                    name, lb
                )
            }
            (None, Some(ub)) => {
                format!(
                    "SELECT * FROM vcf_file WHERE chrom = '{}' AND pos <= {}",
                    name, ub
                )
            }
            (None, None) => format!("SELECT * FROM vcf_file WHERE chrom = '{}'", name),
        };

        return self.sql(&sql).await;
    }

    async fn query_bcf_file(
        &self,
        table_path: &str,
        query: &str,
    ) -> Result<DataFrame, DataFusionError> {
        let region = query.parse().map_err(|e| {
            DataFusionError::Execution(format!(
                "Failed to parse query '{}' as region: {}",
                query, e
            ))
        })?;

        let listing_url = ListingTableUrl::parse(table_path)?;

        let state = self.state();

        let options = ListingBCFTableOptions::default().with_region(region);

        let schema = options.infer_schema(&state, &listing_url).await?;
        let config = ListingBCFTableConfig::new(listing_url).with_options(options);

        let table = ListingBCFTable::try_new(config, schema)?;

        let df = self.read_table(Arc::new(table))?;

        Ok(df)
    }
}
