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

use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    datasource::{
        file_format::file_compression_type::FileCompressionType, listing::ListingTableUrl,
        TableProvider,
    },
    error::{DataFusionError, Result},
    execution::{context::SessionState, object_store::ObjectStoreUrl, runtime_env::RuntimeEnv},
    prelude::{DataFrame, SessionConfig, SessionContext},
};

use crate::{
    error::ExonError,
    udfs::{
        sam::cram_region_filter::register_cram_region_filter_udf,
        sequence::motif::ExonFunctionFactory,
    },
};

use noodles::core::Region;
use object_store::local::LocalFileSystem;

#[cfg(feature = "mzml")]
use crate::datasources::mzml::MzMLScanFunction;

#[cfg(feature = "fcs")]
use crate::datasources::fcs::FCSScanFunction;

#[cfg(feature = "genbank")]
use crate::datasources::genbank::GenbankScanFunction;

use crate::{
    datasources::{
        bam::{BAMIndexedScanFunction, BAMScanFunction},
        bcf::{
            table_provider::{ListingBCFTable, ListingBCFTableConfig, ListingBCFTableOptions},
            BCFScanFunction,
        },
        bed::BEDScanFunction,
        fasta::{
            table_provider::{
                ListingFASTATable, ListingFASTATableConfig, ListingFASTATableOptions,
            },
            FastaIndexedScanFunction, FastaScanFunction,
        },
        fastq::{
            table_provider::{
                ListingFASTQTable, ListingFASTQTableConfig, ListingFASTQTableOptions,
            },
            FastqScanFunction,
        },
        gff::{GFFIndexedScanFunction, GFFScanFunction},
        gtf::GTFScanFunction,
        hmmdomtab::HMMDomTabScanFunction,
        sam::SAMScanFunction,
        vcf::{
            ListingVCFTable, ListingVCFTableConfig, ListingVCFTableOptions, VCFIndexedScanFunction,
            VCFScanFunction,
        },
        ExonFileType, ExonListingTableFactory,
    },
    new_exon_config,
    physical_plan::planner::ExonQueryPlanner,
    udfs::{
        gff::gff_region_filter::register_gff_region_filter_udf,
        sam::bam_region_filter::register_bam_region_filter_udf,
        vcf::vcf_region_filter::register_vcf_region_filter_udf,
    },
};

/// Extension trait for [`SessionContext`] that adds Exon-specific functionality.
#[async_trait]
pub trait ExonSessionExt {
    /// Reads a Exon table from the given path of a certain type and optional compression type.
    async fn read_exon_table(
        &self,
        table_path: &str,
        file_type: ExonFileType,
        file_compression_type: Option<FileCompressionType>,
    ) -> Result<DataFrame, ExonError>;

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
        let mut state = SessionState::new_with_config_rt(config, runtime)
            .with_function_factory(Arc::new(ExonFunctionFactory::default()));

        let sources = vec![
            "BAM",
            "BCF",
            "BED",
            "CRAM",
            "FAA",
            "FASTA",
            "FASTQ",
            "FNA",
            "FQ",
            "GENBANK",
            "GFF",
            "GTF",
            "HMMDOMTAB",
            "INDEXED_GFF",
            "INDEXED_BAM",
            "INDEXED_VCF",
            "SAM",
            "VCF",
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

        // state = state.with_query_planner(Arc::new(ExonQueryPlanner::default()));
        let ctx = SessionContext::new_with_state(
            state.with_query_planner(Arc::new(ExonQueryPlanner::default())),
        );

        // Register the mass spec UDFs
        #[cfg(feature = "mzml")]
        crate::udfs::massspec::register_udfs(&ctx);

        crate::udfs::sequence::register_udfs(&ctx);
        crate::udfs::sam::samflags::register_udfs(&ctx);
        crate::udfs::vcf::register_vcf_udfs(&ctx);

        // Register BAM region filter UDF
        register_bam_region_filter_udf(&ctx);

        // Register CRAM region filter UDF
        register_cram_region_filter_udf(&ctx);

        // Register VCF region filter UDF
        register_vcf_region_filter_udf(&ctx);

        // Register GFF region filter UDF
        register_gff_region_filter_udf(&ctx);

        // Register UDTFs
        ctx.register_udtf("fasta_scan", Arc::new(FastaScanFunction::new(ctx.clone())));
        ctx.register_udtf(
            "fasta_indexed_scan",
            Arc::new(FastaIndexedScanFunction::new(ctx.clone())),
        );
        ctx.register_udtf("fastq_scan", Arc::new(FastqScanFunction::new(ctx.clone())));
        ctx.register_udtf("gff_scan", Arc::new(GFFScanFunction::new(ctx.clone())));
        ctx.register_udtf(
            "gff_indexed_scan",
            Arc::new(GFFIndexedScanFunction::new(ctx.clone())),
        );
        ctx.register_udtf("gtf_scan", Arc::new(GTFScanFunction::new(ctx.clone())));
        ctx.register_udtf("bed_scan", Arc::new(BEDScanFunction::new(ctx.clone())));
        ctx.register_udtf(
            "hmm_dom_tab_scan",
            Arc::new(HMMDomTabScanFunction::default()),
        );

        #[cfg(feature = "genbank")]
        ctx.register_udtf(
            "genbank_scan",
            Arc::new(GenbankScanFunction::new(ctx.clone())),
        );

        #[cfg(feature = "fcs")]
        ctx.register_udtf("fcs_scan", Arc::new(FCSScanFunction::new(ctx.clone())));

        #[cfg(feature = "mzml")]
        ctx.register_udtf("mzml_scan", Arc::new(MzMLScanFunction::new(ctx.clone())));

        ctx.register_udtf("bam_scan", Arc::new(BAMScanFunction::new(ctx.clone())));
        ctx.register_udtf(
            "bam_indexed_scan",
            Arc::new(BAMIndexedScanFunction::new(ctx.clone())),
        );

        ctx.register_udtf("sam_scan", Arc::new(SAMScanFunction::new(ctx.clone())));
        ctx.register_udtf("vcf_scan", Arc::new(VCFScanFunction::new(ctx.clone())));
        ctx.register_udtf(
            "vcf_indexed_scan",
            Arc::new(VCFIndexedScanFunction::new(ctx.clone())),
        );
        ctx.register_udtf("bcf_scan", Arc::new(BCFScanFunction::new(ctx.clone())));

        // Register the local file system by default
        ctx.runtime_env().register_object_store(
            ObjectStoreUrl::local_filesystem().as_ref(),
            Arc::new(LocalFileSystem::new()),
        );

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
        fasta_listing_table_options: Option<ListingFASTATableOptions>,
    ) -> Result<DataFrame, ExonError>;

    /// Read a BAM file.
    async fn read_bam(
        &self,
        table_path: &str,
        file_compression_type: Option<FileCompressionType>,
    ) -> Result<DataFrame, ExonError> {
        return self
            .read_exon_table(table_path, ExonFileType::BAM, file_compression_type)
            .await;
    }

    /// Infer the file type and compression, then read the file.
    async fn read_inferred_exon_table(&self, table_path: &str) -> Result<DataFrame, ExonError>;

    /// Read a SAM file.
    async fn read_sam(
        &self,
        table_path: &str,
        file_compression_type: Option<FileCompressionType>,
    ) -> Result<DataFrame, ExonError> {
        return self
            .read_exon_table(table_path, ExonFileType::SAM, file_compression_type)
            .await;
    }

    /// Read a VCF file.
    async fn read_vcf(
        &self,
        table_path: &str,
        file_compression_type: Option<FileCompressionType>,
    ) -> Result<DataFrame, ExonError> {
        return self
            .read_exon_table(table_path, ExonFileType::VCF, file_compression_type)
            .await;
    }

    /// Read a BCF file.
    async fn read_bcf(
        &self,
        table_path: &str,
        file_compression_type: Option<FileCompressionType>,
    ) -> Result<DataFrame, ExonError> {
        return self
            .read_exon_table(table_path, ExonFileType::BCF, file_compression_type)
            .await;
    }

    /// Read a GFF file.
    async fn read_gff(
        &self,
        table_path: &str,
        file_compression_type: Option<FileCompressionType>,
    ) -> Result<DataFrame, ExonError> {
        return self
            .read_exon_table(table_path, ExonFileType::GFF, file_compression_type)
            .await;
    }

    /// Read a GTF file.
    async fn read_gtf(
        &self,
        table_path: &str,
        file_compression_type: Option<FileCompressionType>,
    ) -> Result<DataFrame, ExonError> {
        self.read_exon_table(table_path, ExonFileType::GTF, file_compression_type)
            .await
    }

    /// Read a BED file.
    async fn read_bed(
        &self,
        table_path: &str,
        file_compression_type: Option<FileCompressionType>,
    ) -> Result<DataFrame, ExonError> {
        return self
            .read_exon_table(table_path, ExonFileType::BED, file_compression_type)
            .await;
    }

    /// Read a FASTQ file.
    async fn read_fastq(
        &self,
        table_path: &str,
        fastq_listing_table_options: Option<ListingFASTQTableOptions>,
    ) -> Result<DataFrame, ExonError>;

    /// Read a GENBANK file.
    #[cfg(feature = "genbank")]
    async fn read_genbank(
        &self,
        table_path: &str,
        file_compression_type: Option<FileCompressionType>,
    ) -> Result<DataFrame, ExonError> {
        return self
            .read_exon_table(table_path, ExonFileType::GENBANK, file_compression_type)
            .await;
    }

    /// Read a HMMER DOMTAB file.
    async fn read_hmm_dom_tab(
        &self,
        table_path: &str,
        file_compression_type: Option<FileCompressionType>,
    ) -> Result<DataFrame, ExonError> {
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
    ) -> Result<DataFrame, ExonError> {
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
    async fn query_vcf_file(&self, table_path: &str, query: &str) -> Result<DataFrame, ExonError>;
}

#[async_trait]
impl ExonSessionExt for SessionContext {
    async fn read_exon_table(
        &self,
        table_path: &str,
        file_type: ExonFileType,
        file_compression_type: Option<FileCompressionType>,
    ) -> Result<DataFrame, ExonError> {
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
                &HashMap::new(),
            )
            .await?;

        let table = self.read_table(table)?;

        Ok(table)
    }

    /// Read a FASTA file.
    async fn read_fasta(
        &self,
        table_path: &str,
        fasta_listing_table_options: Option<ListingFASTATableOptions>,
    ) -> Result<DataFrame, ExonError> {
        let table_path = ListingTableUrl::parse(table_path)?;
        let options = fasta_listing_table_options.unwrap_or_default();

        let state = self.state();
        let table_schema = options.infer_schema(&state).await?;

        let config = ListingFASTATableConfig::new(table_path, options);
        let table = ListingFASTATable::try_new(config, table_schema)?;

        let table = self.read_table(Arc::new(table))?;

        Ok(table)
    }

    /// Read a FASTQ file.
    async fn read_fastq(
        &self,
        table_path: &str,
        fastq_listing_table_options: Option<ListingFASTQTableOptions>,
    ) -> Result<DataFrame, ExonError> {
        let table_path = ListingTableUrl::parse(table_path)?;
        let options = fastq_listing_table_options.unwrap_or_default();

        let table_schema = options.infer_schema();

        let config = ListingFASTQTableConfig::new(table_path, options);
        let table = ListingFASTQTable::try_new(config, table_schema)?;

        let table = self.read_table(Arc::new(table))?;

        Ok(table)
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

    async fn read_inferred_exon_table(&self, table_path: &str) -> Result<DataFrame, ExonError> {
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
                &HashMap::new(),
            )
            .await?;

        let table = self.read_table(table)?;

        Ok(table)
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

        let config = ListingVCFTableConfig::new(table_path, vcf_table_options);

        let provider = Arc::new(ListingVCFTable::try_new(config, table_schema)?);
        self.register_table(table_name, provider)
    }

    async fn query_vcf_file(&self, table_path: &str, query: &str) -> Result<DataFrame, ExonError> {
        let region: Region = query.parse().map_err(|e| {
            DataFusionError::Execution(format!(
                "Failed to parse query '{}' as region: {}",
                query, e
            ))
        })?;

        self.register_vcf_file("vcf_file", table_path).await?;

        let name = std::str::from_utf8(region.name())?;
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

        let resp = self.sql(&sql).await?;

        Ok(resp)
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

#[cfg(test)]
mod tests {
    use datafusion::{
        datasource::file_format::file_compression_type::FileCompressionType,
        execution::context::SessionContext,
    };

    use crate::{
        datasources::{
            fasta::table_provider::ListingFASTATableOptions,
            fastq::table_provider::ListingFASTQTableOptions,
        },
        session_context::ExonSessionExt,
        ExonRuntimeEnvExt,
    };

    #[tokio::test]
    async fn test_read_fastq() -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new_exon();

        let fastq_path = exon_test::test_path("fastq", "test.fq");

        let df = ctx
            .read_fastq(
                fastq_path.to_str().unwrap(),
                Some(ListingFASTQTableOptions::default().with_file_extension("fq".to_string())),
            )
            .await?;

        assert_eq!(df.count().await?, 2);

        let df = ctx
            .read_fastq(
                fastq_path.parent().ok_or("No Parent")?.to_str().unwrap(),
                Some(ListingFASTQTableOptions::default().with_file_extension("fq".to_string())),
            )
            .await?;

        assert_eq!(df.count().await?, 2);

        Ok(())
    }

    #[tokio::test]
    async fn test_read_fastq_gzip() -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new_exon();

        let fastq_path = exon_test::test_path("fastq", "test.fq.gz");

        let options = ListingFASTQTableOptions::new(FileCompressionType::GZIP)
            .with_file_extension("fq".to_string());

        let df = ctx
            .read_fastq(fastq_path.to_str().unwrap(), Some(options))
            .await?;

        assert_eq!(df.count().await?, 2);

        Ok(())
    }

    #[tokio::test]
    async fn test_read_fasta() -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new_exon();

        let fasta_path = exon_test::test_path("fasta", "test.fa");

        let df = ctx
            .read_fasta(
                fasta_path.to_str().unwrap(),
                Some(ListingFASTATableOptions::default().with_file_extension("fa".to_string())),
            )
            .await?;

        assert_eq!(df.count().await?, 2);

        Ok(())
    }

    #[tokio::test]
    async fn test_read_fasta_s3() -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new_exon();

        ctx.runtime_env()
            .register_s3_object_store(&url::Url::parse("s3://test-bucket")?)
            .await?;

        let df = ctx
            .read_fasta(
                "s3://test-bucket/test.fa",
                Some(ListingFASTATableOptions::default().with_file_extension("fa".to_string())),
            )
            .await?;

        assert_eq!(df.count().await?, 2);

        Ok(())
    }

    #[tokio::test]
    async fn test_read_fasta_dir() -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new_exon();

        let fasta_path = exon_test::test_path("fa", "test.fa");
        let fasta_path = fasta_path.parent().ok_or("No parent")?;

        let df = ctx
            .read_fasta(
                fasta_path.to_str().unwrap(),
                Some(ListingFASTATableOptions::default().with_file_extension("fa".to_string())),
            )
            .await?;

        assert_eq!(df.count().await?, 4);

        let df = ctx
            .read_fasta(
                fasta_path.to_str().unwrap(),
                Some(
                    ListingFASTATableOptions::new(FileCompressionType::GZIP)
                        .with_file_extension("fa".to_string()),
                ),
            )
            .await?;

        assert_eq!(df.count().await?, 4);
        Ok(())
    }

    #[tokio::test]
    async fn test_read_fasta_gzip() -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new_exon();

        let fasta_path = exon_test::test_path("fasta", "test.fa.gz");

        let options = ListingFASTATableOptions::new(FileCompressionType::GZIP)
            .with_file_extension("fa".to_string());

        let df = ctx
            .read_fasta(fasta_path.to_str().unwrap(), Some(options))
            .await?;

        assert_eq!(df.count().await?, 2);

        Ok(())
    }
}
