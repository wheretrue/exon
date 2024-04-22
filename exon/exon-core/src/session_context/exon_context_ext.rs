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
    },
    error::{DataFusionError, Result},
    execution::{context::SessionState, object_store::ObjectStoreUrl, runtime_env::RuntimeEnv},
    prelude::{DataFrame, SessionConfig, SessionContext},
};

use crate::{
    datasources::{
        bam::table_provider::{ListingBAMTable, ListingBAMTableOptions},
        bcf::table_provider::{ListingBCFTable, ListingBCFTableOptions},
        bed::table_provider::{ListingBEDTable, ListingBEDTableOptions},
        bigwig,
        exon_listing_table_options::ExonListingConfig,
        genbank::table_provider::{ListingGenbankTable, ListingGenbankTableOptions},
        gff::table_provider::{ListingGFFTable, ListingGFFTableOptions},
        gtf::table_provider::{ListingGTFTable, ListingGTFTableOptions},
        hmmdomtab::table_provider::{ListingHMMDomTabTable, ListingHMMDomTabTableOptions},
        mzml::table_provider::{ListingMzMLTable, ListingMzMLTableOptions},
        sam::table_provider::{ListingSAMTable, ListingSAMTableOptions},
        vcf::ListingVCFTable,
    },
    error::ExonError,
    udfs::{
        register_bigwig_region_filter_udf, sam::cram_region_filter::register_cram_region_filter_udf,
    },
};

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
        bcf::BCFScanFunction,
        bed::BEDScanFunction,
        fasta::{
            table_provider::{ListingFASTATable, ListingFASTATableOptions},
            FastaIndexedScanFunction, FastaScanFunction,
        },
        fastq::{
            table_provider::{ListingFASTQTable, ListingFASTQTableOptions},
            FastqScanFunction,
        },
        gff::{GFFIndexedScanFunction, GFFScanFunction},
        gtf::GTFScanFunction,
        hmmdomtab::HMMDomTabScanFunction,
        sam::SAMScanFunction,
        vcf::{ListingVCFTableOptions, VCFIndexedScanFunction, VCFScanFunction},
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

use super::function_factory::ExonFunctionFactory;

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
            "BIGWIG_ZOOM",
            "BIGWIG_VALUE",
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

        // Register BigWig region filter UDF
        register_bigwig_region_filter_udf(&ctx);

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

    /// Infer the file type and compression, then read the file.
    async fn read_inferred_exon_table(&self, table_path: &str) -> Result<DataFrame, ExonError>;

    /// Register a Exon table from the given path of a certain type and optional compression type.
    async fn register_exon_table(
        &self,
        name: &str,
        table_path: &str,
        file_type: &str,
    ) -> Result<(), DataFusionError>;

    /// Read a BigWig zoom file.
    async fn read_bigwig_zoom(
        &self,
        table_path: &str,
        options: bigwig::zoom::ListingTableOptions,
    ) -> Result<DataFrame, ExonError>;

    /// Read a BigWig file.
    async fn read_bigwig_view(
        &self,
        table_path: &str,
        options: bigwig::value::ListingTableOptions,
    ) -> Result<DataFrame, ExonError>;

    /// Read a FASTA file.
    async fn read_fasta(
        &self,
        table_path: &str,
        options: ListingFASTATableOptions,
    ) -> Result<DataFrame, ExonError>;

    /// Read a BAM file.
    async fn read_bam(
        &self,
        table_path: &str,
        options: ListingBAMTableOptions,
    ) -> Result<DataFrame, ExonError>;

    /// Read a SAM file.
    async fn read_sam(
        &self,
        table_path: &str,
        options: ListingSAMTableOptions,
    ) -> Result<DataFrame, ExonError>;

    /// Read a VCF file.
    async fn read_vcf(
        &self,
        table_path: &str,
        options: ListingVCFTableOptions,
    ) -> Result<DataFrame, ExonError>;

    /// Read a BCF file.
    async fn read_bcf(
        &self,
        table_path: &str,
        options: ListingBCFTableOptions,
    ) -> Result<DataFrame, ExonError>;

    /// Read a GFF file.
    async fn read_gff(
        &self,
        table_path: &str,
        options: ListingGFFTableOptions,
    ) -> Result<DataFrame, ExonError>;

    /// Read a GTF file.
    async fn read_gtf(
        &self,
        table_path: &str,
        options: ListingGTFTableOptions,
    ) -> Result<DataFrame, ExonError>;

    /// Read a BED file.
    async fn read_bed(
        &self,
        table_path: &str,
        options: ListingBEDTableOptions,
    ) -> Result<DataFrame, ExonError>;

    /// Read a FASTQ file.
    async fn read_fastq(
        &self,
        table_path: &str,
        options: ListingFASTQTableOptions,
    ) -> Result<DataFrame, ExonError>;

    /// Read a GENBANK file.
    #[cfg(feature = "genbank")]
    async fn read_genbank(
        &self,
        table_path: &str,
        options: ListingGenbankTableOptions,
    ) -> Result<DataFrame, ExonError>;

    /// Read a HMMER DOMTAB file.
    async fn read_hmm_dom_tab(
        &self,
        table_path: &str,
        options: ListingHMMDomTabTableOptions,
    ) -> Result<DataFrame, ExonError>;

    /// Read a MZML file.
    #[cfg(feature = "mzml")]
    async fn read_mzml(
        &self,
        table_path: &str,
        options: ListingMzMLTableOptions,
    ) -> Result<DataFrame, ExonError>;
}

#[async_trait]
impl ExonSessionExt for SessionContext {
    async fn read_bam(
        &self,
        table_path: &str,
        options: ListingBAMTableOptions,
    ) -> crate::Result<DataFrame> {
        let table_path = ListingTableUrl::parse(table_path)?;

        let table_schema = options.infer_schema(&self.state(), &table_path).await?;

        let config = ExonListingConfig::new_with_options(table_path, options);
        let table = ListingBAMTable::new(config, table_schema);

        let table = self.read_table(Arc::new(table))?;

        Ok(table)
    }

    async fn read_gtf(
        &self,
        table_path: &str,
        options: ListingGTFTableOptions,
    ) -> crate::Result<DataFrame> {
        let table_path = ListingTableUrl::parse(table_path)?;

        let table_schema = options.infer_schema();

        let config = ExonListingConfig::new_with_options(table_path, options);
        let table = ListingGTFTable::new(config, table_schema);

        let table = self.read_table(Arc::new(table))?;

        Ok(table)
    }

    async fn read_genbank(
        &self,
        table_path: &str,
        options: ListingGenbankTableOptions,
    ) -> crate::Result<DataFrame> {
        let table_path = ListingTableUrl::parse(table_path)?;

        let table_schema = options.infer_schema().await?;

        let config = ExonListingConfig::new_with_options(table_path, options);
        let table = ListingGenbankTable::new(config, table_schema);

        let table = self.read_table(Arc::new(table))?;

        Ok(table)
    }

    async fn read_hmm_dom_tab(
        &self,
        table_path: &str,
        options: ListingHMMDomTabTableOptions,
    ) -> crate::Result<DataFrame> {
        let table_path = ListingTableUrl::parse(table_path)?;

        let table_schema = options.infer_schema().await?;

        let config = ExonListingConfig::new_with_options(table_path, options);
        let table = ListingHMMDomTabTable::new(config, table_schema);

        let table = self.read_table(Arc::new(table))?;

        Ok(table)
    }

    async fn read_mzml(
        &self,
        table_path: &str,
        options: ListingMzMLTableOptions,
    ) -> crate::Result<DataFrame> {
        let table_path = ListingTableUrl::parse(table_path)?;

        let table_schema = options.infer_schema().await?;

        let config = ExonListingConfig::new_with_options(table_path, options);
        let table = ListingMzMLTable::new(config, table_schema);

        let table = self.read_table(Arc::new(table))?;

        Ok(table)
    }

    async fn read_bed(
        &self,
        table_path: &str,
        options: ListingBEDTableOptions,
    ) -> crate::Result<DataFrame> {
        let table_path = ListingTableUrl::parse(table_path)?;

        let table_schema = options.infer_schema()?;

        let config = ExonListingConfig::new_with_options(table_path, options);
        let table = ListingBEDTable::new(config, table_schema);

        let table = self.read_table(Arc::new(table))?;

        Ok(table)
    }

    async fn read_bcf(
        &self,
        table_path: &str,
        options: ListingBCFTableOptions,
    ) -> crate::Result<DataFrame> {
        let table_path = ListingTableUrl::parse(table_path)?;

        let table_schema = options.infer_schema(&self.state(), &table_path).await?;

        let config = ExonListingConfig::new_with_options(table_path, options);
        let table = ListingBCFTable::new(config, table_schema);

        let table = self.read_table(Arc::new(table))?;

        Ok(table)
    }

    async fn read_gff(
        &self,
        table_path: &str,
        options: ListingGFFTableOptions,
    ) -> crate::Result<DataFrame> {
        let table_path = ListingTableUrl::parse(table_path)?;

        let table_schema = options.infer_schema().await?;

        let config = ExonListingConfig::new_with_options(table_path, options);
        let table = ListingGFFTable::new(config, table_schema);

        let table = self.read_table(Arc::new(table))?;

        Ok(table)
    }

    async fn read_sam(
        &self,
        table_path: &str,
        options: ListingSAMTableOptions,
    ) -> crate::Result<DataFrame> {
        let table_path = ListingTableUrl::parse(table_path)?;

        let table_schema = options.infer_schema(&self.state(), &table_path).await?;

        let config = ExonListingConfig::new_with_options(table_path, options);
        let table = ListingSAMTable::new(config, table_schema);

        let table = self.read_table(Arc::new(table))?;

        Ok(table)
    }

    async fn read_vcf(
        &self,
        table_path: &str,
        options: ListingVCFTableOptions,
    ) -> crate::Result<DataFrame> {
        let table_path = ListingTableUrl::parse(table_path)?;

        let table_schema = options.infer_schema(&self.state(), &table_path).await?;

        let config = ExonListingConfig::new_with_options(table_path, options);
        let table = ListingVCFTable::new(config, table_schema);

        let table = self.read_table(Arc::new(table))?;

        Ok(table)
    }

    async fn read_exon_table(
        &self,
        table_path: &str,
        file_type: ExonFileType,
        file_compression_type: Option<FileCompressionType>,
    ) -> crate::Result<DataFrame> {
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

    async fn read_fasta(
        &self,
        table_path: &str,
        options: ListingFASTATableOptions,
    ) -> Result<DataFrame, ExonError> {
        let table_path = ListingTableUrl::parse(table_path)?;

        let table_schema = options.infer_schema(&self.state()).await?;

        let config = ExonListingConfig::new_with_options(table_path, options);
        let table = ListingFASTATable::try_new(config, table_schema)?;

        let table = self.read_table(Arc::new(table))?;

        Ok(table)
    }

    /// Read a BigWig zoom file.
    async fn read_bigwig_zoom(
        &self,
        table_path: &str,
        options: bigwig::zoom::ListingTableOptions,
    ) -> Result<DataFrame, ExonError> {
        let table_path = ListingTableUrl::parse(table_path)?;

        let table_schema = options.infer_schema()?;

        let config = bigwig::zoom::ListingTableConfig::new(table_path, options);
        let table = bigwig::zoom::ListingTable::try_new(config, table_schema)?;

        let table = self.read_table(Arc::new(table))?;

        Ok(table)
    }

    /// Read a BigWig view file.
    async fn read_bigwig_view(
        &self,
        table_path: &str,
        options: bigwig::value::ListingTableOptions,
    ) -> Result<DataFrame, ExonError> {
        let table_path = ListingTableUrl::parse(table_path)?;

        let table_schema = options.infer_schema()?;

        let config = ExonListingConfig::new_with_options(table_path, options);
        let table = bigwig::value::ListingTable::new(config, table_schema);

        let table = self.read_table(Arc::new(table))?;

        Ok(table)
    }

    /// Read a FASTQ file.
    async fn read_fastq(
        &self,
        table_path: &str,
        options: ListingFASTQTableOptions,
    ) -> Result<DataFrame, ExonError> {
        let table_path = ListingTableUrl::parse(table_path)?;

        let table_schema = options.infer_schema();

        let config = ExonListingConfig::new_with_options(table_path, options);
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
}

#[cfg(test)]
mod tests {
    use datafusion::{
        datasource::file_format::file_compression_type::FileCompressionType,
        execution::context::SessionContext,
    };

    use crate::{
        datasources::{
            bcf::table_provider::ListingBCFTableOptions, bigwig,
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
                ListingFASTQTableOptions::default().with_some_file_extension(Some("fq")),
            )
            .await?;

        assert_eq!(df.count().await?, 2);

        let df = ctx
            .read_fastq(
                fastq_path.parent().ok_or("No Parent")?.to_str().unwrap(),
                ListingFASTQTableOptions::default().with_some_file_extension(Some("fq")),
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
            .with_some_file_extension(Some("fq"));

        let df = ctx
            .read_fastq(fastq_path.to_str().unwrap(), options)
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
                ListingFASTATableOptions::default().with_some_file_extension(Some("fa")),
            )
            .await?;

        assert_eq!(df.count().await?, 2);

        Ok(())
    }

    #[tokio::test]
    async fn test_bigwig_zoom_file() -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new_exon();

        let bigwig_path = exon_test::test_path("bigwig", "test.bw");

        let df = ctx
            .read_bigwig_zoom(
                bigwig_path.to_str().unwrap(),
                bigwig::zoom::ListingTableOptions::new(400),
            )
            .await?;

        assert_eq!(df.count().await?, 2);

        Ok(())
    }

    #[tokio::test]
    async fn test_read_bigwig_view_file() -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new_exon();

        let bigwig_path = exon_test::test_path("bigwig", "test.bw");

        let df = ctx
            .read_bigwig_view(
                bigwig_path.to_str().unwrap(),
                bigwig::value::ListingTableOptions::default(),
            )
            .await?;
        assert_eq!(df.count().await?, 6);

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
                ListingFASTATableOptions::default().with_some_file_extension(Some("fa")),
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
                ListingFASTATableOptions::default().with_some_file_extension(Some("fa")),
            )
            .await?;

        assert_eq!(df.count().await?, 4);

        let df = ctx
            .read_fasta(
                fasta_path.to_str().unwrap(),
                ListingFASTATableOptions::new(FileCompressionType::GZIP)
                    .with_some_file_extension(Some("fa")),
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
            .with_some_file_extension(Some("fa"));

        let df = ctx
            .read_fasta(fasta_path.to_str().unwrap(), options)
            .await?;

        assert_eq!(df.count().await?, 2);

        Ok(())
    }

    #[tokio::test]
    async fn test_bcf_file() -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new_exon();

        let bcf_path = exon_test::test_path("bcf", "index.bcf");

        let df = ctx
            .read_bcf(
                bcf_path.to_str().unwrap(),
                ListingBCFTableOptions::default(),
            )
            .await?;

        assert_eq!(df.count().await?, 621);

        Ok(())
    }

    #[tokio::test]
    async fn test_bcf_file_with_region() -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new_exon();

        let bcf_path = exon_test::test_path("bcf", "index.bcf");

        let region = "1".parse()?;

        let df = ctx
            .read_bcf(
                bcf_path.to_str().unwrap(),
                ListingBCFTableOptions::default().with_regions(vec![region]),
            )
            .await?;

        assert_eq!(df.count().await?, 191);

        Ok(())
    }
}
