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

use std::{collections::HashMap, sync::Arc, vec};

use datafusion::{
    datasource::{
        file_format::file_compression_type::FileCompressionType, listing::ListingTableUrl,
    },
    error::{DataFusionError, Result},
    execution::{context::SessionState, object_store::ObjectStoreUrl, runtime_env::RuntimeEnv},
    logical_expr::LogicalPlan,
    prelude::{DataFrame, SessionConfig, SessionContext},
};

use crate::{
    datasources::{
        bam::table_provider::{ListingBAMTable, ListingBAMTableOptions},
        bcf::table_provider::{ListingBCFTable, ListingBCFTableOptions},
        bed::{
            table_provider::{ListingBEDTable, ListingBEDTableOptions},
            BEDOptions,
        },
        bigwig,
        cram::table_provider::{ListingCRAMTable, ListingCRAMTableConfig, ListingCRAMTableOptions},
        exon_listing_table_options::ExonListingConfig,
        fasta::FASTAOptions,
        genbank::table_provider::{ListingGenbankTable, ListingGenbankTableOptions},
        gff::table_provider::{ListingGFFTable, ListingGFFTableOptions},
        gtf::table_provider::{ListingGTFTable, ListingGTFTableOptions},
        hmmdomtab::table_provider::{ListingHMMDomTabTable, ListingHMMDomTabTableOptions},
        mzml::table_provider::{ListingMzMLTable, ListingMzMLTableOptions},
        sam::table_provider::{ListingSAMTable, ListingSAMTableOptions},
        vcf::ListingVCFTable,
    },
    error::ExonError,
    logical_plan::{DfExtensionNode, ExonDataSinkLogicalPlanNode, ExonLogicalPlan},
    sql::{ExonParser, ExonStatement},
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

/// Exon session context.
pub struct ExonSession {
    /// The Exon session context.
    pub session: SessionContext,
}

impl ExonSession {
    /// Create a new Exon session context.
    pub fn new(session: SessionContext) -> Self {
        Self { session }
    }

    /// Create a new Exon based [`SessionContext`].
    pub fn new_exon() -> Self {
        let exon_config = new_exon_config();
        Self::with_config_exon(exon_config)
    }

    /// Create a new Exon based [`SessionContext`] with the given config.
    pub fn with_config_exon(config: SessionConfig) -> Self {
        let runtime = Arc::new(RuntimeEnv::default());
        Self::with_config_rt_exon(config, runtime)
    }

    /// Create a new Exon based [`SessionContext`] with the given config and runtime.
    pub fn with_config_rt_exon(config: SessionConfig, runtime: Arc<RuntimeEnv>) -> Self {
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

        let ctx = SessionContext::new_with_state(
            state.with_query_planner(Arc::new(ExonQueryPlanner::default())),
        );

        ctx.register_table_options_extension(FASTAOptions::default());
        ctx.register_table_options_extension(BEDOptions::default());

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

        Self::new(ctx)
    }

    fn exon_sql_to_statement(&self, sql: &str) -> crate::Result<ExonStatement> {
        let mut exon_parser = ExonParser::new(sql)?;

        exon_parser.parse_statement()
    }

    /// Convert Exon SQL to a logical plan.
    async fn exon_statement_to_logical_plan(
        &self,
        stmt: ExonStatement,
    ) -> crate::Result<ExonLogicalPlan> {
        match stmt {
            ExonStatement::DFStatement(stmt) => {
                let plan = self.session.state().statement_to_plan(*stmt).await?;

                Ok(ExonLogicalPlan::DataFusion(plan))
            }
            ExonStatement::ExonCopyTo(stmt) => {
                let node = ExonDataSinkLogicalPlanNode::from(stmt);
                let extension = node.into_extension();
                let plan = LogicalPlan::Extension(extension);

                Ok(ExonLogicalPlan::Exon(plan))
            }
        }
    }

    /// Execute an Exon SQL statement.
    pub async fn sql(&self, sql: &str) -> crate::Result<DataFrame> {
        let statement = self.exon_sql_to_statement(sql)?;
        let plan = self.exon_statement_to_logical_plan(statement).await?;

        match plan {
            ExonLogicalPlan::DataFusion(plan) => {
                let df = self.session.execute_logical_plan(plan).await?;
                Ok(df)
            }
            ExonLogicalPlan::Exon(plan) => {
                let df = DataFrame::new(self.session.state(), plan);
                Ok(df)
            }
        }
    }

    /// Read a BAM file.
    pub async fn read_bam(
        &self,
        table_path: &str,
        options: ListingBAMTableOptions,
    ) -> crate::Result<DataFrame> {
        let table_path = ListingTableUrl::parse(table_path)?;

        let table_schema = options
            .infer_schema(&self.session.state(), &table_path)
            .await?;

        let config = ExonListingConfig::new_with_options(table_path, options);
        let table = ListingBAMTable::new(config, table_schema);

        let table = self.session.read_table(Arc::new(table))?;

        Ok(table)
    }

    /// Read a BCF file.
    #[cfg(feature = "fcs")]
    pub async fn read_fcs(
        &self,
        table_path: &str,
        options: crate::datasources::fcs::table_provider::ListingFCSTableOptions,
    ) -> Result<DataFrame, ExonError> {
        use crate::datasources::fcs::table_provider::ListingFCSTableConfig;

        let table_path = ListingTableUrl::parse(table_path)?;

        let table_schema = options
            .infer_schema(&self.session.state(), &table_path)
            .await?;

        let config = ListingFCSTableConfig::new(table_path, options);
        let table = crate::datasources::fcs::table_provider::ListingFCSTable::try_new(
            config,
            table_schema,
        )?;

        let table = self.session.read_table(Arc::new(table))?;

        Ok(table)
    }

    /// Read a CRAM file.
    pub async fn read_cram(
        &self,
        table_path: &str,
        options: ListingCRAMTableOptions,
    ) -> crate::Result<DataFrame> {
        let table_path = ListingTableUrl::parse(table_path)?;

        let table_schema = options
            .infer_schema(&self.session.state(), &table_path)
            .await?;

        // TODO: refactor this to use the new config setup
        let config = ListingCRAMTableConfig::new(table_path, options);

        let table = ListingCRAMTable::try_new(config, table_schema)?;
        let table = self.session.read_table(Arc::new(table))?;

        Ok(table)
    }

    /// Read a GTF file.
    pub async fn read_gtf(
        &self,
        table_path: &str,
        options: ListingGTFTableOptions,
    ) -> crate::Result<DataFrame> {
        let table_path = ListingTableUrl::parse(table_path)?;

        let table_schema = options.infer_schema();

        let config = ExonListingConfig::new_with_options(table_path, options);
        let table = ListingGTFTable::new(config, table_schema);

        let table = self.session.read_table(Arc::new(table))?;

        Ok(table)
    }

    /// Read a Genbank file.
    pub async fn read_genbank(
        &self,
        table_path: &str,
        options: ListingGenbankTableOptions,
    ) -> crate::Result<DataFrame> {
        let table_path = ListingTableUrl::parse(table_path)?;

        let table_schema = options.infer_schema().await?;

        let config = ExonListingConfig::new_with_options(table_path, options);
        let table = ListingGenbankTable::new(config, table_schema);

        let table = self.session.read_table(Arc::new(table))?;

        Ok(table)
    }

    /// Read a HMM Dom Tab file.
    pub async fn read_hmm_dom_tab(
        &self,
        table_path: &str,
        options: ListingHMMDomTabTableOptions,
    ) -> crate::Result<DataFrame> {
        let table_path = ListingTableUrl::parse(table_path)?;

        let table_schema = options.infer_schema().await?;

        let config = ExonListingConfig::new_with_options(table_path, options);
        let table = ListingHMMDomTabTable::new(config, table_schema);

        let table = self.session.read_table(Arc::new(table))?;

        Ok(table)
    }

    /// Read a MzML file.
    pub async fn read_mzml(
        &self,
        table_path: &str,
        options: ListingMzMLTableOptions,
    ) -> crate::Result<DataFrame> {
        let table_path = ListingTableUrl::parse(table_path)?;

        let table_schema = options.infer_schema().await?;

        let config = ExonListingConfig::new_with_options(table_path, options);
        let table = ListingMzMLTable::new(config, table_schema);

        let table = self.session.read_table(Arc::new(table))?;

        Ok(table)
    }

    /// Read a BED file.
    pub async fn read_bed(
        &self,
        table_path: &str,
        options: ListingBEDTableOptions,
    ) -> crate::Result<DataFrame> {
        let table_path = ListingTableUrl::parse(table_path)?;

        let table_schema = options.infer_schema()?;

        let config = ExonListingConfig::new_with_options(table_path, options);
        let table = ListingBEDTable::new(config, table_schema);

        let table = self.session.read_table(Arc::new(table))?;

        Ok(table)
    }

    /// Read a BCF file.
    pub async fn read_bcf(
        &self,
        table_path: &str,
        options: ListingBCFTableOptions,
    ) -> crate::Result<DataFrame> {
        let table_path = ListingTableUrl::parse(table_path)?;

        let table_schema = options
            .infer_schema(&self.session.state(), &table_path)
            .await?;

        let config = ExonListingConfig::new_with_options(table_path, options);
        let table = ListingBCFTable::new(config, table_schema);

        let table = self.session.read_table(Arc::new(table))?;

        Ok(table)
    }

    /// Read a GFF file.
    pub async fn read_gff(
        &self,
        table_path: &str,
        options: ListingGFFTableOptions,
    ) -> crate::Result<DataFrame> {
        let table_path = ListingTableUrl::parse(table_path)?;

        let table_schema = options.infer_schema().await?;

        let config = ExonListingConfig::new_with_options(table_path, options);
        let table = ListingGFFTable::new(config, table_schema);

        let table = self.session.read_table(Arc::new(table))?;

        Ok(table)
    }

    /// Read a SAM file.
    pub async fn read_sam(
        &self,
        table_path: &str,
        options: ListingSAMTableOptions,
    ) -> crate::Result<DataFrame> {
        let table_path = ListingTableUrl::parse(table_path)?;

        let table_schema = options
            .infer_schema(&self.session.state(), &table_path)
            .await?;

        let config = ExonListingConfig::new_with_options(table_path, options);
        let table = ListingSAMTable::new(config, table_schema);

        let table = self.session.read_table(Arc::new(table))?;

        Ok(table)
    }

    /// Read a VCF file.
    pub async fn read_vcf(
        &self,
        table_path: &str,
        options: ListingVCFTableOptions,
    ) -> crate::Result<DataFrame> {
        let table_path = ListingTableUrl::parse(table_path)?;

        let table_schema = options
            .infer_schema(&self.session.state(), &table_path)
            .await?;

        let config = ExonListingConfig::new_with_options(table_path, options);
        let table = ListingVCFTable::new(config, table_schema);

        let table = self.session.read_table(Arc::new(table))?;

        Ok(table)
    }

    /// Read an Exon table.
    pub async fn read_exon_table(
        &self,
        table_path: &str,
        file_type: ExonFileType,
        file_compression_type: Option<FileCompressionType>,
    ) -> crate::Result<DataFrame> {
        let session_state = self.session.state();

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

        let table = self.session.read_table(table)?;

        Ok(table)
    }

    /// Read a FASTA file.
    pub async fn read_fasta(
        &self,
        table_path: &str,
        options: ListingFASTATableOptions,
    ) -> Result<DataFrame, ExonError> {
        let table_path = ListingTableUrl::parse(table_path)?;

        let table_schema = options.infer_schema(&self.session.state()).await?;

        let config = ExonListingConfig::new_with_options(table_path, options);
        let table = ListingFASTATable::try_new(config, table_schema)?;

        let table = self.session.read_table(Arc::new(table))?;

        Ok(table)
    }

    /// Read a BigWig zoom file.
    pub async fn read_bigwig_zoom(
        &self,
        table_path: &str,
        options: bigwig::zoom::ListingTableOptions,
    ) -> Result<DataFrame, ExonError> {
        let table_path = ListingTableUrl::parse(table_path)?;

        let table_schema = options.infer_schema()?;

        let config = bigwig::zoom::ListingTableConfig::new(table_path, options);
        let table = bigwig::zoom::ListingTable::try_new(config, table_schema)?;

        let table = self.session.read_table(Arc::new(table))?;

        Ok(table)
    }

    /// Read a BigWig view file.
    pub async fn read_bigwig_view(
        &self,
        table_path: &str,
        options: bigwig::value::ListingTableOptions,
    ) -> Result<DataFrame, ExonError> {
        let table_path = ListingTableUrl::parse(table_path)?;

        let table_schema = options.infer_schema()?;

        let config = ExonListingConfig::new_with_options(table_path, options);
        let table = bigwig::value::ListingTable::new(config, table_schema);

        let table = self.session.read_table(Arc::new(table))?;

        Ok(table)
    }

    /// Read a FASTQ file.
    pub async fn read_fastq(
        &self,
        table_path: &str,
        options: ListingFASTQTableOptions,
    ) -> Result<DataFrame, ExonError> {
        let table_path = ListingTableUrl::parse(table_path)?;

        let table_schema = options.infer_schema();

        let config = ExonListingConfig::new_with_options(table_path, options);
        let table = ListingFASTQTable::try_new(config, table_schema)?;

        let table = self.session.read_table(Arc::new(table))?;

        Ok(table)
    }

    /// Register an Exon table.
    pub async fn register_exon_table(
        &self,
        name: &str,
        table_path: &str,
        file_type: &str,
    ) -> Result<(), DataFusionError> {
        let sql_statement = format!(
            "CREATE EXTERNAL TABLE {} STORED AS {} LOCATION '{}'",
            name, file_type, table_path
        );

        self.session.sql(&sql_statement).await?;

        Ok(())
    }

    /// Read an inferred Exon table.
    pub async fn read_inferred_exon_table(&self, table_path: &str) -> Result<DataFrame, ExonError> {
        let session_state = self.session.state();

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

        let table = self.session.read_table(table)?;

        Ok(table)
    }
}

#[cfg(test)]
mod tests {
    use datafusion::datasource::file_format::file_compression_type::FileCompressionType;

    use crate::{
        datasources::{
            bcf::table_provider::ListingBCFTableOptions, bigwig,
            cram::table_provider::ListingCRAMTableOptions,
            fasta::table_provider::ListingFASTATableOptions,
            fastq::table_provider::ListingFASTQTableOptions,
        },
        session_context::exon_context_ext::ExonSession,
        ExonRuntimeEnvExt,
    };

    #[tokio::test]
    async fn test_read_fastq() -> Result<(), Box<dyn std::error::Error>> {
        let ctx = ExonSession::new_exon();

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
        let ctx = ExonSession::new_exon();

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
        let ctx = ExonSession::new_exon();

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
    async fn test_fasta_round_trip() -> Result<(), Box<dyn std::error::Error>> {
        let ctx = ExonSession::new_exon();

        let fasta_path = exon_test::test_path("fasta", "test.fasta");

        let sql = format!(
            "CREATE EXTERNAL TABLE test_fasta STORED AS FASTA LOCATION '{}'",
            fasta_path.to_str().unwrap()
        );
        ctx.sql(&sql).await?.collect().await.unwrap();

        let temp_dir = std::env::temp_dir();
        let temp_path = temp_dir.join("test.fasta");

        let sql = format!(
            "COPY (SELECT * FROM test_fasta) TO '{}' STORED AS FASTA",
            temp_path.display()
        );
        ctx.sql(&sql).await?.collect().await?;

        let df = ctx
            .read_fasta(
                temp_path.to_str().unwrap(),
                ListingFASTATableOptions::default(),
            )
            .await?;

        assert_eq!(df.count().await?, 2);

        // delete the temp file
        std::fs::remove_file(temp_path)?;

        let temp_path = temp_dir.join("test.fasta.gz");
        let sql = format!(
            "COPY test_fasta TO '{}' STORED AS FASTA OPTIONS(compression 'gzip')",
            temp_path.display()
        );

        ctx.sql(&sql).await?.collect().await?;

        let df = ctx
            .read_fasta(
                temp_path.to_str().unwrap(),
                ListingFASTATableOptions::new(FileCompressionType::GZIP),
            )
            .await?;

        assert_eq!(df.count().await?, 2);

        Ok(())
    }

    #[tokio::test]
    async fn test_bigwig_zoom_file() -> Result<(), Box<dyn std::error::Error>> {
        let ctx = ExonSession::new_exon();

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
        let ctx = ExonSession::new_exon();

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
        let ctx = ExonSession::new_exon();

        ctx.session
            .runtime_env()
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
        let ctx = ExonSession::new_exon();

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
    async fn test_cram_file() -> Result<(), Box<dyn std::error::Error>> {
        let ctx = ExonSession::new_exon();

        let cram_path = exon_test::test_path("cram", "test_input_1_a.cram");

        let df = ctx
            .read_cram(
                cram_path.to_str().unwrap(),
                ListingCRAMTableOptions::default(),
            )
            .await?;

        assert_eq!(df.count().await?, 15);

        let cram_path = exon_test::test_path("two-cram", "twolib.sorted.cram");
        let fasta_reference = exon_test::test_path("two-cram", "rand1k.fa");

        let df = ctx
            .read_cram(
                cram_path.to_str().unwrap(),
                ListingCRAMTableOptions::default()
                    .with_fasta_reference(fasta_reference.to_str().map(|s| s.to_string())),
            )
            .await?;

        assert_eq!(df.count().await?, 4);

        let region = "1".parse()?;

        let df = ctx
            .read_cram(
                cram_path.to_str().unwrap(),
                ListingCRAMTableOptions::default()
                    .with_fasta_reference(fasta_reference.to_str().map(|s| s.to_string()))
                    .with_indexed(true)
                    .with_region(Some(region)),
            )
            .await?;

        assert_eq!(df.count().await?, 0);

        Ok(())
    }

    #[cfg(feature = "fcs")]
    #[tokio::test]
    async fn test_read_fcs() -> Result<(), Box<dyn std::error::Error>> {
        let ctx = ExonSession::new_exon();

        let fcs_path = exon_test::test_path("fcs", "Guava Muse.fcs");

        let df = ctx
            .read_fcs(
                fcs_path.to_str().unwrap(),
                crate::datasources::fcs::table_provider::ListingFCSTableOptions::new(
                    FileCompressionType::UNCOMPRESSED,
                ),
            )
            .await?;

        assert_eq!(df.count().await?, 108);

        Ok(())
    }

    #[tokio::test]
    async fn test_read_fasta_gzip() -> Result<(), Box<dyn std::error::Error>> {
        let ctx = ExonSession::new_exon();

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
        let ctx = ExonSession::new_exon();

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
        let ctx = ExonSession::new_exon();

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
