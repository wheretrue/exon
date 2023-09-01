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
    common::FileCompressionType,
    datasource::listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
    error::{DataFusionError, Result},
    execution::{
        context::{QueryPlanner, SessionState},
        options::ReadOptions,
        runtime_env::RuntimeEnv,
    },
    logical_expr::LogicalPlan,
    physical_plan::ExecutionPlan,
    physical_planner::PhysicalPlanner,
    prelude::{DataFrame, SessionConfig, SessionContext},
};
use noodles::core::Region;

use crate::{
    datasources::{
        bam::BAMFormat, bcf::BCFFormat, vcf::VCFFormat, ExonFileType, ExonListingTableFactory,
        ExonReadOptions,
    },
    new_exon_config,
    physical_optimizer::{
        file_repartitioner::ExonRoundRobin, interval_optimizer_rule::ExonIntervalOptimizer,
    },
    physical_optimizer::{
        region_between_rewriter::RegionBetweenRule,
        vcf_region_optimizer_rule::ExonVCFRegionOptimizer,
    },
    physical_plan::exon_physical_planner::ExonPhysicalPlanner,
};

struct DefaultQueryPlanner {}

#[async_trait]
impl QueryPlanner for DefaultQueryPlanner {
    /// Given a `LogicalPlan`, create an [`ExecutionPlan`] suitable for execution
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let planner = ExonPhysicalPlanner::default();

        planner
            .create_physical_plan(logical_plan, session_state)
            .await
    }
}

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

    /// Reads a Exon table from a given path and infers the type of the table and compression type.
    async fn read_inferred_exon_table(
        &self,
        table_path: &str,
    ) -> Result<DataFrame, DataFusionError>;

    /// Create a new Exon based [`SessionContext`].
    fn new_exon() -> SessionContext {
        let exon_config = new_exon_config();
        let ctx = SessionContext::with_config_exon(exon_config);

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

        ctx
    }

    /// Create a new Exon based [`SessionContext`] with the given config.
    fn with_config_exon(config: SessionConfig) -> SessionContext {
        let runtime = Arc::new(RuntimeEnv::default());
        Self::with_config_rt_exon(config, runtime)
    }

    /// Register a Exon table from the given path of a certain type and optional compression type.
    async fn register_exon_table(
        &self,
        name: &str,
        table_path: &str,
        options: ExonReadOptions<'_>,
    ) -> Result<(), DataFusionError>;

    /// Create a new Exon based [`SessionContext`] with the given config and runtime.
    fn with_config_rt_exon(config: SessionConfig, runtime: Arc<RuntimeEnv>) -> SessionContext {
        let round_robin_optimizer = ExonRoundRobin::default();
        let vcf_region_optimizer = ExonVCFRegionOptimizer::default();
        let region_between_optimizer = RegionBetweenRule::default();
        let interval_region_optimizer = ExonIntervalOptimizer::default();

        let mut state = SessionState::with_config_rt(config, runtime)
            .with_physical_optimizer_rules(vec![
                Arc::new(round_robin_optimizer),
                Arc::new(region_between_optimizer),
                Arc::new(vcf_region_optimizer),
                Arc::new(interval_region_optimizer),
            ]);

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
            "SAM",
            "VCF",
            #[cfg(feature = "mzml")]
            "MZML",
        ];

        for source in sources {
            state
                .table_factories_mut()
                .insert(source.into(), Arc::new(ExonListingTableFactory::default()));
        }

        SessionContext::with_state(state)
    }

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
            .read_exon_table(table_path, ExonFileType::HMMER, file_compression_type)
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

    /// Query a VCF file.
    ///
    /// File must be indexed and index file must be in the same directory as the VCF file.
    async fn query_vcf_file(
        &self,
        table_path: &str,
        query: &str,
    ) -> Result<DataFrame, DataFusionError>;

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
    async fn query_bam_file(
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
        let table_path = ListingTableUrl::parse(table_path)?;

        let file_compression_type =
            file_compression_type.unwrap_or(FileCompressionType::UNCOMPRESSED);

        let file_format = file_type.get_file_format(file_compression_type);
        let lo = ListingOptions::new(file_format);

        let resolved_schema = lo.infer_schema(&session_state, &table_path).await?;

        let config = ListingTableConfig::new(table_path)
            .with_listing_options(lo)
            .with_schema(resolved_schema);

        let table = ListingTable::try_new(config)?;

        self.read_table(Arc::new(table))
    }

    async fn register_exon_table(
        &self,
        name: &str,
        table_path: &str,
        options: ExonReadOptions<'_>,
    ) -> Result<(), DataFusionError> {
        let table_path = ListingTableUrl::parse(table_path)?;

        let listing_options = options.to_listing_options(&self.copied_config());

        self.register_listing_table(
            name,
            table_path,
            listing_options,
            options.schema.map(|s| Arc::new(s.to_owned())),
            None,
        )
        .await?;

        Ok(())
    }

    async fn query_bam_file(
        &self,
        table_path: &str,
        query: &str,
    ) -> Result<DataFrame, DataFusionError> {
        let session_state = self.state();
        let table_path = ListingTableUrl::parse(table_path)?;

        let region: Region = query
            .parse()
            .map_err(|_| DataFusionError::Execution("Failed to parse query string".to_string()))?;

        let file_format = BAMFormat::default().with_region_filter(region);
        let boxed_format = Arc::new(file_format);

        let lo = ListingOptions::new(boxed_format);

        let resolved_schema = lo.infer_schema(&session_state, &table_path).await?;

        let config = ListingTableConfig::new(table_path)
            .with_listing_options(lo)
            .with_schema(resolved_schema);

        let table = ListingTable::try_new(config)?;

        self.read_table(Arc::new(table))
    }

    async fn query_bcf_file(
        &self,
        table_path: &str,
        query: &str,
    ) -> Result<DataFrame, DataFusionError> {
        let session_state = self.state();
        let table_path = ListingTableUrl::parse(table_path)?;

        let region: Region = query
            .parse()
            .map_err(|_| DataFusionError::Execution("Failed to parse query string".to_string()))?;

        let file_format = BCFFormat::default().with_region_filter(region);
        let boxed_format = Arc::new(file_format);

        let lo = ListingOptions::new(boxed_format);

        let resolved_schema = lo.infer_schema(&session_state, &table_path).await?;

        let config = ListingTableConfig::new(table_path)
            .with_listing_options(lo)
            .with_schema(resolved_schema);

        let table = ListingTable::try_new(config)?;

        self.read_table(Arc::new(table))
    }

    async fn query_vcf_file(
        &self,
        table_path: &str,
        query: &str,
    ) -> Result<DataFrame, DataFusionError> {
        let session_state = self.state();
        let table_path = ListingTableUrl::parse(table_path)?;

        let region: Region = query
            .parse()
            .map_err(|_| DataFusionError::Execution("Failed to parse query string".to_string()))?;

        let file_format = VCFFormat::new(FileCompressionType::GZIP).with_region_filter(region);
        let boxed_format = Arc::new(file_format);

        let lo = ListingOptions::new(boxed_format);

        let resolved_schema = lo.infer_schema(&session_state, &table_path).await?;

        let config = ListingTableConfig::new(table_path)
            .with_listing_options(lo)
            .with_schema(resolved_schema);

        let table = ListingTable::try_new(config)?;

        self.read_table(Arc::new(table))
    }

    async fn read_inferred_exon_table(
        &self,
        table_path: &str,
    ) -> Result<DataFrame, DataFusionError> {
        let session_state = self.state();

        let table_url = ListingTableUrl::parse(table_path)?;
        let file_format = crate::datasources::infer_exon_format(table_path)?;
        let lo = ListingOptions::new(file_format);

        let resolved_schema = lo.infer_schema(&session_state, &table_url).await?;

        let config = ListingTableConfig::new(table_url)
            .with_listing_options(lo)
            .with_schema(resolved_schema);

        let table = ListingTable::try_new(config)?;

        self.read_table(Arc::new(table))
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use arrow::array::Float32Array;
    use datafusion::{
        error::DataFusionError,
        prelude::{col, SessionContext},
    };

    use crate::{
        context::exon_session_ext::ExonSessionExt,
        datasources::{ExonFileType, ExonReadOptions},
        tests::test_path,
    };

    #[tokio::test]
    async fn test_register() -> Result<(), DataFusionError> {
        let file_file = ExonFileType::from_str("fasta").unwrap();

        let options = ExonReadOptions::new(file_file);

        let ctx = SessionContext::new();

        let test_path = test_path("fasta", "test.fasta");

        ctx.register_exon_table("test_fasta", test_path.to_str().unwrap(), options)
            .await
            .unwrap();

        let df = ctx.sql("SELECT * FROM test_fasta").await.unwrap();

        let batches = df.collect().await.unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_columns(), 3);
        assert_eq!(batches[0].num_rows(), 2);

        Ok(())
    }

    #[tokio::test]
    async fn test_infer() -> Result<(), DataFusionError> {
        let ctx = SessionContext::new();
        let test_table = vec![
            ("bam", "test.bam"),
            ("sam", "test.sam"),
            ("bed", "test.bed.zst"),
            ("bed", "test.bed.gz"),
            ("bed", "test.bed"),
            ("fasta", "test.fasta.zst"),
            ("fasta", "test.fasta.gz"),
            ("fasta", "test.fasta"),
            ("fasta", "test.fa"),
            ("fasta", "test.fna"),
            ("fastq", "test.fastq.zst"),
            ("fastq", "test.fastq.gz"),
            ("fastq", "test.fastq"),
            ("fastq", "test.fq"),
            #[cfg(feature = "genbank")]
            ("genbank", "test.gb"),
            #[cfg(feature = "genbank")]
            ("genbank", "test.gb.gz"),
            #[cfg(feature = "genbank")]
            ("genbank", "test.gb.zst"),
            #[cfg(feature = "genbank")]
            ("genbank", "test.genbank"),
            #[cfg(feature = "genbank")]
            ("genbank", "test.gbk"),
            ("gff", "test.gff.zst"),
            ("gff", "test.gff.gz"),
            ("gff", "test.gff"),
            ("gtf", "test.gtf"),
            ("vcf", "index.vcf"),
            ("bcf", "index.bcf"),
            ("vcf", "index.vcf.gz"),
            ("hmmdomtab", "test.hmmdomtab.zst"),
            ("hmmdomtab", "test.hmmdomtab.gz"),
            ("hmmdomtab", "test.hmmdomtab"),
            #[cfg(feature = "mzml")]
            ("mzml", "test.mzML.zst"),
            #[cfg(feature = "mzml")]
            ("mzml", "test.mzML.gz"),
            #[cfg(feature = "mzml")]
            ("mzml", "test.mzML"),
        ];

        for (cat, fname) in test_table.iter() {
            let test_path = test_path(cat, fname);

            let df = ctx
                .read_inferred_exon_table(test_path.to_str().unwrap())
                .await
                .unwrap();
            let batches = df.collect().await.unwrap();
            let len_sum = batches.iter().fold(0, |acc, b| acc + b.num_rows());

            assert!(len_sum > 0);
        }

        Ok(())
    }

    #[cfg(feature = "mzml")]
    #[tokio::test]
    async fn test_read_mzml() -> Result<(), DataFusionError> {
        let ctx = SessionContext::new();

        let path = test_path("mzml", "test.mzML");

        let df = ctx.read_mzml(path.to_str().unwrap(), None).await.unwrap();

        let batches = df.collect().await.unwrap();
        let len_sum = batches.iter().fold(0, |acc, b| acc + b.num_rows());

        assert_eq!(len_sum, 2);

        Ok(())
    }

    #[tokio::test]
    async fn test_hmmer_dom_tab() -> Result<(), DataFusionError> {
        //! Test tat the ExonSessionExt can read a HMMER domtab file
        let ctx = SessionContext::new();

        let path = test_path("hmmdomtab", "test.hmmdomtab");
        let df = ctx
            .read_hmm_dom_tab(path.to_str().unwrap(), None)
            .await
            .unwrap();

        let batches = df.collect().await.unwrap();
        let len_sum = batches.iter().fold(0, |acc, b| acc + b.num_rows());

        assert_eq!(len_sum, 100);

        Ok(())
    }

    #[tokio::test]
    async fn test_read_sam() -> Result<(), DataFusionError> {
        let ctx = SessionContext::new();

        let path = test_path("sam", "test.sam");
        let df = ctx.read_sam(path.to_str().unwrap(), None).await.unwrap();

        let df = df.select_columns(&["name"]).unwrap();

        let batches = df.collect().await.unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
        assert_eq!(batches[0].num_columns(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_read_bed() -> Result<(), DataFusionError> {
        let ctx = SessionContext::new();

        let path = test_path("bed", "test.bed");
        let df = ctx.read_bed(path.to_str().unwrap(), None).await.unwrap();

        let df = df.select_columns(&["name"]).unwrap();

        let batches = df.collect().await.unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 10);
        assert_eq!(batches[0].num_columns(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_query_vcf() -> Result<(), DataFusionError> {
        let ctx = SessionContext::new();

        // let path = "exon/test-data/datasources/vcf/index.vcf.gz";
        let path = test_path("vcf", "index.vcf.gz");
        let path = path.to_str().unwrap();
        let query = "1";

        let df = ctx
            .query_vcf_file(path, query)
            .await
            .unwrap()
            .select(vec![col("chrom")])?;

        let batches = df.collect().await.unwrap();

        assert!(!batches.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_query_bcf() -> Result<(), DataFusionError> {
        let ctx = SessionContext::new();

        let path = test_path("bcf", "index.bcf");
        let query = "1";

        let df = ctx
            .query_bcf_file(path.to_str().unwrap(), query)
            .await
            .unwrap();

        let batches = df.collect().await.unwrap();

        assert!(!batches.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_query_bam() -> Result<(), DataFusionError> {
        let ctx = SessionContext::new();

        let path = test_path("bam", "test.bam");
        let query = "chr1:1-12209153";

        let df = ctx
            .query_bam_file(path.to_str().unwrap(), query)
            .await
            .unwrap();

        let batches = df.collect().await.unwrap();

        assert!(!batches.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_read_bam() -> Result<(), DataFusionError> {
        //! Test tat the ExonSessionExt can read a BAM file
        let ctx = SessionContext::new();

        let path = test_path("bam", "test.bam");
        let df = ctx.read_bam(path.to_str().unwrap(), None).await.unwrap();

        let df = df.select_columns(&["name"]).unwrap();

        let batches = df.collect().await.unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 61);
        assert_eq!(batches[0].num_columns(), 1);

        Ok(())
    }

    #[cfg(feature = "genbank")]
    #[tokio::test]
    async fn test_read_genbank() -> Result<(), DataFusionError> {
        let ctx = SessionContext::new();

        let path = test_path("genbank", "test.gb");
        let df = ctx
            .read_genbank(path.to_str().unwrap(), None)
            .await
            .unwrap();

        let df = df.select_columns(&["name"]).unwrap();

        let batches = df.collect().await.unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
        assert_eq!(batches[0].num_columns(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_read_fastq() -> Result<(), DataFusionError> {
        //! Test tat the ExonSessionExt can read a FASTQ file
        let ctx = SessionContext::new();

        let path = test_path("fastq", "test.fastq");
        let df = ctx.read_fastq(path.to_str().unwrap(), None).await.unwrap();

        let df = df.select_columns(&["name", "sequence"]).unwrap();

        let batches = df.collect().await.unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 2);
        assert_eq!(batches[0].num_columns(), 2);

        Ok(())
    }

    #[tokio::test]
    async fn test_read_vcf() -> Result<(), DataFusionError> {
        let ctx = SessionContext::new();

        let path = test_path("vcf", "index.vcf");

        let df = ctx.read_vcf(path.to_str().unwrap(), None).await.unwrap();

        let batches = df.collect().await.unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 621);

        Ok(())
    }

    #[tokio::test]
    async fn test_read_bcf() -> Result<(), DataFusionError> {
        let ctx = SessionContext::new();

        // let path = test_listing_table_dir("bcf", "index.bcf");
        let path = test_path("bcf", "index.bcf");

        let df = ctx
            .read_bcf(path.to_str().unwrap(), None)
            .await
            .unwrap()
            .select_columns(&["id"])?;

        let batches = df.collect().await.unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 621);
        assert_eq!(batches[0].num_columns(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_read_gff() -> Result<(), DataFusionError> {
        let ctx = SessionContext::new();

        let path = test_path("gff", "test.gff");
        let df = ctx
            .read_gff(path.to_str().unwrap(), None)
            .await
            .unwrap()
            .select_columns(&["seqname", "source", "type", "start", "end"])?;

        let batches = df.collect().await.unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 5000);
        assert_eq!(batches[0].num_columns(), 5);

        Ok(())
    }

    #[tokio::test]
    async fn test_read_gff_bad_directive() -> Result<(), DataFusionError> {
        let ctx = SessionContext::new();

        let path = test_path("gff-bad-directive", "test.gff");
        let batches = ctx
            .read_gff(path.to_str().unwrap(), None)
            .await?
            .collect()
            .await?;

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 7);

        Ok(())
    }

    #[tokio::test]
    async fn test_gc_content_on_context() -> Result<(), DataFusionError> {
        let ctx = SessionContext::new_exon();

        let sql = r#"
            SELECT gc_content('ATCG') as gc_content
        "#;

        let plan = ctx.state().create_logical_plan(sql).await?;
        let df = ctx.execute_logical_plan(plan).await?;

        let batches = df.collect().await.unwrap();
        let batch = &batches[0];

        let gc_content = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();

        assert_eq!(gc_content.value(0), 0.5);

        Ok(())
    }

    #[cfg(feature = "mzml")]
    #[tokio::test]
    async fn test_bin_vector_udf_on_context() -> Result<(), DataFusionError> {
        let ctx = SessionContext::new_exon();

        let sql = r#"
            SELECT bin_vectors(mz, intensity, 100.0, 3, 1.0) AS binned_vector
            FROM (
                SELECT [100.0, 200.0, 300.0, 400.0, 500.0, 600.0] AS mz,
                       [1.0, 2.0, 3.0, 4.0, 5.0, 6.0] AS intensity
            )
        "#;

        let plan = ctx.state().create_logical_plan(sql).await?;

        let v = ctx.execute_logical_plan(plan).await?;

        assert_eq!(v.schema().fields().len(), 1);
        assert_eq!(v.schema().field(0).name(), "binned_vector");

        let batches = v.collect().await.unwrap();

        let binned = arrow::array::as_list_array(batches[0].column(0));

        // iterate over the rows
        for i in 0..batches[0].num_rows() {
            let array = binned.value(i);
            let array = array
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .unwrap();

            assert_eq!(array.len(), 3);
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_create_external_table() -> Result<(), DataFusionError> {
        //! Test that with the ExonSessionExt we can create an external table

        let path = test_path("fasta", "test.fasta");

        let ctx = SessionContext::new_exon();
        let sql = format!(
            "CREATE EXTERNAL TABLE uniprot STORED AS FASTA LOCATION '{}';",
            path.to_str().unwrap()
        );

        ctx.sql(&sql).await.unwrap();

        let sql = "SELECT id, sequence FROM uniprot LIMIT 5;";
        let plan = ctx.state().create_logical_plan(sql).await?;

        let v = ctx.execute_logical_plan(plan).await?;

        assert_eq!(v.schema().field(0).name(), "id");
        assert_eq!(v.schema().field(1).name(), "sequence");

        assert_eq!(v.schema().fields().len(), 2);

        let batches = v.collect().await.unwrap();

        assert_eq!(batches.len(), 1);

        assert_eq!(batches[0].schema().fields().len(), 2);
        assert_eq!(batches[0].num_rows(), 2);

        Ok(())
    }

    #[cfg(all(feature = "aws", not(target_os = "windows")))]
    #[tokio::test]
    async fn test_read_s3() -> Result<(), DataFusionError> {
        use crate::ExonRuntimeEnvExt;

        let ctx = SessionContext::new();

        let path = "s3://test-bucket/test.fasta";
        let _ = ctx
            .runtime_env()
            .exon_register_object_store_uri(path)
            .await?;

        let df = ctx.read_fasta(path, None).await.unwrap();
        let batches = df.collect().await.unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 2);

        Ok(())
    }
}
