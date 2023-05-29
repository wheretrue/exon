use std::sync::Arc;

use async_trait::async_trait;
use datafusion::{
    datasource::{
        file_format::file_type::FileCompressionType,
        listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
    },
    error::DataFusionError,
    execution::{context::SessionState, runtime_env::RuntimeEnv},
    prelude::{DataFrame, SessionConfig, SessionContext},
};

use crate::datasources::{TCAFileType, TCAListingTableFactory};

/// Extension trait for [`SessionContext`] that adds TCA-specific functionality.
#[async_trait]
pub trait TCASessionExt {
    /// Reads a TCA table from the given path of a certain type and optional compression type.
    async fn read_tca_table(
        &self,
        table_path: &str,
        file_type: TCAFileType,
        file_compression_type: Option<FileCompressionType>,
    ) -> Result<DataFrame, DataFusionError>;

    /// Reads a TCA table from a given path and infers the type of the table and compression type.
    async fn read_inferred_tca_table(&self, table_path: &str)
        -> Result<DataFrame, DataFusionError>;

    /// Create a new TCA based [`SessionContext`].
    fn new_tca() -> SessionContext {
        SessionContext::with_config_tca(SessionConfig::new())
    }

    /// Create a new TCA based [`SessionContext`] with the given config.
    fn with_config_tca(config: SessionConfig) -> SessionContext {
        let runtime = Arc::new(RuntimeEnv::default());
        Self::with_config_rt_tca(config, runtime)
    }

    /// Create a new TCA based [`SessionContext`] with the given config and runtime.
    fn with_config_rt_tca(config: SessionConfig, runtime: Arc<RuntimeEnv>) -> SessionContext {
        let mut state = SessionState::with_config_rt(config, runtime);

        let mut sources = vec![
            "BAM",
            "BCF",
            "BED",
            "FASTA",
            "FASTQ",
            "GENBANK",
            "GFF",
            "HMMDOMTAB",
            "SAM",
            "VCF",
        ];

        #[cfg(feature = "mzml")]
        sources.push("MZML");

        for source in sources {
            state
                .table_factories_mut()
                .insert(source.into(), Arc::new(TCAListingTableFactory::default()));
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
            .read_tca_table(table_path, TCAFileType::FASTA, file_compression_type)
            .await;
    }

    /// Read a BAM file.
    async fn read_bam(
        &self,
        table_path: &str,
        file_compression_type: Option<FileCompressionType>,
    ) -> Result<DataFrame, DataFusionError> {
        return self
            .read_tca_table(table_path, TCAFileType::BAM, file_compression_type)
            .await;
    }

    /// Read a SAM file.
    async fn read_sam(
        &self,
        table_path: &str,
        file_compression_type: Option<FileCompressionType>,
    ) -> Result<DataFrame, DataFusionError> {
        return self
            .read_tca_table(table_path, TCAFileType::SAM, file_compression_type)
            .await;
    }

    /// Read a FASTQ file.
    async fn read_fastq(
        &self,
        table_path: &str,
        file_compression_type: Option<FileCompressionType>,
    ) -> Result<DataFrame, DataFusionError> {
        return self
            .read_tca_table(table_path, TCAFileType::FASTQ, file_compression_type)
            .await;
    }

    /// Read a VCF file.
    async fn read_vcf(
        &self,
        table_path: &str,
        file_compression_type: Option<FileCompressionType>,
    ) -> Result<DataFrame, DataFusionError> {
        return self
            .read_tca_table(table_path, TCAFileType::VCF, file_compression_type)
            .await;
    }

    /// Read a BCF file.
    async fn read_bcf(
        &self,
        table_path: &str,
        file_compression_type: Option<FileCompressionType>,
    ) -> Result<DataFrame, DataFusionError> {
        return self
            .read_tca_table(table_path, TCAFileType::BCF, file_compression_type)
            .await;
    }

    /// Read a GFF file.
    async fn read_gff(
        &self,
        table_path: &str,
        file_compression_type: Option<FileCompressionType>,
    ) -> Result<DataFrame, DataFusionError> {
        return self
            .read_tca_table(table_path, TCAFileType::GFF, file_compression_type)
            .await;
    }

    /// Read a BED file.
    async fn read_bed(
        &self,
        table_path: &str,
        file_compression_type: Option<FileCompressionType>,
    ) -> Result<DataFrame, DataFusionError> {
        return self
            .read_tca_table(table_path, TCAFileType::BED, file_compression_type)
            .await;
    }

    /// Read a GENBANK file.
    async fn read_genbank(
        &self,
        table_path: &str,
        file_compression_type: Option<FileCompressionType>,
    ) -> Result<DataFrame, DataFusionError> {
        return self
            .read_tca_table(table_path, TCAFileType::GENBANK, file_compression_type)
            .await;
    }

    /// Read a HMMER DOMTAB file.
    async fn read_hmm_dom_tab(
        &self,
        table_path: &str,
        file_compression_type: Option<FileCompressionType>,
    ) -> Result<DataFrame, DataFusionError> {
        return self
            .read_tca_table(table_path, TCAFileType::HMMER, file_compression_type)
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
            .read_tca_table(table_path, TCAFileType::MZML, file_compression_type)
            .await;
    }
}

#[async_trait]
impl TCASessionExt for SessionContext {
    async fn read_tca_table(
        &self,
        table_path: &str,
        file_type: TCAFileType,
        file_compression_type: Option<FileCompressionType>,
    ) -> Result<DataFrame, DataFusionError> {
        let session_state = self.state();
        let table_path = ListingTableUrl::parse(table_path)?;

        let file_compression_type =
            file_compression_type.unwrap_or(FileCompressionType::UNCOMPRESSED);

        let file_format = file_type.get_file_format(file_compression_type)?;
        let lo = ListingOptions::new(file_format);

        let resolved_schema = lo.infer_schema(&session_state, &table_path).await?;

        let config = ListingTableConfig::new(table_path)
            .with_listing_options(lo)
            .with_schema(resolved_schema);

        let table = ListingTable::try_new(config)?;

        self.read_table(Arc::new(table))
    }

    async fn read_inferred_tca_table(
        &self,
        table_path: &str,
    ) -> Result<DataFrame, DataFusionError> {
        let session_state = self.state();

        let table_url = ListingTableUrl::parse(table_path)?;
        let file_format = crate::datasources::infer_tca_format(&table_path)?;
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
    use datafusion::{error::DataFusionError, prelude::SessionContext};

    use crate::{context::tca_session_ext::TCASessionExt, tests::test_path};

    #[tokio::test]
    async fn test_infer() -> Result<(), DataFusionError> {
        let ctx = SessionContext::new();

        let mut test_table = vec![
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
            ("genbank", "test.gb"),
            ("genbank", "test.gb.gz"),
            ("genbank", "test.gb.zst"),
            ("genbank", "test.genbank"),
            ("genbank", "test.gbk"),
            ("gff", "test.gff.zst"),
            ("gff", "test.gff.gz"),
            ("gff", "test.gff"),
            ("vcf", "index.vcf"),
            ("bcf", "index.bcf"),
            ("vcf", "index.vcf.gz"),
            ("hmmdomtab", "test.hmmdomtab.zst"),
            ("hmmdomtab", "test.hmmdomtab.gz"),
            ("hmmdomtab", "test.hmmdomtab"),
        ];

        #[cfg(feature = "mzml")]
        test_table.extend(vec![
            ("mzml", "test.mzml.zst"),
            ("mzml", "test.mzml.gz"),
            ("mzml", "test.mzml"),
        ]);

        for (cat, fname) in test_table.iter() {
            let test_path = test_path(cat, fname);

            let df = ctx
                .read_inferred_tca_table(test_path.to_str().unwrap())
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

        let path = test_path("mzml", "test.mzml");

        let df = ctx.read_mzml(path.to_str().unwrap(), None).await.unwrap();

        let batches = df.collect().await.unwrap();
        let len_sum = batches.iter().fold(0, |acc, b| acc + b.num_rows());

        assert_eq!(len_sum, 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_hmmer_dom_tab() -> Result<(), DataFusionError> {
        //! Test tat the TCASessionExt can read a HMMER domtab file
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
        assert_eq!(batches[0].num_rows(), 1);
        assert_eq!(batches[0].num_columns(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_read_bam() -> Result<(), DataFusionError> {
        //! Test tat the TCASessionExt can read a BAM file
        let ctx = SessionContext::new();

        let path = test_path("bam", "test.bam");
        let df = ctx.read_bam(path.to_str().unwrap(), None).await.unwrap();

        let df = df.select_columns(&["name"]).unwrap();

        let batches = df.collect().await.unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
        assert_eq!(batches[0].num_columns(), 1);

        Ok(())
    }

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
        //! Test tat the TCASessionExt can read a FASTQ file
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

        let df = ctx
            .read_vcf(path.to_str().unwrap(), None)
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
            .select_columns(&["seqid", "source", "type", "start", "end"])?;

        let batches = df.collect().await.unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 5000);
        assert_eq!(batches[0].num_columns(), 5);

        Ok(())
    }

    #[tokio::test]
    async fn test_create_external_table() -> Result<(), DataFusionError> {
        //! Test that with the TCASessionExt we can create an external table

        let path = test_path("fasta", "test.fasta");

        let ctx = SessionContext::new_tca();
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
}
