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

use clap::{Parser, Subcommand};
use datafusion::{
    datasource::file_format::file_compression_type::FileCompressionType,
    prelude::{col, lit},
};
use exon::{
    datasources::{
        fasta::table_provider::ListingFASTATableOptions,
        mzml::table_provider::ListingMzMLTableOptions, sdf::ListingSDFTableOptions,
    },
    new_exon_config, ExonRuntimeEnvExt, ExonSession,
};
use tracing::Level;
use tracing_subscriber::FmtSubscriber;

#[derive(Subcommand)]
enum Commands {
    /// Run a VCF query on a file with a region.
    VCFQuery {
        /// which path to use
        #[arg(short, long)]
        path: String,

        /// which region to use
        #[arg(short, long)]
        region: String,
    },
    /// Run a BAM query on a file with a region.
    BAMQuery {
        /// which path to use for the BAM file
        #[arg(short, long)]
        path: String,

        /// which region to use
        #[arg(short, long)]
        region: String,
    },
    /// Run a BAM scan on a file... no region.
    BAMScan {
        #[arg(short, long)]
        path: String,
    },
    /// Scan a FASTA file and count the number of non-methionine start codons
    FASTACodonScan {
        /// which path to use
        #[arg(short, long)]
        path: String,

        /// which compression to use
        #[arg(short, long)]
        compression: Option<FileCompressionType>,
    },
    /// Parallel FASTA Scan
    FASTAScanParallel {
        /// path directory with FASTA files
        #[arg(short, long)]
        path: String,

        /// Number of target partitions
        #[arg(short, long)]
        workers: usize,
    },
    /// Count the number of spectra in a mzML file
    MzMLScan {
        /// which path to use
        #[arg(short, long)]
        path: String,

        /// which compression to use
        #[arg(short, long)]
        compression: Option<FileCompressionType>,
    },
    /// SDF Query
    SDFQuery {
        /// which path to use
        #[arg(short, long)]
        path: String,
    },
}

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Turn debugging information on
    #[arg(short, long, action = clap::ArgAction::Count)]
    debug: u8,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    match &cli.command {
        Some(Commands::SDFQuery { path }) => {
            let path = path.as_str();

            let ctx = ExonSession::new_exon()?;
            ctx.session
                .runtime_env()
                .exon_register_object_store_uri(path)
                .await?;

            let options = ListingSDFTableOptions::default()
                .with_file_compression_type(FileCompressionType::GZIP);

            let sdf_read = ctx.read_sdf(path, options).await?;

            let count = sdf_read.count().await?;
            eprintln!("Count: {}", count);
        }
        Some(Commands::VCFQuery { path, region }) => {
            let path = path.as_str();

            let ctx = ExonSession::new_exon()?;
            ctx.session
                .runtime_env()
                .exon_register_object_store_uri(path)
                .await?;

            ctx.session.sql(
                format!(
                    "CREATE EXTERNAL TABLE vcf_file STORED AS INDEXED_VCF LOCATION '{}' OPTIONS (compression gzip);",
                    path
                )
                .as_str(),
            )
            .await?;

            let df = ctx
                .session.sql(format!("SELECT chrom, pos, array_to_string(id, ':') AS id FROM vcf_file WHERE vcf_region_filter('{}', chrom, pos) = true;", region).as_str())
                .await?;

            let cnt = df.count().await?;
            eprintln!("Count: {}", cnt);
        }
        Some(Commands::BAMScan { path }) => {
            let path = path.as_str();
            let ctx = ExonSession::new_exon()?;
            ctx.session
                .runtime_env()
                .exon_register_object_store_uri(path)
                .await?;

            ctx.session
                .sql(
                    format!(
                        "CREATE EXTERNAL TABLE bam STORED AS BAM LOCATION '{}';",
                        path
                    )
                    .as_str(),
                )
                .await?;

            let df = ctx
                .session
                .sql("SELECT COUNT(*) FROM bam")
                .await?
                .collect()
                .await?;

            assert!(df.len() == 1);

            eprintln!("Batch Count: {:?}", df[0]);
        }
        Some(Commands::BAMQuery { path, region }) => {
            let path = path.as_str();
            let region = region.as_str();

            let ctx = ExonSession::new_exon()?;
            ctx.session
                .runtime_env()
                .exon_register_object_store_uri(path)
                .await?;

            ctx.session
                .sql(
                    format!(
                        "CREATE EXTERNAL TABLE bam STORED AS INDEXED_BAM LOCATION '{}';",
                        path
                    )
                    .as_str(),
                )
                .await?;

            let df = ctx
                .session
                .sql(
                    format!(
                        "SELECT reference FROM bam WHERE bam_region_filter('{}', reference, start, end) = true;",
                        region
                    )
                    .as_str(),
                )
                .await?;

            let cnt = df.count().await?;

            eprintln!("Count: {}", cnt);
        }
        Some(Commands::FASTACodonScan { path, compression }) => {
            let compression = compression.unwrap_or(FileCompressionType::UNCOMPRESSED);
            let options = ListingFASTATableOptions::new(compression);

            let ctx = ExonSession::new_exon()?;

            let df = ctx.read_fasta(path, options).await?;

            let count = df.filter(col("sequence").ilike(lit("M%")))?.count().await?;

            eprintln!("Count: {count}");
        }
        Some(Commands::FASTAScanParallel { path, workers }) => {
            let exon_config = new_exon_config().with_target_partitions(*workers);
            let ctx = ExonSession::with_config_exon(exon_config)?;

            let options = ListingFASTATableOptions::default();

            let df = ctx.read_fasta(path, options).await?;

            let count = df.filter(col("sequence").ilike(lit("M%")))?.count().await?;
            assert_eq!(count, 4_437_864);

            eprintln!("Count: {count}");
        }
        Some(Commands::MzMLScan { path, compression }) => {
            let path = path.as_str();

            let compression = compression.unwrap_or(FileCompressionType::UNCOMPRESSED);
            let options = ListingMzMLTableOptions::new(compression);

            let ctx = ExonSession::new_exon()?;

            let df = ctx.read_mzml(path, options).await?;
            let count = df.count().await?;

            eprintln!("Count: {count}");
        }
        None => {}
    }

    Ok(())
}
