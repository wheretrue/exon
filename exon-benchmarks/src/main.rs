use clap::{Parser, Subcommand};
use datafusion::{
    datasource::file_format::file_type::FileCompressionType,
    prelude::{col, lit, SessionContext},
};
use exon::ExonSessionExt;

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
    /// Scan a FASTA file and count the number of non-methionine start codons
    FASTACodonScan {
        /// which path to use
        #[arg(short, long)]
        path: String,

        /// which compression to use
        #[arg(short, long)]
        compression: Option<FileCompressionType>,
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
async fn main() {
    let cli = Cli::parse();

    match &cli.command {
        Some(Commands::VCFQuery { path, region }) => {
            let path = path.as_str();
            let region = region.as_str();

            let ctx = SessionContext::new();

            let df = ctx.query_vcf_file(path, region).await.unwrap();

            let batch_count = df.count().await.unwrap();

            println!("Row count: {batch_count}");
        }
        Some(Commands::BAMQuery { path, region }) => {
            let path = path.as_str();
            let region = region.as_str();

            let ctx = SessionContext::new();

            let df = ctx.query_bam_file(path, region).await.unwrap();
            let batch_count = df.count().await.unwrap();

            println!("Row count: {batch_count}");
        }
        Some(Commands::FASTACodonScan { path, compression }) => {
            let path = path.as_str();
            let compression = compression.to_owned();

            let ctx = SessionContext::new();

            let df = ctx.read_fasta(path, compression).await.unwrap();

            let count = df
                .filter(col("sequence").ilike(lit("M%")))
                .unwrap()
                .count()
                .await
                .unwrap();

            println!("Count: {count}");
        }
        Some(Commands::MzMLScan { path, compression }) => {
            let path = path.as_str();
            let compression = compression.to_owned();

            let ctx = SessionContext::new();

            let df = ctx.read_mzml(path, compression).await.unwrap();

            let count = df.count().await.unwrap();

            println!("Count: {count}");
        }
        None => {}
    }
}
