use clap::{Parser, Subcommand};
use datafusion::prelude::SessionContext;
use exon::context::ExonSessionExt;

#[derive(Subcommand)]
enum Commands {
    /// Run a VCF query on a file with a region
    VCFQuery {
        /// which path to use
        #[arg(short, long)]
        path: String,

        /// which region to use
        #[arg(short, long)]
        region: String,
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

            println!("Batch count: {batch_count}");
        }
        None => {}
    }
}
