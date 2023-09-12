use clap::{Parser, Subcommand};
use datafusion::prelude::SessionContext;
use exon::ExonRuntimeEnvExt;

#[derive(Subcommand)]
enum Commands {
    /// Cat the file at the given path.
    Cat {
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

    match &cli.command {
        Some(Commands::Cat { path }) => {
            let path = path.as_str();

            let ctx = SessionContext::new();

            let _ = ctx.runtime_env().exon_register_object_store_uri(path).await;
            // let df = ctx.read_inferred_exon_table(path).await?;
            // todo!("read_inferred_exon_table not implemented yet");

            // let count = df.count().await?;
            let count = 0;

            println!("Count: {count}");
        }
        None => {
            println!("No command given");
        }
    }

    Ok(())
}
