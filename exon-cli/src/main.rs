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
