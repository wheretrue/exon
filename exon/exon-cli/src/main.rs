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

use clap::Parser;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::SessionContext;
use datafusion_cli::exec;
use datafusion_cli::print_format::PrintFormat;
use datafusion_cli::print_options::{MaxRows, PrintOptions};
use exon::{new_exon_config, ExonSessionExt};

#[derive(Debug, Parser, PartialEq)]
struct Args {
    #[arg(value_enum, long, default_value = "Table")]
    format: PrintFormat,

    #[clap(
        short,
        long,
        help = "Reduce printing other than the results and work quietly"
    )]
    quiet: bool,

    #[clap(
        long,
        help = "The max number of rows to display for 'Table' format\n[default: 40] [possible values: numbers(0/10/...), inf(no limit)]",
        default_value = "40"
    )]
    maxrows: MaxRows,

    #[clap(
        short = 'c',
        long,
        help = "Execute the given command string(s), then exit"
    )]
    command: Vec<String>,

    #[clap(short, long, help = "Execute commands from file(s), then exit")]
    file: Vec<String>,
}

#[tokio::main]
pub async fn main() -> Result<()> {
    let args = Args::parse();

    let config = new_exon_config();
    let mut ctx = SessionContext::with_config_exon(config);

    let mut print_options = PrintOptions {
        format: args.format,
        quiet: args.quiet,
        maxrows: args.maxrows,
    };

    let commands = args.command;
    let files = args.file;

    if commands.is_empty() && files.is_empty() {
        return exec::exec_from_repl(&mut ctx, &mut print_options)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)));
    }

    if !commands.is_empty() {
        exec::exec_from_commands(&mut ctx, commands, &print_options).await?;
    }

    if !files.is_empty() {
        exec::exec_from_files(&mut ctx, files, &print_options).await?;
    }

    Ok(())
}
