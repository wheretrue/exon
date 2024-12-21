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

use clap::Parser;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::SessionState;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::DataFrame;
use datafusion_cli::cli_context::CliSessionContext;
use datafusion_cli::exec;
use datafusion_cli::print_format::PrintFormat;
use datafusion_cli::print_options::{MaxRows, PrintOptions};
use exon::{new_exon_config, ExonSession};
use object_store::ObjectStore;
use tracing_subscriber::{EnvFilter, FmtSubscriber};

struct ExonCLISession {
    exon_session: ExonSession,
}

impl ExonCLISession {
    fn try_new() -> crate::Result<Self> {
        let config = new_exon_config();
        let exon_session = ExonSession::with_config_exon(config)?;

        Ok(Self { exon_session })
    }
}

#[async_trait::async_trait]
impl CliSessionContext for ExonCLISession {
    fn task_ctx(&self) -> Arc<TaskContext> {
        self.exon_session.session.task_ctx()
    }

    fn session_state(&self) -> SessionState {
        self.exon_session.session.state()
    }

    fn register_table_options_extension_from_scheme(&self, scheme: &str) {
        self.exon_session
            .session
            .register_table_options_extension_from_scheme(scheme);
    }

    fn register_object_store(
        &self,
        url: &url::Url,
        object_store: Arc<dyn ObjectStore>,
    ) -> Option<Arc<dyn ObjectStore + 'static>> {
        self.exon_session
            .session
            .register_object_store(url, object_store)
    }

    /// Execute a logical plan and return a DataFrame.
    async fn execute_logical_plan(&self, plan: LogicalPlan) -> Result<DataFrame, DataFusionError> {
        let df = self.exon_session.session.execute_logical_plan(plan).await?;

        Ok(df)
    }
}

#[derive(Debug, Parser, PartialEq)]
struct Args {
    #[arg(value_enum, long, default_value = "table")]
    format: PrintFormat,

    #[clap(
        short,
        long,
        help = "Reduce printing other than the results and work quietly"
    )]
    quiet: bool,

    #[clap(
        long,
        help = "The max number of rows to display for 'table' format\n[default: 40] [possible values: numbers(0/10/...), inf(no limit)]",
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

    #[clap(long, help = "Enables console syntax highlighting")]
    color: bool,
}

#[tokio::main]
pub async fn main() -> Result<()> {
    let args = Args::parse();

    let filter = EnvFilter::new(std::env::var("EXON_LOG").unwrap_or_else(|_| "OFF".to_string()));
    let subscriber = FmtSubscriber::builder().with_env_filter(filter).finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let ctx = ExonCLISession::try_new()?;

    let mut print_options = PrintOptions {
        format: args.format,
        quiet: args.quiet,
        maxrows: args.maxrows,
        color: args.color,
    };

    let commands = args.command;
    let files = args.file;

    if commands.is_empty() && files.is_empty() {
        return exec::exec_from_repl(&ctx, &mut print_options)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)));
    }

    if !commands.is_empty() {
        exec::exec_from_commands(&ctx, commands, &print_options).await?;
    }

    if !files.is_empty() {
        exec::exec_from_files(&ctx, files, &print_options).await?;
    }

    Ok(())
}
