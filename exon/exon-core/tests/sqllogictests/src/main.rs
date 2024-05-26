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

use std::{path::PathBuf, sync::Arc, time::Duration};

use async_trait::async_trait;
use clap::Parser;
use datafusion::{error::DataFusionError, scalar::ScalarValue};

use exon::ExonSession;
use sqllogictest::{ColumnType, DBOutput, DefaultColumnType, TestErrorKind};
use tracing::Level;
use tracing_subscriber::FmtSubscriber;

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum DFColumnType {
    Boolean,
    DateTime,
    Integer,
    Float,
    Text,
    Timestamp,
    Another,
}

impl ColumnType for DFColumnType {
    fn from_char(value: char) -> Option<Self> {
        match value {
            'B' => Some(Self::Boolean),
            'D' => Some(Self::DateTime),
            'I' => Some(Self::Integer),
            'P' => Some(Self::Timestamp),
            'R' => Some(Self::Float),
            'T' => Some(Self::Text),
            _ => Some(Self::Another),
        }
    }

    fn to_char(&self) -> char {
        match self {
            Self::Boolean => 'B',
            Self::DateTime => 'D',
            Self::Integer => 'I',
            Self::Timestamp => 'P',
            Self::Float => 'R',
            Self::Text => 'T',
            Self::Another => '?',
        }
    }
}

pub type DFOutput = DBOutput<DefaultColumnType>;

#[derive(Debug, Parser)]
struct Options {
    /// The path to the directory containing the test files.
    #[clap(long, default_value = "tests/sqllogictests/slt/")]
    test_dir: PathBuf,

    /// A specific test to run.
    #[clap(long)]
    test_name: Option<String>,
}

pub struct ExonTextRunner {
    context: Arc<ExonSession>,
}

impl ExonTextRunner {
    pub fn new(context: Arc<ExonSession>) -> Self {
        Self { context }
    }
}

async fn run_query(ctx: &ExonSession, sql: impl Into<String>) -> Result<DFOutput, DataFusionError> {
    let q = sql.into();

    let df = ctx.session.sql(q.as_str()).await?;

    let mut output = Vec::new();

    let batches = df.collect().await?;
    let mut num_columns = 0;

    for batch in batches.iter() {
        num_columns = batch.num_columns();

        for row_idx in 0..batch.num_rows() {
            let mut row_output = Vec::with_capacity(num_columns);

            for col in batch.columns() {
                let scalar = ScalarValue::try_from_array(col, row_idx)?;
                let scalar_string = scalar.to_string();

                // rstrip the string to remove the trailing whitespace
                let scalar_string = scalar_string.trim_end().to_string();

                row_output.push(scalar_string);
            }

            output.push(row_output);
        }
    }

    Ok(DBOutput::Rows {
        types: vec![DefaultColumnType::Text; num_columns],
        rows: output,
    })
}

#[async_trait]
impl sqllogictest::AsyncDB for ExonTextRunner {
    type Error = DataFusionError;
    type ColumnType = DefaultColumnType;

    async fn run(&mut self, sql: &str) -> Result<DFOutput, DataFusionError> {
        run_query(&self.context, sql).await
    }

    /// Engine name of current database.
    fn engine_name(&self) -> &str {
        "ExonRunner"
    }

    /// [`Runner`] calls this function to perform sleep.
    ///
    /// The default implementation is `std::thread::sleep`, which is universal to any async runtime
    /// but would block the current thread. If you are running in tokio runtime, you should override
    /// this by `tokio::time::sleep`.
    async fn sleep(dur: Duration) {
        tokio::time::sleep(dur).await;
    }
}

async fn run_tests(test_options: &Options) -> Result<(), DataFusionError> {
    // Iterate through the test files and run the tests.
    let test_files = std::fs::read_dir(&test_options.test_dir)?;

    let exon_context = Arc::new(ExonSession::new_exon());

    for test_file in test_files {
        let test_file = test_file?;

        // if the filename matches the test_name only run that test
        if let Some(ref test_name) = test_options.test_name {
            if test_file.file_name().to_str().expect("expected file name") != test_name {
                continue;
            }
        }

        // special case pssm tests when the motif-udf feature is enabled
        if test_file
            .path()
            .file_name()
            .expect("expected file name")
            .to_str()
            .expect("expected file name")
            == "pssm.slt"
        {
            #[cfg(not(feature = "motif-udf"))]
            continue;
        }

        // if the file doesn't end with an slt extension skip it
        if test_file
            .path()
            .extension()
            .expect("expected file extension")
            != "slt"
        {
            continue;
        }

        let mut runner =
            sqllogictest::Runner::new(|| async { Ok(ExonTextRunner::new(exon_context.clone())) });

        let err = runner
            .run_file_async(test_file.path())
            .await
            .map_err(|e| match e.kind() {
                TestErrorKind::QueryResultMismatch {
                    actual, expected, ..
                } if actual == expected => {
                    if actual == expected {
                        DataFusionError::Context(
                            "Not equal but equal".to_string(),
                            Box::new(DataFusionError::Execution(
                                "Error running sqllogictest file".to_string(),
                            )),
                        )
                    } else {
                        DataFusionError::Execution(format!(
                            "Error running sqllogictest file: {}",
                            e
                        ))
                    }
                }

                _ => DataFusionError::Execution(format!("Error running sqllogictest file: {}", e)),
            });

        match err {
            Ok(_) => {}
            Err(DataFusionError::Context(_msg, _e)) => {}
            Err(e) => return Err(e),
        }
    }

    Ok(())
}

#[tokio::main]
pub async fn main() -> Result<(), DataFusionError> {
    // dont run on windows
    if cfg!(windows) {
        return Ok(());
    }

    // Setup tracing
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let test_options: Options = clap::Parser::parse();
    run_tests(&test_options).await
}
