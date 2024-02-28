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
use datafusion::{error::DataFusionError, prelude::SessionContext, scalar::ScalarValue};
use exon::ExonSessionExt;

use sqllogictest::{ColumnType, DBOutput, DefaultColumnType};
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

struct TestOptions {
    /// The path to the directory containing the test files.
    test_dir: PathBuf,
}

impl Default for TestOptions {
    fn default() -> Self {
        Self {
            test_dir: PathBuf::from("tests/sqllogictests/slt/"),
        }
    }
}

pub struct ExonTextRunner {
    context: Arc<SessionContext>,
}

impl ExonTextRunner {
    pub fn new(context: Arc<SessionContext>) -> Self {
        Self { context }
    }
}

async fn run_query(
    ctx: &SessionContext,
    sql: impl Into<String>,
) -> Result<DFOutput, DataFusionError> {
    let q = sql.into();

    let df = ctx.sql(q.as_str()).await?;

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

async fn run_tests() -> Result<(), DataFusionError> {
    let test_options = TestOptions::default();

    // Iterate through the test files and run the tests.
    let test_files = std::fs::read_dir(&test_options.test_dir)?;

    let exon_context = Arc::new(SessionContext::new_exon());
    exon_context.runtime_env();

    for test_file in test_files {
        let test_file = test_file?;

        // only run cram-select-tests.slt
        if test_file.path().file_name().expect("expected file name") != "cram-select-tests.slt" {
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

        runner.run_file_async(test_file.path()).await.map_err(|e| {
            DataFusionError::Execution(format!("Error running sqllogictest file: {:?}", e))
        })?;
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

    run_tests().await
}
