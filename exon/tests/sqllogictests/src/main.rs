use std::{path::PathBuf, sync::Arc, time::Duration};

use async_trait::async_trait;
use datafusion::{error::DataFusionError, prelude::SessionContext};
use exon::ExonSessionExt;

use sqllogictest::{ColumnType, DBOutput, TestError};

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

pub type DFOutput = DBOutput<DFColumnType>;

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

async fn run_query(ctx: &SessionContext, sql: impl Into<String>) -> Result<DFOutput, TestError> {
    let _ = ctx.sql(sql.into().as_str()).await.unwrap();
    Ok(DBOutput::StatementComplete(0))
}

#[async_trait]
impl sqllogictest::AsyncDB for ExonTextRunner {
    type Error = TestError;
    type ColumnType = DFColumnType;

    async fn run(&mut self, sql: &str) -> Result<DFOutput, TestError> {
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
    let test_files = std::fs::read_dir(&test_options.test_dir).unwrap();

    let exon_context = Arc::new(SessionContext::new_exon());

    for test_file in test_files {
        let test_file = test_file.unwrap();

        // if the file doesn't end with an slt extension skip it
        if test_file.path().extension().unwrap() != "slt" {
            continue;
        }

        let mut runner =
            sqllogictest::Runner::new(|| async { Ok(ExonTextRunner::new(exon_context.clone())) });
        runner.run_file_async(test_file.path()).await.unwrap();
    }

    Ok(())
}

#[tokio::main]
pub async fn main() -> Result<(), DataFusionError> {
    run_tests().await
}
