use std::{path::PathBuf, sync::Arc};

use datafusion::{error::DataFusionError, prelude::SessionContext};
use datafusion_sqllogictest::DataFusionTestRunner;
use exon::ExonSessionExt;

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

        let mut runner = sqllogictest::Runner::new(|| async {
            Ok(DataFusionTestRunner::new(exon_context.clone()))
        });

        runner.run_file_async(test_file.path()).await.unwrap();
    }

    Ok(())
}

#[tokio::main]
pub async fn main() -> Result<(), DataFusionError> {
    run_tests().await
}
