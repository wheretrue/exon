use std::sync::Arc;

use exon::datasources::fasta::FASTAConfig;
use extendr_api::prelude::*;

fn read_fasta_file_inner(path: &str) -> &'static str {
    let rt = tokio::runtime::Runtime::new().unwrap();

    rt.block_on(async {
        let file = tokio::fs::File::open(path).await.unwrap();
        let mut reader = tokio::io::BufReader::new(file);

        let config = Arc::new(FASTAConfig::default());
        let batch_reader = exon::datasources::fasta::BatchReader::new(reader, config);
    });

    return "Hello world!";
}

/// Return string `"Hello world!"` to R.
/// @export
#[extendr]
fn read_fasta_file() -> &'static str {
    return read_fasta_file_inner("/Users/thauck/wheretrue/github.com/wheretrue/exon/exon/test-data/datasources/fasta/test.fasta");
}

// Macro to generate exports.
// This ensures exported functions are registered with R.
// See corresponding C code in `entrypoint.c`.
extendr_module! {
    mod exonr;

    fn read_fasta_file;
}
