use datafusion::prelude::SessionConfig;
// use exon::{context::ExonSessionExt, ffi::create_dataset_stream_from_table_provider};
use extendr_api::prelude::*;
// use tokio::runtime::Runtime;

/// Return string `"Hello world!"` to R.
/// @export
#[extendr]
fn read_fasta_file() -> &'static str {
    let config = SessionConfig::new().with_batch_size(8096);
    // let _ctx = SessionContext::with_config(config);
    return "Hello world!";

    // return stream_ptr;
}

// Macro to generate exports.
// This ensures exported functions are registered with R.
// See corresponding C code in `entrypoint.c`.
extendr_module! {
    mod exonr;

    fn read_fasta_file;
}
