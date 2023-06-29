use std::sync::Arc;

use datafusion::{arrow::ffi_stream::FFI_ArrowArrayStream, prelude::SessionContext};
use exon::{context::ExonSessionExt, ffi::create_dataset_stream_from_table_provider};
use extendr_api::prelude::*;

fn read_fasta_file_inner(path: &str, stream_ptr: *mut FFI_ArrowArrayStream) {
    let rt = Arc::new(tokio::runtime::Runtime::new().unwrap());

    let ctx = SessionContext::new();

    rt.block_on(async {
        let df = ctx.read_fasta(path, None).await.unwrap();

        create_dataset_stream_from_table_provider(df, rt.clone(), stream_ptr)
            .await
            .unwrap();
    });
}

/// Return string `"Hello world!"` to R.
/// @export
#[extendr]
fn read_fasta_file_extendr(file_path: &str, stream_ptr: &str) {
    let stream_out_ptr_addr: usize = stream_ptr.parse().unwrap();

    let stream_out_ptr = stream_out_ptr_addr as *mut FFI_ArrowArrayStream;

    read_fasta_file_inner(file_path, stream_out_ptr);

    eprintln!("read_fasta_file_extendr");
}

extendr_module! {
    mod exonr;

    fn read_fasta_file_extendr;
}
