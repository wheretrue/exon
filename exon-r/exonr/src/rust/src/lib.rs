use std::sync::Arc;

use datafusion::{arrow::ffi_stream::FFI_ArrowArrayStream, prelude::SessionContext};
use exon::{context::ExonSessionExt, ffi::create_dataset_stream_from_table_provider};
use extendr_api::prelude::*;

fn read_inferred_exon_table_inner(path: &str, stream_ptr: *mut FFI_ArrowArrayStream) {
    let rt = Arc::new(tokio::runtime::Runtime::new().unwrap());

    let ctx = SessionContext::new();

    rt.block_on(async {
        let df = ctx.read_inferred_exon_table(path).await.unwrap();

        create_dataset_stream_from_table_provider(df, rt.clone(), stream_ptr)
            .await
            .unwrap();
    });
}

/// Copy the inferred exon table from the given path into the given stream.
/// @export
#[extendr]
fn read_inferred_exon_table(file_path: &str, stream_ptr: &str) {
    let stream_out_ptr_addr: usize = stream_ptr.parse().unwrap();

    let stream_out_ptr = stream_out_ptr_addr as *mut FFI_ArrowArrayStream;

    read_inferred_exon_table_inner(file_path, stream_out_ptr);
}

extendr_module! {
    mod exonr;

    fn read_inferred_exon_table;
}
