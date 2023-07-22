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

use arrow::ffi_stream::FFI_ArrowArrayStream;
use datafusion::error::DataFusionError;
use datafusion::prelude::SessionContext;
use exon::runtime_env::ExonRuntimeEnvExt;
use exon::{context::ExonSessionExt, ffi::create_dataset_stream_from_table_provider};
use extendr_api::{extendr, extendr_module, list, Attributes, Conversions, IntoRobj};

fn read_inferred_exon_table_inner(
    path: &str,
    stream_ptr: *mut FFI_ArrowArrayStream,
) -> Result<(), DataFusionError> {
    let rt = Arc::new(tokio::runtime::Runtime::new().unwrap());

    let ctx = SessionContext::new();

    rt.block_on(async {
        ctx.runtime_env()
            .exon_register_object_store_uri(path)
            .await?;

        let df = ctx.read_inferred_exon_table(path).await?;

        create_dataset_stream_from_table_provider(df, rt.clone(), stream_ptr).await?;

        Ok::<(), DataFusionError>(())
    })?;

    Ok(())
}

/// Wrap a result in a list with "ok" and "err" fields.
pub fn r_result_list<T, E>(result: std::result::Result<T, E>) -> list::List
where
    T: IntoRobj,
    E: std::fmt::Display,
{
    match result {
        Ok(x) => list!(ok = x.into_robj(), err = extendr_api::NULL),
        Err(x) => list!(ok = extendr_api::NULL, err = x.to_string()),
    }
    .set_class(&["rust_result"])
    .unwrap_or_default()
    .as_list()
    .unwrap_or_default()
}

/// Copy the inferred exon table from the given path into the given stream.
/// @export
#[extendr]
fn read_inferred_exon_table(file_path: &str, stream_ptr: &str) -> list::List {
    let stream_out_ptr_addr: usize = stream_ptr.parse().unwrap();

    let stream_out_ptr = stream_out_ptr_addr as *mut FFI_ArrowArrayStream;

    let val = read_inferred_exon_table_inner(file_path, stream_out_ptr);

    r_result_list(val)
}

extendr_module! {
    mod exonr;

    fn read_inferred_exon_table;
}
