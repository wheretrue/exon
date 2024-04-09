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

use arrow::ffi_stream::export_reader_into_raw;
use arrow::ffi_stream::FFI_ArrowArrayStream;
use datafusion::error::DataFusionError;
use datafusion::prelude::DataFrame;
use datafusion::prelude::SessionContext;
use exon::ffi::DataFrameRecordBatchStream;
use exon::new_exon_config;
use exon::ExonRuntimeEnvExt;
use exon::{ffi::create_dataset_stream_from_table_provider, ExonSessionExt};
use extendr_api::{
    extendr, extendr_module, list, prelude::*, Attributes, Conversions, IntoRobj, Result,
};
use tokio::runtime::Runtime;

fn read_inferred_exon_table_inner(path: &str, stream_ptr: *mut FFI_ArrowArrayStream) -> Result<()> {
    let rt = Arc::new(tokio::runtime::Runtime::new().unwrap());

    let config = new_exon_config();
    let ctx = ExonSessionExt::with_config_exon(config);

    rt.block_on(async {
        ctx.runtime_env()
            .exon_register_object_store_uri(path)
            .await?;

        let df = ctx.read_inferred_exon_table(path).await?;

        create_dataset_stream_from_table_provider(df, rt.clone(), stream_ptr).await?;

        Ok::<(), DataFusionError>(())
    })
    .map_err(|e| {
        Error::from(format!(
            "Error reading inferred exon table: {}\n{}",
            path,
            e.to_string()
        ))
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

#[derive(Debug, Clone)]
pub struct RDataFrame(pub DataFrame);

#[extendr]
impl RDataFrame {
    pub fn print(&self) -> Self {
        rprintln!("{:?}", self);
        self.clone()
    }

    pub fn to_arrow(&self, stream_ptr: &str) -> list::List {
        let stream_out_ptr_addr: usize = stream_ptr.parse().unwrap();

        let stream_out_ptr = stream_out_ptr_addr as *mut FFI_ArrowArrayStream;

        let runtime = Arc::new(Runtime::new().unwrap());

        let stream = match runtime.block_on(async { self.0.clone().execute_stream().await }) {
            Ok(stream) => stream,
            Err(e) => {
                return r_result_list::<(), Error>(Err(Error::Other(e.to_string())));
            }
        };

        let dataset_record_batch_stream = DataFrameRecordBatchStream::new(stream, runtime);

        unsafe { export_reader_into_raw(Box::new(dataset_record_batch_stream), stream_out_ptr) }

        r_result_list::<(), Error>(Ok(()))
    }
}

impl From<DataFrame> for RDataFrame {
    fn from(df: DataFrame) -> Self {
        Self(df)
    }
}

pub struct ExonSessionContext {
    ctx: SessionContext,
    runtime: Runtime,
}

impl Default for ExonSessionContext {
    fn default() -> Self {
        Self {
            ctx: SessionContext::new_exon(),
            runtime: Runtime::new().unwrap(),
        }
    }
}

#[extendr]
impl ExonSessionContext {
    fn new() -> Result<Self> {
        Ok(Self::default())
    }

    fn sql(&mut self, query: &str) -> list::List {
        let df = match self.runtime.block_on(self.ctx.sql(query)).map_err(|e| {
            Error::from(format!(
                "Error executing query: {}\n{}",
                query,
                e.to_string()
            ))
        }) {
            Ok(df) => df,
            Err(e) => {
                return r_result_list::<RDataFrame, Error>(Err(e));
            }
        };

        let rdf = RDataFrame::from(df);

        r_result_list::<RDataFrame, Error>(Ok(rdf))
    }

    /// Eagerly execute a query and return the results.
    fn execute(&mut self, query: &str) -> list::List {
        self.runtime.block_on(async {
            let df = match self.ctx.sql(query).await.map_err(|e| {
                Error::from(format!(
                    "Error executing query: {}\n{}",
                    query,
                    e.to_string()
                ))
            }) {
                Ok(df) => df,
                Err(e) => {
                    return r_result_list::<(), Error>(Err(e));
                }
            };

            match df.collect().await {
                Ok(_) => r_result_list::<(), Error>(Ok(())),
                Err(e) => r_result_list::<(), Error>(Err(Error::Other(e.to_string()))),
            }
        })
    }
}

extendr_module! {
    mod exonr;

    fn read_inferred_exon_table;

    impl ExonSessionContext;
    impl RDataFrame;
}
