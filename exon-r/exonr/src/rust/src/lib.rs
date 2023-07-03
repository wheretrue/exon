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

use datafusion::{arrow::ffi_stream::FFI_ArrowArrayStream, prelude::SessionContext};
use exon::{context::ExonSessionExt, ffi::create_dataset_stream_from_table_provider};
use extendr_api::prelude::*;
use object_store::{aws::AmazonS3Builder, gcp::GoogleCloudStorageBuilder};
use url::Url;

fn add_s3_filesystem(ctx: &mut SessionContext, path: &str) -> std::io::Result<()> {
    let url = Url::parse(path).map_err(|e| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("Invalid URL: {e}"),
        )
    })?;

    let host_str = url.host_str().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("Invalid URL: {path}"),
        )
    })?;

    let bucket_address = Url::parse(format!("s3://{host_str}").as_str()).map_err(|e| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("Invalid URL: {e}"),
        )
    })?;

    let s3 = AmazonS3Builder::from_env()
        .with_bucket_name(host_str)
        .build()?;

    ctx.runtime_env()
        .register_object_store(&bucket_address, Arc::new(s3));

    Ok(())
}

fn add_google_filesystem(ctx: &mut SessionContext, path: &str) -> std::io::Result<()> {
    let url = Url::parse(path).map_err(|e| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("Invalid URL: {e}"),
        )
    })?;

    let host_str = url.host_str().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("Invalid URL: {path}"),
        )
    })?;

    let bucket_address = Url::parse(format!("gs://{host_str}").as_str()).map_err(|e| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("Invalid URL: {e}"),
        )
    })?;

    let gcs = GoogleCloudStorageBuilder::from_env()
        .with_bucket_name(host_str)
        .build()?;

    ctx.runtime_env()
        .register_object_store(&bucket_address, Arc::new(gcs));

    Ok(())
}

fn read_inferred_exon_table_inner(
    path: &str,
    stream_ptr: *mut FFI_ArrowArrayStream,
) -> std::io::Result<()> {
    let rt = Arc::new(tokio::runtime::Runtime::new().unwrap());

    let mut ctx = SessionContext::new();

    if path.starts_with("gs://") {
        add_google_filesystem(&mut ctx, path).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Error adding Google Cloud Storage: {e}"),
            )
        })?;
    }

    if path.starts_with("s3://") {
        add_s3_filesystem(&mut ctx, path).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Error adding Amazon S3: {e}"),
            )
        })?;
    }

    rt.block_on(async {
        let df = ctx.read_inferred_exon_table(path).await.unwrap();

        create_dataset_stream_from_table_provider(df, rt.clone(), stream_ptr)
            .await
            .unwrap();
    });

    Ok(())
}

/// Copy the inferred exon table from the given path into the given stream.
/// @export
#[extendr]
fn read_inferred_exon_table(file_path: &str, stream_ptr: &str) {
    let stream_out_ptr_addr: usize = stream_ptr.parse().unwrap();

    let stream_out_ptr = stream_out_ptr_addr as *mut FFI_ArrowArrayStream;

    // TODO: Handle errors
    read_inferred_exon_table_inner(file_path, stream_out_ptr).unwrap();
}

extendr_module! {
    mod exonr;

    fn read_inferred_exon_table;
}
