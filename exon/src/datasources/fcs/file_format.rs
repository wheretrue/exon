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

use std::{any::Any, sync::Arc};

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::{
    datasource::{
        file_format::{file_type::FileCompressionType, FileFormat},
        physical_plan::FileScanConfig,
    },
    error::DataFusionError,
    execution::context::SessionState,
    physical_plan::{ExecutionPlan, PhysicalExpr, Statistics},
};
use futures::TryStreamExt;
use object_store::{ObjectMeta, ObjectStore};
use tokio_util::io::StreamReader;

use super::{reader::FcsReader, scanner::FCSScan};

#[derive(Debug)]
/// Implements a datafusion `FileFormat` for FCS files.
pub struct FCSFormat {
    /// The compression type of the file.
    file_compression_type: FileCompressionType,
}

impl Default for FCSFormat {
    fn default() -> Self {
        Self {
            file_compression_type: FileCompressionType::UNCOMPRESSED,
        }
    }
}

#[async_trait]
impl FileFormat for FCSFormat {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn infer_schema(
        &self,
        _state: &SessionState,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> datafusion::error::Result<SchemaRef> {
        let get_result = store.get(&objects[0].location).await?;

        let stream_reader = Box::pin(get_result.into_stream().map_err(DataFusionError::from));
        let stream_reader = StreamReader::new(stream_reader);

        let fcs_reader = FcsReader::new(stream_reader).await?;
        let parameter_names = fcs_reader.text_data.parameter_names();

        // Create the schema assuming all columns are of type `Float32`
        let mut fields = Vec::with_capacity(parameter_names.len());
        for parameter_name in parameter_names {
            fields.push(arrow::datatypes::Field::new(
                parameter_name,
                arrow::datatypes::DataType::Float32,
                false,
            ));
        }

        let schema = Arc::new(arrow::datatypes::Schema::new(fields));

        Ok(schema)
    }

    async fn infer_stats(
        &self,
        _state: &SessionState,
        _store: &Arc<dyn ObjectStore>,
        _table_schema: SchemaRef,
        _object: &ObjectMeta,
    ) -> datafusion::error::Result<Statistics> {
        Ok(Statistics::default())
    }

    async fn create_physical_plan(
        &self,
        _state: &SessionState,
        conf: FileScanConfig,
        _filters: Option<&Arc<dyn PhysicalExpr>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let scan = FCSScan::new(conf, self.file_compression_type.clone());
        Ok(Arc::new(scan))
    }
}
