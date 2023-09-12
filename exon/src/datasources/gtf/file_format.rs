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

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

pub fn schema() -> SchemaRef {
    let attribute_key_field = Field::new("keys", DataType::Utf8, false);
    let attribute_value_field = Field::new("values", DataType::Utf8, true);

    let inner = Schema::new(vec![
        // https://useast.ensembl.org/info/website/upload/gff.html
        Field::new("seqname", DataType::Utf8, false),
        Field::new("source", DataType::Utf8, true),
        Field::new("type", DataType::Utf8, false),
        Field::new("start", DataType::Int64, false),
        Field::new("end", DataType::Int64, false),
        Field::new("score", DataType::Float32, true),
        Field::new("strand", DataType::Utf8, false),
        Field::new("frame", DataType::Utf8, true),
        Field::new_map(
            "attributes",
            "entries",
            attribute_key_field,
            attribute_value_field,
            false,
            true,
        ),
    ]);

    inner.into()
}

// #[cfg(test)]
// mod tests {
//     use std::sync::Arc;

//     use super::GTFFormat;
//     use datafusion::{
//         datasource::listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
//         prelude::SessionContext,
//     };

//     #[tokio::test]
//     async fn test_listing() {
//         let ctx = SessionContext::new();
//         let session_state = ctx.state();

//         let table_path = ListingTableUrl::parse("test-data").unwrap();

//         let gtf_format = Arc::new(GTFFormat::default());
//         let lo = ListingOptions::new(gtf_format.clone()).with_file_extension("gtf");

//         let resolved_schema = lo.infer_schema(&session_state, &table_path).await.unwrap();

//         let config = ListingTableConfig::new(table_path)
//             .with_listing_options(lo)
//             .with_schema(resolved_schema);

//         let provider = Arc::new(ListingTable::try_new(config).unwrap());
//         let df = ctx.read_table(provider.clone()).unwrap();

//         let mut row_cnt = 0;
//         let bs = df.collect().await.unwrap();
//         for batch in bs {
//             row_cnt += batch.num_rows();
//         }
//         assert_eq!(row_cnt, 77)
//     }
// }
