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

// #[cfg(test)]
// mod tests {
//     use std::sync::Arc;

//     use super::FASTQFormat;
//     use datafusion::{
//         datasource::listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
//         prelude::SessionContext,
//     };

//     #[tokio::test]
//     async fn test_schema_inference() {
//         let ctx = SessionContext::new();
//         let session_state = ctx.state();

//         let table_path = ListingTableUrl::parse("test-data").unwrap();

//         let fasta_format = Arc::new(FASTQFormat::default());
//         let lo = ListingOptions::new(fasta_format.clone()).with_file_extension("fastq");

//         let resolved_schema = lo.infer_schema(&session_state, &table_path).await.unwrap();

//         assert_eq!(resolved_schema.fields().len(), 4);

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
//         assert_eq!(row_cnt, 2)
//     }
// }
