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

//     use crate::tests::test_listing_table_url;

//     use super::HMMDomTabFormat;
//     use datafusion::{
//         datasource::listing::{ListingOptions, ListingTable, ListingTableConfig},
//         prelude::SessionContext,
//     };

//     #[tokio::test]
//     async fn test_listing() {
//         let ctx = SessionContext::new();
//         let session_state = ctx.state();

//         let table_path = test_listing_table_url("hmmdomtab");

//         let hmm_format = Arc::new(HMMDomTabFormat::default());
//         let lo = ListingOptions::new(hmm_format.clone()).with_file_extension(".hmmdomtab");

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
//         assert_eq!(row_cnt, 100)
//     }
// }
