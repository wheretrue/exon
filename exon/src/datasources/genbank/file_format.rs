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

use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};

pub fn schema() -> SchemaRef {
    let kind_field = Field::new("kind", DataType::Utf8, false);
    let location_field = Field::new("location", DataType::Utf8, false);

    let qualifier_key_field = Field::new("keys", DataType::Utf8, false);
    let qualifier_value_field = Field::new("values", DataType::Utf8, true);
    let qualifiers_field = Field::new_map(
        "qualifiers",
        "entries",
        qualifier_key_field,
        qualifier_value_field,
        false,
        true,
    );

    let fields = Fields::from(vec![kind_field, location_field, qualifiers_field]);
    let feature_field = Field::new("item", DataType::Struct(fields), true);

    let comment_field = Field::new("item", DataType::Utf8, true);

    let schema = Schema::new(vec![
        Field::new("sequence", DataType::Utf8, false),
        Field::new("accession", DataType::Utf8, true),
        Field::new("comments", DataType::List(Arc::new(comment_field)), true),
        Field::new("contig", DataType::Utf8, true),
        Field::new("date", DataType::Utf8, true),
        Field::new("dblink", DataType::Utf8, true),
        Field::new("definition", DataType::Utf8, true),
        Field::new("division", DataType::Utf8, false),
        Field::new("keywords", DataType::Utf8, true),
        Field::new("molecule_type", DataType::Utf8, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("source", DataType::Utf8, true),
        Field::new("version", DataType::Utf8, true),
        Field::new("topology", DataType::Utf8, false),
        Field::new("features", DataType::List(Arc::new(feature_field)), true),
    ]);

    Arc::new(schema)
}

// #[cfg(test)]
// mod tests {
//     use std::sync::Arc;

//     use crate::tests::test_listing_table_url;

//     use super::GenbankFormat;
//     use datafusion::{
//         datasource::listing::{ListingOptions, ListingTable, ListingTableConfig},
//         prelude::SessionContext,
//     };

//     #[tokio::test]
//     async fn test_listing() {
//         let ctx = SessionContext::new();
//         let session_state = ctx.state();

//         let table_path = test_listing_table_url("genbank/test.gb");

//         let genbank_format = Arc::new(GenbankFormat::default());
//         let lo = ListingOptions::new(genbank_format.clone());

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
//         assert_eq!(row_cnt, 1)
//     }
// }
