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

//! This example shows how to load a GFF, and do a self join in order to find
//! repeat units within a given CRISPR array.

use arrow::util::pretty::pretty_format_batches;
use datafusion::error::DataFusionError;
use exon::ExonSession;

#[tokio::main]
async fn main() -> Result<(), DataFusionError> {
    let ctx = ExonSession::new_exon()?;

    // From GNPS, create a table.
    let path = "./exon-examples/data/GNPS00002_A3_p.mzML";
    let sql = format!("CREATE EXTERNAL TABLE mzml STORED AS MZML LOCATION '{path}';",);
    ctx.session.sql(&sql).await?;

    // Query the table, select the scan id where the spectrum contains a peak of interest.
    let df = ctx
        .session
        .sql(
            r#"SELECT id
            FROM mzml
            WHERE contains_peak(mz.mz, 100.0, 0.1) = true
            "#,
        )
        .await?;

    let batches = df.collect().await?;
    let formatted = pretty_format_batches(&batches)?;
    println!("{}", formatted);

    Ok(())
}
