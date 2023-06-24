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
use datafusion::prelude::*;
use exon::context::ExonSessionExt;

#[tokio::main]
async fn main() -> Result<(), DataFusionError> {
    let ctx = SessionContext::new_exon();

    let path = "./exon-examples/data/Ga0604745_crt.gff";
    let sql = format!(
        "CREATE EXTERNAL TABLE gff STORED AS GFF LOCATION '{}';",
        path
    );

    ctx.sql(&sql).await?;

    let df = ctx
        .sql(
            r#"SELECT crispr.seqid, crispr.start, crispr.end, repeat.start, repeat.end
            FROM (SELECT * FROM gff WHERE type = 'CRISPR') AS crispr
                JOIN (SELECT * FROM gff WHERE type = 'repeat_unit') AS repeat
                    ON crispr.seqid = repeat.seqid
                    AND crispr.start <= repeat.start
                    AND crispr.end >= repeat.end

            ORDER BY crispr.seqid, crispr.start, crispr.end, repeat.start, repeat.end
            LIMIT 10"#,
        )
        .await?;

    // Show the logical plan.
    let logical_plan = df.logical_plan();
    assert_eq!(
        format!("\n{:?}", logical_plan),
        r#"
Limit: skip=0, fetch=10
  Sort: crispr.seqid ASC NULLS LAST, crispr.start ASC NULLS LAST, crispr.end ASC NULLS LAST, repeat.start ASC NULLS LAST, repeat.end ASC NULLS LAST
    Projection: crispr.seqid, crispr.start, crispr.end, repeat.start, repeat.end
      Inner Join:  Filter: crispr.seqid = repeat.seqid AND crispr.start <= repeat.start AND crispr.end >= repeat.end
        SubqueryAlias: crispr
          Projection: gff.seqid, gff.source, gff.type, gff.start, gff.end, gff.score, gff.strand, gff.phase, gff.attributes
            Filter: gff.type = Utf8("CRISPR")
              TableScan: gff
        SubqueryAlias: repeat
          Projection: gff.seqid, gff.source, gff.type, gff.start, gff.end, gff.score, gff.strand, gff.phase, gff.attributes
            Filter: gff.type = Utf8("repeat_unit")
              TableScan: gff"#,
    );

    // Uncomment to show the physical plan, though it's obviously messier.
    // let physical_plan = df.create_physical_plan().await?;
    // println!("Physical plan: {:?}", physical_plan);

    // Collect the results as Arrow RecordBatches.
    let results = df.collect().await?;
    let formatted_results = pretty_format_batches(results.as_slice())?;
    assert_eq!(
        format!("\n{}", formatted_results),
        r#"
+------------------+-------+------+-------+-----+
| seqid            | start | end  | start | end |
+------------------+-------+------+-------+-----+
| Ga0604745_000026 | 1     | 3473 | 1     | 37  |
| Ga0604745_000026 | 1     | 3473 | 73    | 109 |
| Ga0604745_000026 | 1     | 3473 | 147   | 183 |
| Ga0604745_000026 | 1     | 3473 | 219   | 255 |
| Ga0604745_000026 | 1     | 3473 | 291   | 327 |
| Ga0604745_000026 | 1     | 3473 | 365   | 401 |
| Ga0604745_000026 | 1     | 3473 | 437   | 473 |
| Ga0604745_000026 | 1     | 3473 | 510   | 546 |
| Ga0604745_000026 | 1     | 3473 | 582   | 618 |
| Ga0604745_000026 | 1     | 3473 | 654   | 690 |
+------------------+-------+------+-------+-----+"#
    );

    Ok(())
}
