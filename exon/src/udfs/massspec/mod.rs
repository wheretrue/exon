mod bin_vectors;
mod contains_peak;

use datafusion::logical_expr::ScalarUDF;

/// Returns a vector of ScalarUDFs from the sequence module.
pub fn register_udfs() -> Vec<ScalarUDF> {
    vec![
        bin_vectors::bin_vectors_udf(),
        contains_peak::contains_peak_udf(),
    ]
}
