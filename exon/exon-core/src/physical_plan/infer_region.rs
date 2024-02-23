use std::str::FromStr;

use datafusion::logical_expr::{expr::ScalarFunction, Expr};
use noodles::core::Region;

pub(crate) fn infer_region_from_udf(scalar_udf: &ScalarFunction, name: &str) -> Option<Region> {
    if scalar_udf.name() == name {
        match &scalar_udf.args[0] {
            Expr::Literal(l) => {
                let region_str = l.to_string();
                let region = Region::from_str(region_str.as_str()).ok()?;
                Some(region)
            }
            _ => None,
        }
    } else {
        None
    }
}
