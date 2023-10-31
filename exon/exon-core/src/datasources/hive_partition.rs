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

use arrow::datatypes::DataType;
use datafusion::{
    logical_expr::{BinaryExpr, Operator, TableProviderFilterPushDown},
    prelude::Expr,
};

/// Returns the extent to which a filter expression can be pushed down to a table provider's partition columns.
///
/// # Arguments
///
/// * `expr` - The filter expression to check.
/// * `table_partition_cols` - The partition columns of the table provider.
///
/// # Returns
///
/// * `TableProviderFilterPushDown::Exact` if the filter expression can be pushed down exactly to the table provider's partition columns.
/// * `TableProviderFilterPushDown::Unsupported` if the filter expression cannot be pushed down to the table provider's partition columns.
pub(crate) fn filter_matches_partition_cols(
    expr: &&Expr,
    table_partition_cols: &[(String, DataType)],
) -> TableProviderFilterPushDown {
    if let Expr::BinaryExpr(BinaryExpr { left, op, right }) = expr {
        if *op == Operator::Eq {
            if let Expr::Column(c) = &**left {
                if let Expr::Literal(_) = &**right {
                    let name = &c.name;

                    if table_partition_cols.iter().any(|(n, _)| n == name) {
                        return TableProviderFilterPushDown::Exact;
                    } else {
                        return TableProviderFilterPushDown::Unsupported;
                    }
                }
            }
        }
    }

    TableProviderFilterPushDown::Unsupported
}
