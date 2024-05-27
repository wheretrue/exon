// Copyright 2024 WHERE TRUE Technologies.
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

mod exon_data_sink_node;

use std::sync::Arc;

use datafusion::logical_expr::{Extension, LogicalPlan, UserDefinedLogicalNodeCore};
pub(crate) use exon_data_sink_node::ExonDataSinkLogicalPlanNode;

pub trait DfExtensionNode: Sized + UserDefinedLogicalNodeCore {
    fn into_extension(self) -> Extension {
        Extension {
            node: Arc::new(self),
        }
    }
}

pub enum ExonLogicalPlan {
    DataFusion(LogicalPlan),
    Exon(LogicalPlan),
}
