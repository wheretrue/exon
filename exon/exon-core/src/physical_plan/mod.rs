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

/// A physical expression that represents the name of a region.
pub mod region_name_physical_expr;

/// A physical expression that represents a genomic interval.
pub mod pos_interval_physical_expr;

/// A physical expression that represents a region, e.g. chr1:100-200.
pub mod region_physical_expr;

/// Utilities for working with object stores.
pub mod object_store;

/// Builder for a file scan configuration.
pub mod file_scan_config_builder;

/// A physical expression that represents start/end interval
pub mod start_end_interval_physical_expr;

/// A region expression that builds off of a start/end interval.
pub mod start_end_region_physical_expr;

/// A custom PhysicalPlanner that adds Exon-specific functionality.
pub mod planner;

/// A macro for extracting the region from a UDF.
pub mod infer_region;
