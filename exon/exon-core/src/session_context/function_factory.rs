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

use async_trait::async_trait;
use datafusion::{
    execution::context::{FunctionFactory, RegisterFunction, SessionState},
    logical_expr::CreateFunction,
};

use crate::error::ExonError;

#[derive(Default, Debug)]
pub struct ExonFunctionFactory {}

#[async_trait]
impl FunctionFactory for ExonFunctionFactory {
    async fn create(
        &self,
        _state: &SessionState,
        statement: CreateFunction,
    ) -> datafusion::error::Result<RegisterFunction> {
        let CreateFunction {
            temporary: _,
            name,
            args: _,
            return_type: _,
            params: _,
            schema: _,
            or_replace: _,
        } = statement;

        Err(ExonError::UnsupportedFunction(name.clone()).into())
    }
}
