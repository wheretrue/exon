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

use std::str::FromStr;

use async_trait::async_trait;
use datafusion::{
    execution::context::{FunctionFactory, RegisterFunction, SessionState},
    logical_expr::CreateFunction,
};

use crate::error::ExonError;

#[cfg(feature = "motif-udf")]
use crate::udfs::sequence::motif::create_pssm_function;

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

        let function = ExonFunctions::from_str(name.as_str())?;

        match function {
            #[cfg(feature = "motif-udf")]
            ExonFunctions::Pssm => {
                let pssm = create_pssm_function(state, &name, &params, &args).await?;
                Ok(RegisterFunction::Scalar(Arc::new(pssm.into())))
            }
            #[allow(unreachable_patterns)]
            _ => Err(ExonError::UnsupportedFunction(name.clone()).into()),
        }
    }
}

pub enum ExonFunctions {
    Pssm,
}

impl FromStr for ExonFunctions {
    type Err = ExonError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "pssm" => Ok(Self::Pssm),
            _ => Err(ExonError::UnsupportedFunction(s.to_string())),
        }
    }
}
