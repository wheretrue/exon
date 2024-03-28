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

use std::{any::Any, sync::Arc, vec};

use arrow::{
    array::{Array, Float32Builder},
    datatypes::DataType,
};
use async_trait::async_trait;
use datafusion::{
    common::cast::as_string_array,
    execution::{
        config::SessionConfig,
        context::{FunctionFactory, RegisterFunction},
    },
    logical_expr::{ColumnarValue, CreateFunction, ScalarUDFImpl, Signature, Volatility},
};
use lightmotif::{CountMatrix, Dna, EncodedSequence, ScoringMatrix};

#[derive(Default, Debug)]
pub struct LightMotifFunctionFactory {}

#[async_trait]
impl FunctionFactory for LightMotifFunctionFactory {
    async fn create(
        &self,
        state: &SessionConfig,
        statement: CreateFunction,
    ) -> datafusion::error::Result<RegisterFunction> {
        let signature = Signature::exact(vec![DataType::Utf8], Volatility::Immutable);

        let udf = LightMotifUDF::new("lightmotif", signature);

        Ok(RegisterFunction::Scalar(Arc::new(udf.into())))
    }
}

#[derive(Debug)]
struct LightMotifUDF {
    name: String,
    signature: Signature,
    pssm: ScoringMatrix<Dna>,
}

impl LightMotifUDF {
    pub fn new(name: &str, signature: Signature) -> Self {
        let counts = CountMatrix::<Dna>::from_sequences(
            ["GTTGACCTTATCAAC", "GTTGATCCAGTCAAC"]
                .into_iter()
                .map(|s| EncodedSequence::encode(s).unwrap()),
        )
        .unwrap();

        // Create a PSSM with 0.1 pseudocounts and uniform background frequencies.
        let pssm = counts.to_freq(0.1).to_scoring(None);

        Self {
            name: name.to_string(),
            signature,
            pssm,
        }
    }
}

impl ScalarUDFImpl for LightMotifUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Float32)
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn invoke(&self, args: &[ColumnarValue]) -> datafusion::error::Result<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(args)?;

        if args.len() != 1 {
            return Err(datafusion::error::DataFusionError::Execution(
                "LightMotif UDFs require exactly one argument".to_string(),
            ));
        }

        let sequence = as_string_array(args[0].as_ref())?;

        let mut float_builder = Float32Builder::with_capacity(sequence.len());

        for s in sequence.into_iter() {
            if let Some(s) = s {
                let encoded_sequence = EncodedSequence::encode(&s).unwrap();
                let mut stripped = encoded_sequence.to_striped();

                stripped.configure(&self.pssm);

                let scores = self.pssm.score(&stripped).to_vec();
                float_builder.append_value(scores[0]);
            } else {
                float_builder.append_null();
            }
        }

        let float_builder = float_builder.finish();
        Ok(ColumnarValue::Array(Arc::new(float_builder)))
    }
}

#[cfg(test)]
mod tests {
    use crate::session_context::ExonSessionExt;
    use datafusion::execution::context::SessionContext;

    #[tokio::test]
    async fn test_udf() -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new_exon();

        let sql = r#"
        CREATE FUNCTION lightmotif(VARCHAR)
        RETURNS FLOAT
        "#;

        ctx.sql(sql).await?;

        let df = ctx.sql("SELECT lightmotif('GTTGACCTTATCAAC')").await?;
        df.show().await?;

        // let results = df.collect().await?;

        // assert_eq!(results.len(), 1);

        // eprintln!("{:?}", results);

        Ok(())
    }
}
