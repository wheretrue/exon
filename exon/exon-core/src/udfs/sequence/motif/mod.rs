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

use std::{any::Any, io::BufReader, str::FromStr, sync::Arc};

use arrow::{
    array::{Array, Float32Builder},
    datatypes::DataType,
};
use datafusion::{
    common::cast::as_string_array,
    datasource::listing::ListingTableUrl,
    execution::context::SessionState,
    logical_expr::{
        ColumnarValue, CreateFunctionBody, OperateFunctionArg, ScalarUDFImpl, Signature,
        TypeSignature,
    },
};
use lightmotif::*;
use pli::{Encode, Pipeline, Score, Stripe};

use crate::error::ExonError;

pub enum PSSMFormats {
    Jaspar16,
    Transfac,
    Uniprobe,
}

pub enum ExonAlphabet {
    Protein(lightmotif::Protein),
    Dna(lightmotif::Dna),
}

impl FromStr for PSSMFormats {
    type Err = ExonError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "jaspar16" => Ok(Self::Jaspar16),
            "transfac" => Ok(Self::Transfac),
            "uniprobe" => Ok(Self::Uniprobe),
            _ => Err(ExonError::UnsupportedFunction(
                "Unknown PSSM format".to_string(),
            )),
        }
    }
}

const DEFAULT_PSEUDO_COUNT: f32 = 0.01;

#[derive(Debug)]
pub enum ExonScoringMatrix {
    Protein(lightmotif::ScoringMatrix<lightmotif::Protein>),
    Dna(lightmotif::ScoringMatrix<lightmotif::Dna>),
}

#[derive(Debug)]
pub struct Pssmudf {
    name: String,
    signature: Signature,
    pssm: ExonScoringMatrix,
}

impl Pssmudf {
    pub fn new(name: &str, signature: Signature, pssm: ExonScoringMatrix) -> Self {
        Self {
            name: name.to_string(),
            signature,
            pssm,
        }
    }
}

impl ScalarUDFImpl for Pssmudf {
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
                "pssm takes exactly one argument".to_string(),
            ));
        }

        let sequence = as_string_array(args[0].as_ref())?;

        let mut float_builder = Float32Builder::with_capacity(sequence.len());

        for s in sequence.into_iter() {
            if let Some(s) = s {
                match &self.pssm {
                    ExonScoringMatrix::Protein(pssm) => {
                        let pli = Pipeline::dispatch();
                        let encoded_sequence = pli.encode(s).map_err(|e| {
                            datafusion::error::DataFusionError::Execution(format!(
                                "Error encoding sequence: {}",
                                e
                            ))
                        })?;

                        let mut stripped = pli.stripe(&encoded_sequence);
                        stripped.configure(pssm);

                        let scores = pli.score(&stripped, pssm).to_vec();
                        float_builder.append_value(scores[0]);
                    }
                    ExonScoringMatrix::Dna(pssm) => {
                        let pli = Pipeline::dispatch();
                        let encoded_sequence = pli.encode(s).map_err(|e| {
                            datafusion::error::DataFusionError::Execution(format!(
                                "Error encoding sequence: {}",
                                e
                            ))
                        })?;

                        let mut stripped = pli.stripe(&encoded_sequence);
                        stripped.configure(pssm);

                        let scores = pli.score(&stripped, pssm).to_vec();
                        float_builder.append_value(scores[0]);
                    }
                }
            } else {
                float_builder.append_null();
            }
        }

        let float_builder = float_builder.finish();
        Ok(ColumnarValue::Array(Arc::new(float_builder)))
    }
}

pub async fn create_pssm_function(
    state: &SessionState,
    name: &str,
    params: &CreateFunctionBody,
    args: &Option<Vec<OperateFunctionArg>>,
) -> Result<Pssmudf, ExonError> {
    let pssm_file = match &params.as_ {
        Some(DefinitionStatement::DoubleDollarDef(s)) => s,
        Some(DefinitionStatement::SingleQuotedDef(s)) => s,
        None => {
            return Err(ExonError::ExecutionError(
                "pssm function requires a PSSM file".to_string(),
            ))
        }
    };

    // Get the alphabet from the arguments, defaults to protein
    let alphabet = if let Some(args) = args.as_ref() {
        if args.len() != 1 {
            return Err(ExonError::ExecutionError(
                "pssm function requires exactly one argument".to_string(),
            ));
        }

        let arg = args.first().ok_or(ExonError::ExecutionError(
            "pssm function requires exactly one argument".to_string(),
        ))?;

        let name = arg.name.clone().ok_or(ExonError::ExecutionError(
            "pssm function requires named arguments".to_string(),
        ))?;

        if name.value == "DNA" {
            ExonAlphabet::Dna(Dna {})
        } else {
            ExonAlphabet::Protein(Protein {})
        }
    } else {
        ExonAlphabet::Protein(Protein {})
    };

    let table_listing_path = ListingTableUrl::parse(pssm_file)?;
    let store = state.runtime_env().object_store(&table_listing_path)?;

    let contents = store
        .get(table_listing_path.prefix())
        .await?
        .bytes()
        .await?;

    let cursor = std::io::Cursor::new(contents);
    let buf_reader = BufReader::new(cursor);

    let pssm_format = params
        .language
        .as_ref()
        .map(|s| s.value.as_str())
        .unwrap_or("jaspar16");

    let pssm_format = PSSMFormats::from_str(pssm_format)?;

    let pssm = match (alphabet, pssm_format) {
        (ExonAlphabet::Protein(_protein), PSSMFormats::Jaspar16) => {
            let record = lightmotif_io::jaspar16::Reader::<_, lightmotif::Protein>::new(buf_reader)
                .next()
                .ok_or(ExonError::ExecutionError(
                    "Error reading PSSM file".to_string(),
                ))?
                .map_err(|_| ExonError::ExecutionError("Error reading PSSM file".to_string()))?;

            let pssm = record
                .matrix()
                .to_freq(DEFAULT_PSEUDO_COUNT)
                .to_scoring(None);

            ExonScoringMatrix::Protein(pssm)
        }
        (ExonAlphabet::Dna(_dna), PSSMFormats::Transfac) => {
            let record = lightmotif_io::transfac::Reader::<_, lightmotif::Dna>::new(buf_reader)
                .next()
                .ok_or(ExonError::ExecutionError(
                    "Error reading PSSM file".to_string(),
                ))?
                .map_err(|_| ExonError::ExecutionError("Error reading PSSM file".to_string()))?;

            let pssm = record
                .to_counts()
                .ok_or(ExonError::ExecutionError(
                    "Error reading PSSM file".to_string(),
                ))?
                .to_freq(DEFAULT_PSEUDO_COUNT)
                .to_scoring(None);

            ExonScoringMatrix::Dna(pssm)
        }
        (ExonAlphabet::Dna(_dna), PSSMFormats::Jaspar16) => {
            let record = lightmotif_io::jaspar16::Reader::<_, lightmotif::Dna>::new(buf_reader)
                .next()
                .ok_or(ExonError::ExecutionError(
                    "Error reading PSSM file".to_string(),
                ))?
                .map_err(|_| ExonError::ExecutionError("Error reading PSSM file".to_string()))?;

            let pssm = record
                .matrix()
                .to_freq(DEFAULT_PSEUDO_COUNT)
                .to_scoring(None);

            ExonScoringMatrix::Dna(pssm)
        }
        _ => {
            return Err(ExonError::UnsupportedFunction(
                "Unsupported PSSM format".to_string(),
            ))
        }
    };

    let signature = Signature::new(
        TypeSignature::Exact(vec![DataType::Utf8]),
        datafusion::logical_expr::Volatility::Stable,
    );

    Ok(Pssmudf::new(name, signature, pssm))
}
