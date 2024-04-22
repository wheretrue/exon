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

use std::sync::Arc;

use arrow::{
    array::{
        ArrayBuilder, ArrayRef, Float64Builder, GenericListBuilder, GenericStringBuilder,
        Int64Builder, ListBuilder, StructBuilder,
    },
    datatypes::{DataType, Field, Fields},
};

use crate::mzml_reader::{
    BinaryDataArray, BinaryDataType, CompressionType, DataType as MzDataType, Spectrum,
    FLOAT_32_DATA_TYPE_MS_NUMBER, FLOAT_64_DATA_TYPE_MS_NUMBER, INTENSITY_ARRAY, MZ_ARRAY,
    NO_COMPRESSION_MS_NUMBER, WAVE_LENGTH_ARRAY, ZLIB_COMPRESSION_MS_NUMBER,
};

// https://github.com/wfondrie/depthcharge/blob/d46adf12deba06fb5d1019eb6e7a2ff621bfb388/depthcharge/data/parsers.py#L253

use super::mzml_reader::binary_conversion::decode_binary_array;

pub struct MzMLArrayBuilder {
    id: GenericStringBuilder<i32>,

    // data array is a map of strings to struct, where the struct is a single f64 array
    mz: StructBuilder,

    intensity: StructBuilder,
    wavelength: StructBuilder,

    // cv_params: MapBuilder<GenericStringBuilder<i32>, StructBuilder>,
    cv_params: GenericListBuilder<i32, StructBuilder>,

    precursor_mz: Float64Builder,
    precursor_charge: Int64Builder,
}

impl MzMLArrayBuilder {
    pub fn new() -> Self {
        let mz_fields = Fields::from(vec![Field::new(
            "mz",
            DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
            true,
        )]);

        let mz_array_builder =
            GenericListBuilder::<i32, Float64Builder>::new(Float64Builder::new());

        let intensity_fields = Fields::from(vec![Field::new(
            "intensity",
            DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
            true,
        )]);

        let intensity_array_builder =
            GenericListBuilder::<i32, Float64Builder>::new(Float64Builder::new());

        let wavelength_fields = Fields::from(vec![Field::new(
            "wavelength",
            DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
            true,
        )]);

        let wavelength_array_builder =
            GenericListBuilder::<i32, Float64Builder>::new(Float64Builder::new());

        let mz_builder = StructBuilder::new(mz_fields, vec![Box::new(mz_array_builder)]);
        let intensity_builder =
            StructBuilder::new(intensity_fields, vec![Box::new(intensity_array_builder)]);
        let wavelength_builder =
            StructBuilder::new(wavelength_fields, vec![Box::new(wavelength_array_builder)]);

        let cv_param_builder = StructBuilder::new(
            Fields::from(vec![
                Field::new("accession", DataType::Utf8, true),
                Field::new("name", DataType::Utf8, true),
                Field::new("value", DataType::Utf8, true),
            ]),
            vec![
                Box::new(GenericStringBuilder::<i32>::new()),
                Box::new(GenericStringBuilder::<i32>::new()),
                Box::new(GenericStringBuilder::<i32>::new()),
            ],
        );

        let cv_params_builder = GenericListBuilder::new(cv_param_builder);

        let precursor_mz = Float64Builder::new();

        Self {
            id: GenericStringBuilder::<i32>::new(),

            mz: mz_builder,
            intensity: intensity_builder,
            wavelength: wavelength_builder,
            cv_params: cv_params_builder,
            precursor_mz,
            precursor_charge: Int64Builder::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.id.len()
    }

    fn append_data_arrays_none_content(
        &mut self,
        binary_array_type: Option<BinaryDataType>,
    ) -> std::io::Result<()> {
        match binary_array_type {
            Some(BinaryDataType::Mz) => {
                let mz_builder = self
                    .mz
                    .field_builder::<ListBuilder<Float64Builder>>(0)
                    .unwrap();

                mz_builder.append_null();
                self.mz.append(true);
            }
            Some(BinaryDataType::Intensity) => {
                let intensity_builder = self
                    .intensity
                    .field_builder::<ListBuilder<Float64Builder>>(0)
                    .unwrap();

                intensity_builder.append_null();
                self.intensity.append(true);
            }
            Some(BinaryDataType::Wavelength) => {
                let wavelength_builder = self
                    .wavelength
                    .field_builder::<ListBuilder<Float64Builder>>(0)
                    .unwrap();

                wavelength_builder.append_null();
                self.wavelength.append(true);
            }
            None => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "No binary array type found",
                ))
            }
        }

        Ok(())
    }

    /// Handles the append case where there is a binary data array.
    fn append_data_arrays_some_content(
        &mut self,
        compression_type: Option<CompressionType>,
        data_type: Option<MzDataType>,
        binary_array_type: Option<BinaryDataType>,
        mz: &BinaryDataArray,
    ) -> std::io::Result<()> {
        let data_array = match (compression_type, data_type) {
            (Some(compression), Some(data_type)) => {
                decode_binary_array(&mz.binary, &compression, &data_type)?
            }
            _ => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "No compression or data type found",
                ))
            }
        };

        match binary_array_type {
            Some(BinaryDataType::Mz) => {
                let mz_builder = self
                    .mz
                    .field_builder::<ListBuilder<Float64Builder>>(0)
                    .unwrap();

                let mz_values = mz_builder.values();
                for value in data_array {
                    mz_values.append_value(value);
                }

                mz_builder.append(true);

                self.mz.append(true);
            }
            Some(BinaryDataType::Intensity) => {
                let intensity_builder = self
                    .intensity
                    .field_builder::<ListBuilder<Float64Builder>>(0)
                    .unwrap();

                let intensity_values = intensity_builder.values();
                for value in data_array {
                    intensity_values.append_value(value);
                }

                intensity_builder.append(true);

                self.intensity.append(true);
            }
            Some(BinaryDataType::Wavelength) => {
                let wavelength_builder = self
                    .wavelength
                    .field_builder::<ListBuilder<Float64Builder>>(0)
                    .unwrap();

                let wavelength_values = wavelength_builder.values();
                for value in data_array {
                    wavelength_values.append_value(value);
                }

                wavelength_builder.append(true);

                self.wavelength.append(true);
            }
            None => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "No binary array type found",
                ))
            }
        }

        Ok(())
    }

    /// Append the data arrays to the arrow array batch builder.
    fn append_data_arrays(&mut self, record: &Spectrum) -> std::io::Result<()> {
        for mz in &record.binary_data_array_list.binary_data_array {
            let mut binary_array_type = None;
            let mut compression_type = None;
            let mut data_type = None;

            let cv_params = &mz.cv_param;

            for cv_param in cv_params {
                match cv_param.accession.as_str() {
                    MZ_ARRAY | INTENSITY_ARRAY | WAVE_LENGTH_ARRAY => {
                        binary_array_type = Some(
                            BinaryDataType::try_from(cv_param.accession.as_str()).map_err(|e| {
                                std::io::Error::new(
                                    std::io::ErrorKind::InvalidData,
                                    format!("Invalid binary array type: {e}"),
                                )
                            })?,
                        );
                    }
                    ZLIB_COMPRESSION_MS_NUMBER | NO_COMPRESSION_MS_NUMBER => {
                        compression_type =
                            Some(CompressionType::try_from(cv_params).map_err(|e| {
                                std::io::Error::new(
                                    std::io::ErrorKind::InvalidData,
                                    format!("Invalid compression type: {e}"),
                                )
                            })?);
                    }
                    FLOAT_32_DATA_TYPE_MS_NUMBER | FLOAT_64_DATA_TYPE_MS_NUMBER => {
                        data_type = Some(MzDataType::try_from(cv_params).map_err(|e| {
                            std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                format!("Invalid data type: {e}"),
                            )
                        })?);
                    }
                    _ => {}
                }
            }

            match &mz.binary.content {
                Some(_) => {
                    self.append_data_arrays_some_content(
                        compression_type,
                        data_type,
                        binary_array_type,
                        mz,
                    )?;
                }
                None => {
                    self.append_data_arrays_none_content(binary_array_type)?;
                }
            };
        }

        // We may not see a certain array type in the data array list, so if not, append null to main equilength arrays.
        if self.id.len() != self.mz.len() {
            let mz_builder = self
                .mz
                .field_builder::<ListBuilder<Float64Builder>>(0)
                .unwrap();

            mz_builder.append_null();
            self.mz.append_null();
        }

        if self.id.len() != self.intensity.len() {
            let intensity_builder = self
                .intensity
                .field_builder::<ListBuilder<Float64Builder>>(0)
                .unwrap();

            intensity_builder.append_null();
            self.intensity.append_null();
        }

        if self.id.len() != self.wavelength.len() {
            let wavelength_builder = self
                .wavelength
                .field_builder::<ListBuilder<Float64Builder>>(0)
                .unwrap();

            wavelength_builder.append_null();
            self.wavelength.append_null();
        }

        Ok(())
    }

    pub fn append(&mut self, record: &Spectrum) -> std::io::Result<()> {
        self.id.append_value(&record.id);

        for cv_param in &record.cv_param {
            self.cv_params
                .values()
                .field_builder::<GenericStringBuilder<i32>>(0)
                .unwrap()
                .append_value(&cv_param.accession);

            self.cv_params
                .values()
                .field_builder::<GenericStringBuilder<i32>>(1)
                .unwrap()
                .append_value(&cv_param.name);

            self.cv_params
                .values()
                .field_builder::<GenericStringBuilder<i32>>(2)
                .unwrap()
                .append_option(cv_param.value.as_ref().and_then(|v| {
                    if v.is_empty() {
                        None
                    } else {
                        Some(v.to_string())
                    }
                }));

            self.cv_params.values().append(true);
        }
        self.cv_params.append(true);

        self.append_data_arrays(record)?;

        match &record.precursor_list {
            Some(precursor_list) => {
                let precursor = &precursor_list.precursor[0];
                let selected_ion = &precursor.selected_ion_list.selected_ion[0];

                let selected_ion_mz = selected_ion.cv_param.iter().find_map(|f| {
                    if f.accession == "MS:1000744" {
                        if let Some(value) = &f.value {
                            let string_value = value.to_string();
                            let float_value = string_value.parse::<f64>().unwrap();

                            Some(float_value)
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                });

                if let Some(selected_ion_mz) = selected_ion_mz {
                    self.precursor_mz.append_value(selected_ion_mz);
                } else {
                    self.precursor_mz.append_null();
                }

                let charge_state = selected_ion.cv_param.iter().find_map(|f| {
                    if f.accession == "MS:1000041" {
                        if let Some(value) = &f.value {
                            Some(value)
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                });

                if let Some(charge_state) = charge_state {
                    let charge_state = charge_state.parse::<i64>().unwrap();
                    self.precursor_charge.append_value(charge_state);
                } else {
                    self.precursor_charge.append_null();
                }
            }
            None => {
                self.precursor_mz.append_null();
                self.precursor_charge.append_null();
            }
        }

        Ok(())
    }

    pub fn finish(&mut self) -> Vec<ArrayRef> {
        let id = self.id.finish();
        let mz = self.mz.finish();
        let intensity = self.intensity.finish();
        let wavelength = self.wavelength.finish();

        let cv_params = self.cv_params.finish();

        let precursor_mz = self.precursor_mz.finish();
        let precursor_charge = self.precursor_charge.finish();

        vec![
            Arc::new(id),
            Arc::new(mz),
            Arc::new(intensity),
            Arc::new(wavelength),
            Arc::new(cv_params),
            Arc::new(precursor_mz),
            Arc::new(precursor_charge),
        ]
    }
}
