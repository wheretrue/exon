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

use serde::{Deserialize, Serialize};

use byteorder::{LittleEndian, ReadBytesExt};

use base64::Engine;
use std::convert::TryFrom;
use std::error::Error;
use std::fmt;
use std::io::Cursor;

use super::binary_conversion;

#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub struct CVParam {
    #[serde(rename = "@cvRef")]
    pub cv_ref: String,

    #[serde(rename = "@accession")]
    pub accession: String,

    #[serde(rename = "@name")]
    pub name: String,

    #[serde(rename = "@value")]
    pub value: Option<String>,

    #[serde(rename = "@unitAccession")]
    pub unit_accession: Option<String>,

    #[serde(rename = "@unitName")]
    pub unit_name: Option<String>,

    #[serde(rename = "@unitCvRef")]
    pub unit_cv_ref: Option<String>,
}

/// The data type of the CVParam.
impl CVParam {
    /// Get the data type of the CVParam.
    #[allow(dead_code)]
    pub fn get_data_type(&self) -> Result<DataType, MissingDataTypeError> {
        let dt = DataType::try_from(self)?;
        Ok(dt)
    }

    /// Create a new CVParam.
    #[allow(dead_code)]
    pub fn new(
        cv_ref: String,
        accession: String,
        name: String,
        value: Option<String>,
        unit_accession: Option<String>,
        unit_name: Option<String>,
        unit_cv_ref: Option<String>,
    ) -> Self {
        Self {
            cv_ref,
            accession,
            name,
            value,
            unit_accession,
            unit_name,
            unit_cv_ref,
        }
    }
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct UserParam {
    pub name: String,
    pub value: String,
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ScanWindow {
    pub cv_param: CVVector,
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ScanWindowList {
    pub scan_window: Vec<ScanWindow>,

    #[serde(rename = "@count")]
    pub count: String,
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Scan {
    pub cv_param: CVVector,
    pub scan_window_list: Option<ScanWindowList>,
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ScanList {
    pub cv_param: CVVector,
    pub scan: Vec<Scan>,
}

pub(crate) const MZ_ARRAY: &str = "MS:1000514";
pub(crate) const INTENSITY_ARRAY: &str = "MS:1000515";
pub(crate) const WAVE_LENGTH_ARRAY: &str = "MS:1000617";

#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub enum BinaryDataType {
    Mz,
    Intensity,
    Wavelength,
}

impl TryFrom<&str> for BinaryDataType {
    type Error = MissingDataTypeError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            MZ_ARRAY => Ok(BinaryDataType::Mz),
            INTENSITY_ARRAY => Ok(BinaryDataType::Intensity),
            WAVE_LENGTH_ARRAY => Ok(BinaryDataType::Wavelength),
            _ => Err(MissingDataTypeError),
        }
    }
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub struct Binary {
    #[serde(rename = "$value")]
    pub content: Option<String>,
}

impl Binary {
    #[allow(dead_code)]
    pub fn new(content: Option<String>) -> Binary {
        Binary { content }
    }
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct BinaryDataArray {
    #[serde(rename = "@encodedLength")]
    pub encoded_length: String,
    pub cv_param: CVVector,
    pub binary: Binary,
}

impl BinaryDataArray {
    #[allow(dead_code)]
    pub fn binary_array_to_vector(&self) -> Result<Vec<f64>, std::io::Error> {
        let data_type = DataType::try_from(&self.cv_param).unwrap();
        let compression_type = CompressionType::try_from(&self.cv_param).unwrap();

        let result =
            binary_conversion::decode_binary_array(&self.binary, &compression_type, &data_type)?;

        Ok(result)
    }
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub enum DataType {
    Float64Bit,
    Float32Bit,
}

impl TryFrom<&CVVector> for DataType {
    type Error = MissingDataTypeError;

    fn try_from(value: &CVVector) -> Result<Self, Self::Error> {
        for cv_param in value.iter() {
            match DataType::try_from(cv_param) {
                Ok(data_type) => return Ok(data_type),
                Err(_) => continue,
            }
        }
        Err(MissingDataTypeError)
    }
}

#[derive(Debug, Clone)]
pub struct MissingDataTypeError;

impl fmt::Display for MissingDataTypeError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "invalid first item to double")
    }
}

pub(crate) const FLOAT_32_DATA_TYPE_MS_NUMBER: &str = "MS:1000521";
pub(crate) const FLOAT_64_DATA_TYPE_MS_NUMBER: &str = "MS:1000523";

impl Error for MissingDataTypeError {}

impl TryFrom<&CVParam> for DataType {
    type Error = MissingDataTypeError;

    fn try_from(value: &CVParam) -> Result<Self, Self::Error> {
        match value.accession.as_str() {
            FLOAT_32_DATA_TYPE_MS_NUMBER => Ok(DataType::Float32Bit),
            FLOAT_64_DATA_TYPE_MS_NUMBER => Ok(DataType::Float64Bit),
            _ => Err(MissingDataTypeError),
        }
    }
}

impl TryFrom<&str> for DataType {
    type Error = MissingDataTypeError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            FLOAT_32_DATA_TYPE_MS_NUMBER => Ok(DataType::Float32Bit),
            FLOAT_64_DATA_TYPE_MS_NUMBER => Ok(DataType::Float64Bit),
            _ => Err(MissingDataTypeError),
        }
    }
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub enum CompressionType {
    NoCompression,
    ZlibCompression,
}

#[derive(Debug, Clone)]
pub struct MissingCompressionError;

impl fmt::Display for MissingCompressionError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "missing compression in controlled vocabulary parameters")
    }
}

impl Error for MissingCompressionError {}

type CVVector = Vec<CVParam>;

trait CVVectorMethods {}

impl CVVectorMethods for CVVector {}

impl TryFrom<&CVVector> for CompressionType {
    type Error = MissingCompressionError;

    fn try_from(value: &CVVector) -> Result<Self, Self::Error> {
        for cv_param in value.iter() {
            match CompressionType::try_from(cv_param) {
                Ok(compression_type) => return Ok(compression_type),
                Err(_) => continue,
            };
        }
        Err(MissingCompressionError)
    }
}

pub(crate) const NO_COMPRESSION_MS_NUMBER: &str = "MS:1000576";
pub(crate) const ZLIB_COMPRESSION_MS_NUMBER: &str = "MS:1000574";

impl TryFrom<&CVParam> for CompressionType {
    type Error = MissingDataTypeError;

    fn try_from(value: &CVParam) -> Result<Self, Self::Error> {
        match value.accession.as_str() {
            "MS:1000576" => Ok(CompressionType::NoCompression),
            "MS:1000574" => Ok(CompressionType::ZlibCompression),
            _ => Err(MissingDataTypeError),
        }
    }
}

#[derive(Debug, Clone)]
pub struct DataArrayDecodingError;

impl fmt::Display for DataArrayDecodingError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "invalid first item to double")
    }
}

impl Error for DataArrayDecodingError {}
//https://docs.rs/fastobo/0.13.1/fastobo/ast/struct.OboDoc.html
//https://raw.githubusercontent.com/HUPO-PSI/psi-ms-CV/master/psi-ms.obo

#[derive(Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct BinaryDataArrayList {
    pub binary_data_array: Vec<BinaryDataArray>,

    #[serde(rename = "@count")]
    pub count: String,
}

#[allow(dead_code)]
type DecodeArrayError = &'static str;

#[allow(dead_code)]
type DecodedArrayResult<T> = Result<T, DecodeArrayError>;

pub trait DecodedArray {
    fn decode_array(&self, i: usize) -> DecodedArrayResult<Vec<f64>> {
        let de = self.decompress_binary_string(i).unwrap();
        let decoded = base64::engine::general_purpose::STANDARD.decode(de);

        if let Ok(v) = decoded {
            let mut rdr = Cursor::new(v);

            let mut peaks = Vec::<f64>::new();
            while let Ok(fl) = rdr.read_f64::<LittleEndian>() {
                peaks.push(fl);
            }
            return Ok(peaks);
        };

        Err("error")
    }

    fn decompress_binary_string(&self, i: usize) -> std::io::Result<&String>;
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct IsolationWindow {
    pub cv_param: CVVector,
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SelectedIon {
    pub cv_param: CVVector,
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Activation {
    pub cv_param: CVVector,
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SelectedIonList {
    pub selected_ion: Vec<SelectedIon>,
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Precursor {
    #[serde(rename = "@spectrumRef")]
    pub spectrum_ref: Option<String>,

    pub isolation_window: Option<IsolationWindow>,

    pub selected_ion_list: SelectedIonList,

    pub activation: Activation,
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct PrecursorList {
    pub precursor: Vec<Precursor>,
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Spectrum {
    pub cv_param: CVVector,

    #[serde(rename = "@index")]
    pub index: String,

    #[serde(rename = "@id")]
    pub id: String,

    #[serde(rename = "@defaultArrayLength")]
    pub default_array_length: String,

    pub binary_data_array_list: BinaryDataArrayList,

    pub scan_list: Option<ScanList>,

    pub precursor_list: Option<PrecursorList>,
}

impl DecodedArray for Spectrum {
    fn decompress_binary_string(&self, i: usize) -> std::io::Result<&String> {
        match &self.binary_data_array_list.binary_data_array[i]
            .binary
            .content
        {
            Some(content) => Ok(content),
            None => Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "no binary data",
            )),
        }
    }
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SpectrumList {
    pub spectrum: Vec<Spectrum>,
    pub count: String,
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Chromatogram {
    pub cv_param: CVVector,

    #[serde(rename = "@index")]
    pub index: String,

    #[serde(rename = "@id")]
    pub id: String,

    pub default_array_length: String,
    pub binary_data_array_list: BinaryDataArrayList,
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ChromatogramList {
    pub chromatogram: Vec<Chromatogram>,
    pub count: String,
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Run {
    pub id: String,
    pub spectrum_list: SpectrumList,
    pub chromatogram_list: ChromatogramList,
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ProcessingMethod {
    pub software_ref: String,
    pub cv_param: CVVector,
    pub user_param: Vec<UserParam>,
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct DataProcessing {
    pub id: String,
    pub processing_method: Vec<ProcessingMethod>,
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct DataProcessingList {
    pub data_processing: Vec<DataProcessing>,
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct FileContent {
    cv_param: CVVector,
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SourceFile {
    pub id: String,
    pub name: String,
    pub location: String,
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SourceFileList {
    source_file: SourceFile,
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct FileDescription {
    pub file_content: FileContent,

    pub source_file_list: SourceFileList,
    pub cv_param: CVVector,
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct MzML {
    pub run: Run,
    pub data_processing_list: DataProcessingList,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compression_from_cvvector_test() {
        let cv_param = CVParam::new(
            String::from("test"),
            String::from("MS:1000576"),
            String::from("test"),
            None,
            None,
            None,
            None,
        );

        let ctype = CompressionType::try_from(&cv_param).unwrap();
        assert_eq!(ctype, CompressionType::NoCompression);

        let cv_params = vec![cv_param];

        let new_ctype = CompressionType::try_from(&cv_params).unwrap();
        assert_eq!(new_ctype, CompressionType::NoCompression);
    }

    #[test]
    fn test_deserialize_cv_param() {
        let cv_param_tag =
            "<cvParam cvRef=\"MS\" accession=\"MS:1000576\" name=\"no compression\" value=\"\"/>";

        let cv_param: CVParam = quick_xml::de::from_str(cv_param_tag).unwrap();

        assert!(cv_param.cv_ref == "MS");
        assert!(cv_param.accession == "MS:1000576");
        assert!(cv_param.name == "no compression");
    }

    #[test]
    fn test_read_array() {
        let body = r#"<spectrum index="0" id="controllerType=0 controllerNumber=1 scan=500" defaultArrayLength="483">
        <cvParam cvRef="MS" accession="MS:1000511" name="ms level" value="1"/>
        <cvParam cvRef="MS" accession="MS:1000579" name="MS1 spectrum" value=""/>
        <cvParam cvRef="MS" accession="MS:1000130" name="positive scan" value=""/>
        <cvParam cvRef="MS" accession="MS:1000285" name="total ion current" value="9104850"/>
        <cvParam cvRef="MS" accession="MS:1000127" name="centroid spectrum" value=""/>
        <cvParam cvRef="MS" accession="MS:1000504" name="base peak m/z" value="624.24169921875" unitCvRef="MS" unitAccession="MS:1000040" unitName="m/z"/>
        <cvParam cvRef="MS" accession="MS:1000505" name="base peak intensity" value="908558.6875" unitCvRef="MS" unitAccession="MS:1000131" unitName="number of detector counts"/>
        <cvParam cvRef="MS" accession="MS:1000528" name="lowest observed m/z" value="551.828186035156" unitCvRef="MS" unitAccession="MS:1000040" unitName="m/z"/>
        <cvParam cvRef="MS" accession="MS:1000527" name="highest observed m/z" value="1732.8310546875" unitCvRef="MS" unitAccession="MS:1000040" unitName="m/z"/>
        <scanList count="1">
          <cvParam cvRef="MS" accession="MS:1000795" name="no combination" value=""/>
          <scan>
            <cvParam cvRef="MS" accession="MS:1000016" name="scan start time" value="1.06908425945" unitCvRef="UO" unitAccession="UO:0000031" unitName="minute"/>
            <cvParam cvRef="MS" accession="MS:1000512" name="filter string" value="FTMS + p NSI Full ms [550.0000-1800.0000]"/>
            <cvParam cvRef="MS" accession="MS:1000927" name="ion injection time" value="30.324" unitCvRef="UO" unitAccession="UO:0000028" unitName="millisecond"/>
            <scanWindowList count="1">
              <scanWindow>
                <cvParam cvRef="MS" accession="MS:1000501" name="scan window lower limit" value="550" unitCvRef="MS" unitAccession="MS:1000040" unitName="m/z"/>
                <cvParam cvRef="MS" accession="MS:1000500" name="scan window upper limit" value="1800" unitCvRef="MS" unitAccession="MS:1000040" unitName="m/z"/>
              </scanWindow>
            </scanWindowList>
          </scan>
        </scanList>
        <precursorList count="1">
          <precursor spectrumRef="controllerType=0 controllerNumber=1 scan=30068">
            <isolationWindow>
              <cvParam cvRef="MS" accession="MS:1000827" name="isolation window target m/z" value="643.368408203125" unitCvRef="MS" unitAccession="MS:1000040" unitName="m/z"/>
              <cvParam cvRef="MS" accession="MS:1000828" name="isolation window lower offset" value="1.0" unitCvRef="MS" unitAccession="MS:1000040" unitName="m/z"/>
              <cvParam cvRef="MS" accession="MS:1000829" name="isolation window upper offset" value="1.0" unitCvRef="MS" unitAccession="MS:1000040" unitName="m/z"/>
              <userParam name="ms level" value="1"/>
            </isolationWindow>
            <selectedIonList count="1">
              <selectedIon>
                <cvParam cvRef="MS" accession="MS:1000744" name="selected ion m/z" value="643.034396630915" unitCvRef="MS" unitAccession="MS:1000040" unitName="m/z"/>
                <cvParam cvRef="MS" accession="MS:1000041" name="charge state" value="3"/>
                <cvParam cvRef="MS" accession="MS:1000042" name="peak intensity" value="3.0614616075e08" unitCvRef="MS" unitAccession="MS:1000131" unitName="number of detector counts"/>
              </selectedIon>
            </selectedIonList>
            <activation>
              <cvParam cvRef="MS" accession="MS:1000422" name="beam-type collision-induced dissociation" value=""/>
              <cvParam cvRef="MS" accession="MS:1000045" name="collision energy" value="25.0" unitCvRef="UO" unitAccession="UO:0000266" unitName="electronvolt"/>
            </activation>
          </precursor>
        </precursorList>
        <binaryDataArrayList count="2">
          <binaryDataArray encodedLength="5152">
            <cvParam cvRef="MS" accession="MS:1000523" name="64-bit float" value=""/>
            <cvParam cvRef="MS" accession="MS:1000576" name="no compression" value=""/>
            <cvParam cvRef="MS" accession="MS:1000514" name="m/z array" value="" unitCvRef="MS" unitAccession="MS:1000040" unitName="m/z"/>
            <binary>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</binary>
          </binaryDataArray>
          <binaryDataArray encodedLength="2576">
            <cvParam cvRef="MS" accession="MS:1000521" name="32-bit float" value=""/>
            <cvParam cvRef="MS" accession="MS:1000576" name="no compression" value=""/>
            <cvParam cvRef="MS" accession="MS:1000515" name="intensity array" value="" unitCvRef="MS" unitAccession="MS:1000131" unitName="number of detector counts"/>
            <binary>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</binary>
          </binaryDataArray>
        </binaryDataArrayList>
      </spectrum>"#;
        let spectrum: Spectrum = quick_xml::de::from_str(body).unwrap();

        assert_eq!(spectrum.index, "0");
        assert_eq!(spectrum.cv_param.len(), 9);

        let scan_list = spectrum.scan_list.unwrap();

        assert_eq!(scan_list.scan.len(), 1);
        assert_eq!(scan_list.cv_param.len(), 1);

        let precursor_list = spectrum.precursor_list.unwrap();
        assert_eq!(precursor_list.precursor.len(), 1);

        assert_eq!(spectrum.binary_data_array_list.binary_data_array.len(), 2);
        assert_eq!(
            spectrum.binary_data_array_list.binary_data_array[0]
                .cv_param
                .len(),
            3
        );
    }

    #[test]
    fn test_from_massive_error() {
        let body = r#"<spectrum index="968" id="scanId=108186" defaultArrayLength="0">
        <cvParam cvRef="MS" accession="MS:1000130" name="positive scan" value=""/>
        <cvParam cvRef="MS" accession="MS:1000504" name="base peak m/z" value="160.99991725769" unitCvRef="MS" unitAccession="MS:1000040" unitName="m/z"/>
        <cvParam cvRef="MS" accession="MS:1000505" name="base peak intensity" value="54.864727" unitCvRef="MS" unitAccession="MS:1000131" unitName="number of detector counts"/>
        <cvParam cvRef="MS" accession="MS:1000285" name="total ion current" value="109.338783" unitCvRef="MS" unitAccession="MS:1000131" unitName="number of detector counts"/>
        <cvParam cvRef="MS" accession="MS:1000511" name="ms level" value="2"/>
        <cvParam cvRef="MS" accession="MS:1000580" name="MSn spectrum" value=""/>
        <cvParam cvRef="MS" accession="MS:1000127" name="centroid spectrum" value=""/>
        <cvParam cvRef="MS" accession="MS:1000796" name="spectrum title" value="SALJA0984.108186.108186.1 File:&quot;SALJA0984.d&quot;, NativeID:&quot;scanId=108186&quot;"/>
        <scanList count="1">
          <cvParam cvRef="MS" accession="MS:1000795" name="no combination" value=""/>
          <scan>
            <cvParam cvRef="MS" accession="MS:1000016" name="scan start time" value="1.802966666667" unitCvRef="UO" unitAccession="UO:0000031" unitName="minute"/>
            <scanWindowList count="1">
              <scanWindow>
                <cvParam cvRef="MS" accession="MS:1000501" name="scan window lower limit" value="0.0" unitCvRef="MS" unitAccession="MS:1000040" unitName="m/z"/>
                <cvParam cvRef="MS" accession="MS:1000500" name="scan window upper limit" value="0.0" unitCvRef="MS" unitAccession="MS:1000040" unitName="m/z"/>
              </scanWindow>
            </scanWindowList>
          </scan>
        </scanList>
        <precursorList count="1">
          <precursor spectrumRef="scanId=107792">
            <isolationWindow>
              <cvParam cvRef="MS" accession="MS:1000827" name="isolation window target m/z" value="250.114906311035" unitCvRef="MS" unitAccession="MS:1000040" unitName="m/z"/>
            </isolationWindow>
            <selectedIonList count="1">
              <selectedIon>
                <cvParam cvRef="MS" accession="MS:1000744" name="selected ion m/z" value="250.114906311035" unitCvRef="MS" unitAccession="MS:1000040" unitName="m/z"/>
                <cvParam cvRef="MS" accession="MS:1000041" name="charge state" value="1"/>
                <cvParam cvRef="MS" accession="MS:1000042" name="peak intensity" value="109.33878326416" unitCvRef="MS" unitAccession="MS:1000131" unitName="number of detector counts"/>
              </selectedIon>
            </selectedIonList>
            <activation>
              <cvParam cvRef="MS" accession="MS:1000422" name="beam-type collision-induced dissociation" value=""/>
              <cvParam cvRef="MS" accession="MS:1000045" name="collision energy" value="40.0" unitCvRef="UO" unitAccession="UO:0000266" unitName="electronvolt"/>
            </activation>
          </precursor>
        </precursorList>
        <binaryDataArrayList count="2">
          <binaryDataArray encodedLength="0">
            <cvParam cvRef="MS" accession="MS:1000521" name="32-bit float" value=""/>
            <cvParam cvRef="MS" accession="MS:1000576" name="no compression" value=""/>
            <cvParam cvRef="MS" accession="MS:1000514" name="m/z array" value="" unitCvRef="MS" unitAccession="MS:1000040" unitName="m/z"/>
            <binary></binary>
          </binaryDataArray>
          <binaryDataArray encodedLength="0">
            <cvParam cvRef="MS" accession="MS:1000521" name="32-bit float" value=""/>
            <cvParam cvRef="MS" accession="MS:1000576" name="no compression" value=""/>
            <cvParam cvRef="MS" accession="MS:1000515" name="intensity array" value="" unitCvRef="MS" unitAccession="MS:1000131" unitName="number of detector counts"/>
            <binary></binary>
          </binaryDataArray>
        </binaryDataArrayList>
      </spectrum>"#;

        let spectrum: Spectrum = quick_xml::de::from_str(body).unwrap();

        assert_eq!(spectrum.index, "968");
    }

    #[test]
    fn test_deserialize_spectrum_body() {
        let body = r#"<spectrum index="0" sourceFileRef="sf2" id="declaration=0 collection=0 scan=0" defaultArrayLength="15">
        <referenceableParamGroupRef ref="CommonPDASpectrumParams"/>
        <cvParam cvRef="MS" accession="MS:1000806" name="absorption spectrum" value=""/>
        <cvParam cvRef="MS" accession="MS:1000128" name="profile spectrum" value=""/>
        <cvParam cvRef="MS" accession="MS:1000618" name="highest observed wavelength" value="200" unitAccession="UO:0000018" unitName="nanometer" unitCvRef="UO"/>
        <cvParam cvRef="MS" accession="MS:1000619" name="lowest observed wavelength" value="600" unitAccession="UO:0000018" unitName="nanometer" unitCvRef="UO"/>
        <binaryDataArrayList count="2">
           <binaryDataArray encodedLength="160" >
             <cvParam cvRef="MS" accession="MS:1000523" name="64-bit float" value=""/>
             <cvParam cvRef="MS" accession="MS:1000576" name="no compression" value=""/>
             <cvParam cvRef="MS" accession="MS:1000617" name="wavelength array" unitAccession="UO:0000018" unitName="nanometer" unitCvRef="UO"/>
             <binary>AAAAAAAAAAAAAAAAAADwPwAAAAAAAABAAAAAAAAACEAAAAAAAAAQQAAAAAAAABRAAAAAAAAAGEAAAAAAAAAcQAAAAAAAACBAAAAAAAAAIkAAAAAAAAAkQAAAAAAAACZAAAAAAAAAKEAAAAAAAAAqQAAAAAAAACxA</binary>
           </binaryDataArray>
           <binaryDataArray encodedLength="160" >
             <cvParam cvRef="MS" accession="MS:1000523" name="64-bit float" value=""/>
             <cvParam cvRef="MS" accession="MS:1000576" name="no compression" value=""/>
             <cvParam cvRef="MS" accession="MS:1000515" name="intensity array" value=""
                      unitAccession="UO:0000269" unitName="absorbance unit" unitCvRef="UO"/>
                         <userParam name="itname" value="itarray1"/>
             <binary>AAAAAAAALkAAAAAAAAAsQAAAAAAAACpAAAAAAAAAKEAAAAAAAAAmQAAAAAAAACRAAAAAAAAAIkAAAAAAAAAgQAAAAAAAABxAAAAAAAAAGEAAAAAAAAAUQAAAAAAAABBAAAAAAAAACEAAAAAAAAAAQAAAAAAAAPA/</binary>
           </binaryDataArray>
         </binaryDataArrayList>
    </spectrum>"#;

        let spectrum: Spectrum = quick_xml::de::from_str(body).unwrap();

        assert_eq!(spectrum.index, "0");
    }
}
