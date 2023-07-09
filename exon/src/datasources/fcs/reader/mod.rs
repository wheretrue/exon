use std::{
    collections::HashMap,
    fmt::{Debug, Formatter},
    io::Cursor,
};

use tokio::io::{AsyncRead, AsyncReadExt};

use byteorder::{BigEndian, LittleEndian, ReadBytesExt};

// https://www.bioconductor.org/packages/release/bioc/vignettes/flowCore/inst/doc/fcs3.html

/// An FCS Record.
#[derive(Debug, Default)]
pub struct FcsRecord {
    /// The data of the record.
    pub data: Vec<f32>,
}

/// The MetaData of the FCS file
#[derive(Debug, Default)]
pub struct MetaData {
    file_version: String,
    text_start: u64,
    text_end: u64,
    data_start: u64,
    data_end: u64,
    analysis_start: u64,
    analysis_end: u64,
}

impl MetaData {
    /// Sets the file version.
    ///
    /// # Arguments
    ///
    /// * `file_version` - The file version.
    pub fn set_file_version(&mut self, file_version: String) {
        self.file_version = file_version;
    }

    /// Sets the text start.
    ///
    /// # Arguments
    ///
    /// * `text_start` - The text start.
    pub fn set_text_start(&mut self, text_start: u64) {
        self.text_start = text_start;
    }

    /// Sets the text end.
    ///
    /// # Arguments
    ///
    /// * `text_end` - The text end.
    pub fn set_text_end(&mut self, text_end: u64) {
        self.text_end = text_end;
    }

    /// Sets the data start.
    ///
    /// # Arguments
    ///
    /// * `data_start` - The data start.
    pub fn set_data_start(&mut self, data_start: u64) {
        self.data_start = data_start;
    }

    /// Sets the data end.
    ///
    /// # Arguments
    ///
    /// * `data_end` - The data end.
    pub fn set_data_end(&mut self, data_end: u64) {
        self.data_end = data_end;
    }

    /// Sets the analysis start.
    ///
    /// # Arguments
    ///
    /// * `analysis_start` - The analysis start.
    pub fn set_analysis_start(&mut self, analysis_start: u64) {
        self.analysis_start = analysis_start;
    }

    /// Sets the analysis end.
    ///
    /// # Arguments
    ///
    /// * `analysis_end` - The analysis end.
    pub fn set_analysis_end(&mut self, analysis_end: u64) {
        self.analysis_end = analysis_end;
    }

    /// Clears the meta data.
    pub fn clear(&mut self) {
        self.file_version.clear();
        self.text_start = 0;
        self.text_end = 0;
        self.data_start = 0;
        self.data_end = 0;
        self.analysis_start = 0;
        self.analysis_end = 0;
    }
}

fn parse_ascii_encoded_offset(buffer: &[u8]) -> std::io::Result<u64> {
    if let Ok(offset) = std::str::from_utf8(buffer) {
        if let Ok(offset) = offset.trim().parse::<u64>() {
            return Ok(offset);
        }
    }

    Err(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        "Invalid offset",
    ))
}

/// A reader for FCS files
pub struct Reader<R> {
    /// The underlying reader
    inner: R,

    /// Consumed bytes
    consumed: usize,
}

/// Read the metadata section of the FCS file
pub async fn read_metadata<R>(reader: &mut R, metadata: &mut MetaData) -> std::io::Result<usize>
where
    R: AsyncRead + Unpin,
{
    let mut buffer = [0u8; 58];
    reader.read_exact(&mut buffer).await?; // Read the metadata section into the buffer

    // convert the first 6 bytes to a string
    let file_version = std::str::from_utf8(&buffer[0..6])
        .map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Invalid file version string",
            )
        })?
        .to_string();
    metadata.set_file_version(file_version);

    let text_start = parse_ascii_encoded_offset(&buffer[10..18])?;
    metadata.set_text_start(text_start);

    let text_end = parse_ascii_encoded_offset(&buffer[18..26])?;
    metadata.set_text_end(text_end);

    let data_start = parse_ascii_encoded_offset(&buffer[26..34])?;
    metadata.set_data_start(data_start);

    let data_end = parse_ascii_encoded_offset(&buffer[34..42])?;
    metadata.set_data_end(data_end);

    let analysis_start = parse_ascii_encoded_offset(&buffer[42..50])?;
    metadata.set_analysis_start(analysis_start);

    let analysis_end = parse_ascii_encoded_offset(&buffer[50..58])?;
    metadata.set_analysis_end(analysis_end);

    Ok(58)
}

/// TextData an alias for a HashMap of String to String
pub struct TextData(HashMap<String, String>);

impl Debug for TextData {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TextData").field("data", &self.0).finish()
    }
}

impl Default for TextData {
    /// Create a new TextData
    fn default() -> Self {
        Self::new()
    }
}

impl TextData {
    /// Create a new TextData
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    /// Get the value for a key
    pub fn get(&self, key: &str) -> Option<&String> {
        self.0.get(key)
    }

    /// Insert a key value pair
    pub fn insert(&mut self, key: String, value: String) {
        self.0.insert(key, value);
    }

    /// Check the number of events
    pub fn number_of_events(&self) -> Option<u64> {
        if let Some(number_of_events) = self.get("$TOT") {
            if let Ok(number_of_events) = number_of_events.parse::<u64>() {
                return Some(number_of_events);
            }
        }

        None
    }

    /// Check the number of parameters
    pub fn number_of_parameters(&self) -> Option<u64> {
        if let Some(number_of_parameters) = self.get("$PAR") {
            if let Ok(number_of_parameters) = number_of_parameters.parse::<u64>() {
                return Some(number_of_parameters);
            }
        }

        None
    }

    /// Check the data type
    pub fn data_type(&self) -> Option<DataType> {
        match self.get("$DATATYPE") {
            Some(data_type) => match data_type.as_str() {
                "F" => Some(DataType::Float32),
                _ => None,
            },
            None => None,
        }
    }

    /// Check the endianness
    pub fn endianness(&self) -> Option<Endianness> {
        if let Some(byteorder_value) = self.get("$BYTEORD") {
            if byteorder_value == "1,2,3,4" {
                return Some(Endianness::Little);
            } else if byteorder_value == "4,3,2,1" {
                return Some(Endianness::Big);
            }
        }

        None
    }

    /// Get the bytes per parameter
    pub fn bytes_per_parameter(&self) -> Vec<usize> {
        let mut bytes_per = Vec::new();

        let par_number = self.number_of_parameters().unwrap_or(0);

        for i in 1..=par_number {
            if let Some(bits_per_parameter) = self.get(&format!("$P{}B", i)) {
                if let Ok(bits_per_parameter) = bits_per_parameter.parse::<usize>() {
                    bytes_per.push(bits_per_parameter / 8);
                }
            }
        }

        bytes_per
    }
}

/// Holds the endianness of the data
#[derive(Debug, PartialEq)]
pub enum Endianness {
    /// Big endian
    Big,
    /// Little endian
    Little,
}

/// Read the text section of the FCS file
pub async fn read_text<R>(
    reader: &mut R,
    text_data: &mut TextData,
    metadata: &MetaData,
) -> std::io::Result<usize>
where
    R: AsyncRead + Unpin,
{
    let text_section_length = metadata.text_end - metadata.text_start;

    let mut buffer = vec![0u8; text_section_length as usize];

    reader.read_exact(&mut buffer).await?;

    let text = std::str::from_utf8(&buffer).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Invalid text section string",
        )
    })?;

    // Parse the text section into a HashMap
    // An example of a text section is: /GTI$BEGINLOG/   8482338/GTI$ENDLOG/   8488938
    // These are two key value pairs
    let mut parts = text.strip_prefix('/').unwrap_or(text).split('/');

    while let Some(key) = parts.next() {
        if let Some(value) = parts.next() {
            text_data.insert(key.to_string(), value.to_string());
        }
    }

    Ok(text_section_length as usize)
}

/// Read a record from the FCS file
pub async fn read_record<R>(
    reader: &mut R,
    _metadata: &MetaData,
    text: &TextData,
    record: &mut FcsRecord,
) -> std::io::Result<usize>
where
    R: AsyncRead + Unpin,
{
    let total_bytes = text.bytes_per_parameter().iter().sum::<usize>();
    let mut buffer = vec![0u8; total_bytes];

    let f32_size = total_bytes / 4;

    record.data = vec![0f32; f32_size];
    reader.read_exact(&mut buffer).await?;

    let mut cursor = Cursor::new(buffer);

    match text.endianness() {
        Some(Endianness::Big) => {
            for i in 0..f32_size {
                record.data[i] = ReadBytesExt::read_f32::<BigEndian>(&mut cursor)?;
            }
        }
        Some(Endianness::Little) => {
            for i in 0..f32_size {
                record.data[i] = ReadBytesExt::read_f32::<LittleEndian>(&mut cursor)?;
            }
        }
        None => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Invalid endianness",
            ))
        }
    }

    Ok(total_bytes)
}

/// A data type in the FCS file.
pub enum DataType {
    /// A 32-bit float (f32)
    Float32,
}

impl<R> Reader<R>
where
    R: AsyncRead + Unpin,
{
    /// Create a new FCS reader
    pub fn new(inner: R) -> Reader<R> {
        Reader { inner, consumed: 0 }
    }

    /// Read the Metadata of the FCS file
    pub async fn read_metadata(&mut self) -> std::io::Result<MetaData> {
        let mut metadata = MetaData::default();

        let read_data = read_metadata(&mut self.inner, &mut metadata).await?;
        self.consumed += read_data;

        Ok(metadata)
    }

    /// Read the text section of the FCS file
    pub async fn read_text(&mut self, metadata: &MetaData) -> std::io::Result<TextData> {
        let mut text = TextData::new();

        let read_data = read_text(&mut self.inner, &mut text, metadata).await?;
        self.consumed += read_data;

        Ok(text)
    }

    /// Read a single byte to advance the reader to the Data section
    pub async fn read_to_data(&mut self) -> std::io::Result<()> {
        let mut single_byte = [0u8; 1];
        let read_data = self.inner.read_exact(&mut single_byte).await?;

        self.consumed += read_data;

        Ok(())
    }

    /// Read a single record from the FCS file
    pub async fn read_record(
        &mut self,
        metadata: &MetaData,
        text: &TextData,
    ) -> std::io::Result<FcsRecord> {
        let mut fcs_record = FcsRecord::default();

        let bytes_read = read_record(&mut self.inner, metadata, text, &mut fcs_record).await?;
        self.consumed += bytes_read;

        Ok(fcs_record)
    }
}

#[cfg(test)]
mod tests {
    use crate::tests::test_path;

    use super::*;

    #[tokio::test]
    async fn test_read_file() -> std::io::Result<()> {
        let test_path = test_path("fcs", "Guava Muse.fcs");

        let file = tokio::fs::File::open(test_path).await.unwrap();

        let mut reader = Reader::new(file);

        let metadata = reader.read_metadata().await.unwrap();
        assert_eq!(reader.consumed, 58);

        assert_eq!(metadata.file_version, "FCS3.0");
        assert_eq!(metadata.text_start, 58);
        assert_eq!(metadata.text_end, 3445);
        assert_eq!(metadata.data_start, 3446);
        assert_eq!(metadata.data_end, 7765);
        assert_eq!(metadata.analysis_start, 0);
        assert_eq!(metadata.analysis_end, 0);

        let text_data = reader.read_text(&metadata).await.unwrap();
        assert_eq!(reader.consumed, 3445);

        assert_eq!(text_data.get("$TOT"), Some(&"108".to_string()));
        assert_eq!(text_data.get("$PAR"), Some(&"10".to_string()));
        assert_eq!(text_data.get("$BYTEORD"), Some(&"1,2,3,4".to_string()));
        assert_eq!(text_data.get("$DATATYPE"), Some(&"F".to_string()));

        assert_eq!(text_data.number_of_events(), Some(108));
        assert_eq!(text_data.number_of_parameters(), Some(10));

        let endianness = text_data.endianness();
        assert_eq!(endianness, Some(Endianness::Little));

        reader.read_to_data().await.unwrap();

        for i in 0..108 {
            let record = reader.read_record(&metadata, &text_data).await.unwrap();

            // test the first record
            if i == 0 {
                assert_eq!(record.data.len(), 10);
                assert_eq!(
                    record.data,
                    vec![
                        481.9313, 7.5, 84.2256, 7.5, 395.87415, 7.5, 35964.0, 2.682985, 1.9254441,
                        2.597557
                    ]
                );
            }
        }

        assert_eq!(reader.consumed, 7766);

        Ok(())
    }
}
