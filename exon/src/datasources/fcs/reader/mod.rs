use std::{
    collections::HashMap,
    fmt::{Debug, Formatter},
    io::Cursor,
};

use noodles::bgzf::gzi::r#async;
use tokio::io::{AsyncBufRead, AsyncRead, AsyncReadExt};

use byteorder::{BigEndian, LittleEndian};

// https://www.bioconductor.org/packages/release/bioc/vignettes/flowCore/inst/doc/fcs3.html

pub struct FcsRecord {
    pub data: Vec<f32>,
}

impl FcsRecord {
    pub fn new() -> Self {
        Self { data: Vec::new() }
    }
}

pub struct MetaData {
    file_version: String,
    text_start: u64,
    text_end: u64,
    data_start: u64,
    data_end: u64,
    analysis_start: u64,
    analysis_end: u64,
}

impl Default for MetaData {
    fn default() -> Self {
        Self {
            file_version: String::new(),
            text_start: 0,
            text_end: 0,
            data_start: 0,
            data_end: 0,
            analysis_start: 0,
            analysis_end: 0,
        }
    }
}

impl MetaData {
    pub fn set_file_version(&mut self, file_version: String) {
        self.file_version = file_version;
    }

    pub fn set_text_start(&mut self, text_start: u64) {
        self.text_start = text_start;
    }

    pub fn set_text_end(&mut self, text_end: u64) {
        self.text_end = text_end;
    }

    pub fn set_data_start(&mut self, data_start: u64) {
        self.data_start = data_start;
    }

    pub fn set_data_end(&mut self, data_end: u64) {
        self.data_end = data_end;
    }

    pub fn set_analysis_start(&mut self, analysis_start: u64) {
        self.analysis_start = analysis_start;
    }

    pub fn set_analysis_end(&mut self, analysis_end: u64) {
        self.analysis_end = analysis_end;
    }

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

    return Err(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        "Invalid offset",
    ));
}

pub struct Reader<R> {
    /// The inner buffered reader.
    inner: R,

    /// The number of bytes consumed from the underlying reader.
    consumed: usize,
}

pub async fn read_metadata<R>(reader: &mut R, metadata: &mut MetaData) -> std::io::Result<usize>
where
    R: AsyncRead + Unpin,
{
    let mut buffer = [0u8; 58]; // Create a buffer to read the metadata section
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

    return Ok(58);
}

// TextData an alias for a HashMap of String to String
pub struct TextData(HashMap<String, String>);

impl Debug for TextData {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TextData").field("data", &self.0).finish()
    }
}

impl TextData {
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    pub fn get(&self, key: &str) -> Option<&String> {
        self.0.get(key)
    }

    pub fn insert(&mut self, key: String, value: String) {
        self.0.insert(key, value);
    }

    pub fn number_of_events(&self) -> Option<u64> {
        if let Some(number_of_events) = self.get("$TOT") {
            if let Ok(number_of_events) = number_of_events.parse::<u64>() {
                return Some(number_of_events);
            }
        }

        None
    }

    pub fn number_of_parameters(&self) -> Option<u64> {
        if let Some(number_of_parameters) = self.get("$PAR") {
            if let Ok(number_of_parameters) = number_of_parameters.parse::<u64>() {
                return Some(number_of_parameters);
            }
        }

        None
    }

    pub fn data_type(&self) -> Option<DataType> {
        match self.get("$DATATYPE") {
            Some(data_type) => match data_type.as_str() {
                "F" => Some(DataType::Float),
                "I" => Some(DataType::Integer),
                "D" => Some(DataType::Decimal),
                _ => None,
            },
            None => None,
        }
    }

    pub fn endianness(&self) -> Option<Endianness> {
        if let Some(byteorder_value) = self.get("$BYTEORD") {
            if byteorder_value == "1,2,3,4" {
                return Some(Endianness::Big);
            } else if byteorder_value == "4,3,2,1" {
                return Some(Endianness::Little);
            }
        }

        None
    }

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

#[derive(Debug, PartialEq)]
pub enum Endianness {
    Big,
    Little,
}

// Read the text section of the FCS file
// Starting position of the reader should be at the beginning of the text section
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

pub async fn read_record<R>(
    reader: &mut R,
    metadata: &MetaData,
    text: &TextData,
    record: &mut FcsRecord,
) -> std::io::Result<usize>
where
    R: AsyncRead + Unpin,
{
    let total_bytes = text.bytes_per_parameter().iter().sum::<usize>();
    let mut buffer = vec![0u8; total_bytes];

    // try to parse the buffer into an array of f32
    let mut f32_buffer = vec![0f32; total_bytes / 4];
    reader.read_exact(&mut buffer).await?;

    let mut cursor = Cursor::new(buffer);

    match text.endianness() {
        Some(Endianness::Big) => {
            for i in 0..f32_buffer.len() {
                f32_buffer[i] = cursor.read_f32().await?;
            }
        }
        Some(Endianness::Little) => {
            for i in 0..f32_buffer.len() {
                f32_buffer[i] = cursor.read_f32().await?;
            }
        }
        None => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Invalid endianness",
            ))
        }
    }

    eprintln!("f32_buffer: {:?}", f32_buffer);

    Ok(total_bytes)
}

pub enum DataType {
    Float,
    Integer,
    Decimal,
}

impl<R> Reader<R>
where
    R: AsyncRead + Unpin,
{
    pub fn new(inner: R) -> Reader<R> {
        Reader { inner, consumed: 0 }
    }

    pub async fn read_metadata(&mut self) -> std::io::Result<MetaData> {
        let mut metadata = MetaData::default();

        let read_data = read_metadata(&mut self.inner, &mut metadata).await?;
        self.consumed += read_data;

        Ok(metadata)
    }

    pub async fn read_text(&mut self, metadata: &MetaData) -> std::io::Result<TextData> {
        let mut text = TextData::new();

        let read_data = read_text(&mut self.inner, &mut text, metadata).await?;
        self.consumed += read_data;

        Ok(text)
    }

    pub async fn read_record(
        &mut self,
        metadata: &MetaData,
        text: &TextData,
    ) -> std::io::Result<FcsRecord> {
        let mut fcs_record = FcsRecord::new();

        let bytes_read = read_record(&mut self.inner, metadata, text, &mut fcs_record).await?;
        self.consumed += bytes_read;

        Ok(fcs_record)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_read_file() -> std::io::Result<()> {
        let file = tokio::fs::File::open("/Users/thauck/wheretrue/github.com/wheretrue/exon/exon/test-data/datasources/fcs/Guava Muse.fcs")
            .await
            .unwrap();

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

        assert_eq!(text_data.number_of_events(), Some(108));
        assert_eq!(text_data.number_of_parameters(), Some(10));

        let endianness = text_data.endianness();
        assert_eq!(endianness, Some(Endianness::Big));

        // 4320
        // _, ii = fcsparser.parse("/Users/thauck/wheretrue/github.com/wheretrue/exon/exon/test-data/datasources/fcs/Guava Muse.fcs")

        for _ in 0..108 {
            let record = reader.read_record(&metadata, &text_data).await.unwrap();
        }

        // let record = reader.read_record(&metadata, &text_data).await.unwrap();
        // let record = reader.read_record(&metadata, &text_data).await.unwrap();

        Ok(())
    }
}
