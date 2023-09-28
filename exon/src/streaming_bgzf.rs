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

// A streaming bgzf reader. Mimics seek, but only works for forward reads.

use noodles::bgzf::{self, VirtualPosition};
use tokio::io::AsyncReadExt;

/// A streaming bgzf reader.
pub struct AsyncBGZFReader<R>
where
    R: tokio::io::AsyncRead + Unpin + tokio::io::AsyncBufRead,
{
    inner: bgzf::AsyncReader<R>,
}

impl<R> AsyncBGZFReader<R>
where
    R: tokio::io::AsyncRead + Unpin + tokio::io::AsyncBufRead,
{
    /// Create a new streaming bgzf reader.
    pub fn new(reader: bgzf::AsyncReader<R>) -> Self {
        Self { inner: reader }
    }

    /// Create a new streaming bgzf reader from a reader.
    pub fn from_reader(reader: R) -> Self {
        let reader = bgzf::AsyncReader::new(reader);

        Self::new(reader)
    }

    /// Convert the reader into the inner reader.
    pub fn into_inner(self) -> bgzf::AsyncReader<R> {
        self.inner
    }

    /// Get the virtual position of the reader.
    pub fn virtual_position(&self) -> VirtualPosition {
        self.inner.virtual_position()
    }

    /// Scan to a virtual position.
    pub async fn scan_to_virtual_position(&mut self, vp: VirtualPosition) -> std::io::Result<()> {
        let mut buf = [0; 1];

        while self.inner.virtual_position() < vp {
            self.inner.read_exact(&mut buf).await?;
        }

        Ok(())
    }

    /// Read to a virtual position.
    pub async fn read_to_virtual_position(
        &mut self,
        vp: VirtualPosition,
    ) -> std::io::Result<Vec<u8>> {
        let mut buf = Vec::new();

        while self.inner.virtual_position() < vp {
            let mut b = [0; 1];
            self.inner.read_exact(&mut b).await?;
            buf.push(b[0]);
        }

        Ok(buf)
    }
}

#[cfg(test)]
mod tests {
    use object_store::path::Path;
    use tokio_util::io::StreamReader;

    use crate::{streaming_bgzf::AsyncBGZFReader, tests::test_path};

    #[cfg(not(target_os = "windows"))]
    #[tokio::test]
    async fn test_read() -> Result<(), Box<dyn std::error::Error>> {
        let table_path = test_path("biobear-vcf", "vcf_file.vcf.gz");
        let table_path = Path::from(table_path.to_str().unwrap());

        let object_store = crate::tests::make_object_store();
        let object_meta = object_store.head(&table_path).await?;

        let stream = object_store.get(&table_path).await?.into_stream();
        let stream_reader = StreamReader::new(stream);

        let mut reader = AsyncBGZFReader::from_reader(stream_reader);

        let region = "1".parse()?;
        let chunks = crate::datasources::indexed_file_utils::IndexedFile::Vcf
            .get_byte_range_for_file(object_store.clone(), &object_meta, &region)
            .await?;
        let first_chunk = chunks.first().unwrap();

        reader.scan_to_virtual_position(first_chunk.start()).await?;

        assert_eq!(reader.virtual_position(), first_chunk.start());

        Ok(())
    }
}
