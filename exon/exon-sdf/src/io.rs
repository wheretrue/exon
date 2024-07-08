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

use std::io::BufRead;

use crate::{record::parse_to_record, Record};

/// A reader for reading records from an SD file.
pub struct Reader<R> {
    inner: R,
}

impl<R> Reader<R>
where
    R: BufRead,
{
    pub fn new(inner: R) -> Self {
        Reader { inner }
    }

    /// Read a single record's bytes from the underlying reader.
    pub fn read_record_bytes(&mut self, buf: &mut Vec<u8>) -> std::io::Result<usize> {
        let mut bytes_read = 0;
        loop {
            let n = self.inner.read_until(b'\n', buf)?;
            bytes_read += n;

            if n == 0 {
                return Ok(bytes_read);
            }

            if buf.ends_with(b"$$$$\n") || buf.ends_with(b"$$$$\r\n") {
                return Ok(bytes_read);
            }
        }
    }

    /// Read record from the underlying reader.
    pub fn read_record(&mut self) -> crate::Result<Option<Record>> {
        let mut buf = Vec::new();
        let bytes_read = self.read_record_bytes(&mut buf)?;

        if bytes_read == 0 {
            return Ok(None);
        }

        let s = match std::str::from_utf8(&buf) {
            Ok(v) => v,
            Err(e) => panic!("Invalid UTF-8 sequence: {}", e),
        };

        let record = parse_to_record(s)?;
        Ok(Some(record))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_record() {
        let molfile_content = r#"
Methane
Example

2  1  0  0  0  0            999 V2000
    0.0000    0.0000    0.0000 C   0  0  0  0  0  0
    0.0000    1.0000    0.0000 H   0  0  0  0  0  0
1  2  1  0  0  0
M  END
>  <MELTING.POINT>
-182.5

$$$$
"#
        .trim();

        let mut reader = Reader::new(std::io::Cursor::new(molfile_content));

        let record = reader.read_record().unwrap().unwrap();

        assert_eq!(record.header(), "Methane\nExample\n");
        assert_eq!(record.data().len(), 1);
        assert_eq!(record.atom_count(), 2);
        assert_eq!(record.bond_count(), 1);

        let data = record
            .data()
            .into_iter()
            .map(|d| (d.header(), d.data()))
            .collect::<Vec<_>>();

        assert_eq!(data, vec![("MELTING.POINT", "-182.5")]);
    }
}
