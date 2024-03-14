// Copyright 2014-2015 Johannes Köster, Vadim Nazarov, Patrick Marks
// Licensed under the MIT license (http://opensource.org/licenses/MIT)
// This file may not be copied, modified, or distributed
// except according to those terms.

//! Types for representing pairwise sequence alignments

#[cfg(feature = "clap")]
use clap::ValueEnum;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

pub type TextSlice<'a> = &'a [u8];

/// Alignment operations supported are match, substitution, insertion, deletion
/// and clipping. Clipping is a special boundary condition where you are allowed
/// to clip off the beginning/end of the sequence for a fixed clip penalty. The
/// clip penalty could be different for the two sequences x and y, and the
/// clipping operations on both are distinguishable (Xclip and Yclip). The usize
/// value associated with the clipping operations are the lengths clipped. In case
/// of standard modes like Global, Semi-Global and Local alignment, the clip operations
/// are filtered out
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Eq, PartialEq, Debug, Copy, Clone, Hash)]
pub enum AlignmentOperation {
    Match,
    Subst,
    Del,
    Ins,
    Xclip(usize),
    Yclip(usize),
}

/// The modes of alignment supported by the aligner include standard modes such as
/// Global, Semi-Global and Local alignment. In addition to this, user can also invoke
/// the custom mode. In the custom mode, users can explicitly specify the clipping penalties
/// for prefix and suffix of strings 'x' and 'y' independently. Under the hood the standard
/// modes are implemented as special cases of the custom mode with the clipping penalties
/// appropriately set.
///
/// The default alignment mode is Global.
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "clap", derive(ValueEnum))]
#[derive(Debug, PartialEq, Eq, Copy, Clone)]
pub enum AlignmentMode {
    Local,
    Semiglobal,
    Global,
    Custom,
}

impl Default for AlignmentMode {
    fn default() -> Self {
        AlignmentMode::Global
    }
}

/// We consider alignment between two sequences x and  y. x is the query or read sequence
/// and y is the reference or template sequence. An alignment, consisting of a score,
/// the start and end position of the alignment on sequence x and sequence y, the
/// lengths of sequences x and y, and the alignment edit operations. The start position
/// and end position of the alignment does not include the clipped regions. The length
/// of clipped regions are already encapsulated in the Alignment Operation.
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Debug, Eq, PartialEq, Clone, Default)]
pub struct Alignment {
    /// Smith-Waterman alignment score
    pub score: i32,

    /// Start position of alignment in reference
    pub ystart: usize,

    /// Start position of alignment in query
    pub xstart: usize,

    /// End position of alignment in reference
    pub yend: usize,

    /// End position of alignment in query
    pub xend: usize,

    /// Length of the reference sequence
    pub ylen: usize,

    /// Length of the query sequence
    pub xlen: usize,

    /// Vector of alignment operations
    pub operations: Vec<AlignmentOperation>,
    pub mode: AlignmentMode,
}

impl Alignment {
    /// Calculate the cigar string from the alignment struct. x is the target string
    ///
    /// # Example
    ///
    /// ```
    /// use bio_types::alignment::{Alignment,AlignmentMode};
    /// use bio_types::alignment::AlignmentOperation::{Match, Subst, Ins, Del};
    /// let alignment = Alignment {
    ///     score: 5,
    ///     xstart: 3,
    ///     ystart: 0,
    ///     xend: 9,
    ///     yend: 10,
    ///     ylen: 10,
    ///     xlen: 10,
    ///     operations: vec![Match, Match, Match, Subst, Ins, Ins, Del, Del],
    ///     mode: AlignmentMode::Semiglobal
    /// };
    /// assert_eq!(alignment.cigar(false), "3S3=1X2I2D1S");
    /// ```
    pub fn cigar(&self, hard_clip: bool) -> String {
        match self.mode {
            AlignmentMode::Global => panic!(" Cigar fn not supported for Global Alignment mode"),
            AlignmentMode::Local => panic!(" Cigar fn not supported for Local Alignment mode"),
            _ => {}
        }

        let clip_str = if hard_clip { "H" } else { "S" };

        let add_op = |op: AlignmentOperation, k, cigar: &mut String| match op {
            AlignmentOperation::Match => cigar.push_str(&format!("{}{}", k, "=")),
            AlignmentOperation::Subst => cigar.push_str(&format!("{}{}", k, "X")),
            AlignmentOperation::Del => cigar.push_str(&format!("{}{}", k, "D")),
            AlignmentOperation::Ins => cigar.push_str(&format!("{}{}", k, "I")),
            _ => {}
        };

        let mut cigar = "".to_owned();
        if self.operations.is_empty() {
            return cigar;
        }

        let mut last = self.operations[0];
        if self.xstart > 0 {
            cigar.push_str(&format!("{}{}", self.xstart, clip_str))
        }
        let mut k = 1;
        for &op in self.operations[1..].iter() {
            if op == last {
                k += 1;
            } else {
                add_op(last, k, &mut cigar);
                k = 1;
            }
            last = op;
        }
        add_op(last, k, &mut cigar);
        if self.xlen > self.xend {
            cigar.push_str(&format!("{}{}", self.xlen - self.xend, clip_str))
        }

        cigar
    }

    /// Return the pretty formatted alignment as a String. The string
    /// contains sets of 3 lines of length 100. First line is for the
    /// sequence x, second line is for the alignment operation and the
    /// the third line is for the sequence y. A '-' in the sequence
    /// indicates a blank (insertion/deletion). The operations follow
    /// the following convention: '|' for a match, '\\' (a single backslash) for a mismatch,
    /// '+' for an insertion, 'x' for a deletion and ' ' for clipping
    ///
    /// # Example
    ///
    /// If we align the strings "CCGTCCGGCAAGGG" and "AAAAACCGTTGACGGCCAA"
    /// in various modes, we will get the following output:
    ///
    /// Semiglobal:
    /// ```c
    ///         CCGTCCGGCAAGGG
    ///         ||||++++\\|\||
    ///    AAAAACCGT----TGACGGCCAA
    /// ```
    ///
    /// Local:
    /// ```c
    ///         CCGTCCGGCAAGGG
    ///         ||||
    ///    AAAAACCGT          TGACGGCCAA
    /// ```
    ///
    /// Global:
    /// ```c
    ///    -----CCGT--CCGGCAAGGG
    ///    xxxxx||||xx\||||\|++\
    ///    AAAAACCGTTGACGGCCA--A
    /// ```
    ///
    pub fn pretty(&self, x: TextSlice, y: TextSlice, ncol: usize) -> String {
        let mut x_pretty = String::new();
        let mut y_pretty = String::new();
        let mut inb_pretty = String::new();

        if !self.operations.is_empty() {
            let mut x_i: usize;
            let mut y_i: usize;

            // If the alignment mode is one of the standard ones, the prefix clipping is
            // implicit so we need to process it here
            match self.mode {
                AlignmentMode::Custom => {
                    x_i = 0;
                    y_i = 0;
                }
                _ => {
                    x_i = self.xstart;
                    y_i = self.ystart;
                    for k in x.iter().take(self.xstart) {
                        x_pretty.push_str(&format!("{}", String::from_utf8_lossy(&[*k])));
                        inb_pretty.push(' ');
                        y_pretty.push(' ')
                    }
                    for k in y.iter().take(self.ystart) {
                        y_pretty.push_str(&format!("{}", String::from_utf8_lossy(&[*k])));
                        inb_pretty.push(' ');
                        x_pretty.push(' ')
                    }
                }
            }

            // Process the alignment.
            for i in 0..self.operations.len() {
                match self.operations[i] {
                    AlignmentOperation::Match => {
                        x_pretty.push_str(&format!("{}", String::from_utf8_lossy(&[x[x_i]])));
                        x_i += 1;

                        inb_pretty.push('|');

                        y_pretty.push_str(&format!("{}", String::from_utf8_lossy(&[y[y_i]])));
                        y_i += 1;
                    }
                    AlignmentOperation::Subst => {
                        x_pretty.push_str(&format!("{}", String::from_utf8_lossy(&[x[x_i]])));
                        x_i += 1;

                        inb_pretty.push('\\');

                        y_pretty.push_str(&format!("{}", String::from_utf8_lossy(&[y[y_i]])));
                        y_i += 1;
                    }
                    AlignmentOperation::Del => {
                        x_pretty.push('-');

                        inb_pretty.push('x');

                        y_pretty.push_str(&format!("{}", String::from_utf8_lossy(&[y[y_i]])));
                        y_i += 1;
                    }
                    AlignmentOperation::Ins => {
                        x_pretty.push_str(&format!("{}", String::from_utf8_lossy(&[x[x_i]])));
                        x_i += 1;

                        inb_pretty.push('+');

                        y_pretty.push('-');
                    }
                    AlignmentOperation::Xclip(len) => {
                        for k in x.iter().take(len) {
                            x_pretty.push_str(&format!("{}", String::from_utf8_lossy(&[*k])));
                            x_i += 1;

                            inb_pretty.push(' ');

                            y_pretty.push(' ')
                        }
                    }
                    AlignmentOperation::Yclip(len) => {
                        for k in y.iter().take(len) {
                            y_pretty.push_str(&format!("{}", String::from_utf8_lossy(&[*k])));
                            y_i += 1;

                            inb_pretty.push(' ');

                            x_pretty.push(' ')
                        }
                    }
                }
            }

            // If the alignment mode is one of the standard ones, the suffix clipping is
            // implicit so we need to process it here
            match self.mode {
                AlignmentMode::Custom => {}
                _ => {
                    for k in x.iter().take(self.xlen).skip(x_i) {
                        x_pretty.push_str(&format!("{}", String::from_utf8_lossy(&[*k])));
                        inb_pretty.push(' ');
                        y_pretty.push(' ')
                    }
                    for k in y.iter().take(self.ylen).skip(y_i) {
                        y_pretty.push_str(&format!("{}", String::from_utf8_lossy(&[*k])));
                        inb_pretty.push(' ');
                        x_pretty.push(' ')
                    }
                }
            }
        }

        let mut s = String::new();
        let mut idx = 0;
        use std::cmp::min;

        assert_eq!(x_pretty.len(), inb_pretty.len());
        assert_eq!(y_pretty.len(), inb_pretty.len());

        let ml = x_pretty.len();

        while idx < ml {
            let rng = idx..min(idx + ncol, ml);
            s.push_str(&x_pretty[rng.clone()]);
            s.push('\n');

            s.push_str(&inb_pretty[rng.clone()]);
            s.push('\n');

            s.push_str(&y_pretty[rng]);
            s.push('\n');

            s.push_str("\n\n");
            idx += ncol;
        }

        s
    }

    /// Returns the optimal path in the alignment matrix
    ///
    /// # Example
    ///
    /// ```
    /// use bio_types::alignment::{Alignment,AlignmentMode};
    /// use bio_types::alignment::AlignmentOperation::*;
    /// let alignment = Alignment {
    ///     score: 5,
    ///     xstart: 3,
    ///     ystart: 0,
    ///     xend: 9,
    ///     yend: 10,
    ///     ylen: 10,
    ///     xlen: 10,
    ///     operations: vec![Match, Match, Match, Subst, Ins, Ins, Del, Del],
    ///     mode: AlignmentMode::Semiglobal,
    /// };
    /// assert_eq!(alignment.path(),[
    ///     (4, 5, Match),
    ///     (5, 6, Match),
    ///     (6, 7, Match),
    ///     (7, 8, Subst),
    ///     (8, 8, Ins),
    ///     (9, 8, Ins),
    ///     (9, 9, Del),
    ///     (9, 10, Del)])
    /// ```
    pub fn path(&self) -> Vec<(usize, usize, AlignmentOperation)> {
        let mut path = Vec::new();

        if !self.operations.is_empty() {
            let last = match self.mode {
                AlignmentMode::Custom => (self.xlen, self.ylen),
                _ => (self.xend, self.yend),
            };
            let mut x_i = last.0;
            let mut y_i = last.1;

            let mut ops = self.operations.clone();
            ops.reverse();

            // Process the alignment.
            for i in ops {
                path.push((x_i, y_i, i));
                match i {
                    AlignmentOperation::Match => {
                        x_i -= 1;
                        y_i -= 1;
                    }
                    AlignmentOperation::Subst => {
                        x_i -= 1;
                        y_i -= 1;
                    }
                    AlignmentOperation::Del => {
                        y_i -= 1;
                    }
                    AlignmentOperation::Ins => {
                        x_i -= 1;
                    }
                    AlignmentOperation::Xclip(len) => {
                        x_i -= len;
                    }
                    AlignmentOperation::Yclip(len) => {
                        y_i -= len;
                    }
                }
            }
        }
        path.reverse();
        path
    }

    /// Filter out Xclip and Yclip operations from the list of operations. Useful
    /// when invoking the standard modes.
    pub fn filter_clip_operations(&mut self) {
        use self::AlignmentOperation::{Del, Ins, Match, Subst};
        self.operations
            .retain(|x| (*x == Match || *x == Subst || *x == Ins || *x == Del));
    }

    /// Number of bases in reference sequence that are aligned
    pub fn y_aln_len(&self) -> usize {
        self.yend - self.ystart
    }

    /// Number of bases in query sequence that are aigned
    pub fn x_aln_len(&self) -> usize {
        self.xend - self.xstart
    }
}

#[cfg(test)]
mod tests {
    use super::AlignmentOperation::*;
    use super::*;

    #[test]
    fn test_cigar() {
        let alignment = Alignment {
            score: 5,
            xstart: 3,
            ystart: 0,
            xend: 9,
            yend: 10,
            ylen: 10,
            xlen: 10,
            operations: vec![Match, Match, Match, Subst, Ins, Ins, Del, Del],
            mode: AlignmentMode::Semiglobal,
        };
        assert_eq!(alignment.cigar(false), "3S3=1X2I2D1S");

        let alignment = Alignment {
            score: 5,
            xstart: 0,
            ystart: 5,
            xend: 4,
            yend: 10,
            ylen: 10,
            xlen: 5,
            operations: vec![Yclip(5), Match, Subst, Subst, Ins, Del, Del, Xclip(1)],
            mode: AlignmentMode::Custom,
        };
        assert_eq!(alignment.cigar(false), "1=2X1I2D1S");
        assert_eq!(alignment.cigar(true), "1=2X1I2D1H");

        let alignment = Alignment {
            score: 5,
            xstart: 0,
            ystart: 5,
            xend: 3,
            yend: 8,
            ylen: 10,
            xlen: 3,
            operations: vec![Yclip(5), Subst, Match, Subst, Yclip(2)],
            mode: AlignmentMode::Custom,
        };
        assert_eq!(alignment.cigar(false), "1X1=1X");

        let alignment = Alignment {
            score: 5,
            xstart: 0,
            ystart: 5,
            xend: 3,
            yend: 8,
            ylen: 10,
            xlen: 3,
            operations: vec![Subst, Match, Subst],
            mode: AlignmentMode::Semiglobal,
        };
        assert_eq!(alignment.cigar(false), "1X1=1X");
    }

    #[test]
    fn test_pretty() {
        let alignment = Alignment {
            score: 1,
            xstart: 0,
            ystart: 2,
            xend: 3,
            yend: 5,
            ylen: 7,
            xlen: 2,
            operations: vec![Subst, Match, Match],
            mode: AlignmentMode::Semiglobal,
        };
        let pretty = concat!("  GAT  \n", "  \\||  \n", "CTAATCC\n", "\n\n");
        assert_eq!(alignment.pretty(b"GAT", b"CTAATCC", 100), pretty);
        let alignment = Alignment {
            score: 5,
            xstart: 0,
            ystart: 5,
            xend: 4,
            yend: 10,
            ylen: 10,
            xlen: 5,
            operations: vec![Yclip(5), Match, Subst, Subst, Ins, Del, Del, Xclip(1)],
            mode: AlignmentMode::Custom,
        };
        let pretty = concat!("     AAAA--A\n     |\\\\+xx \nTTTTTTTT-TT \n\n\n");
        assert_eq!(alignment.pretty(b"AAAAA", b"TTTTTTTTTT", 100), pretty);
    }

    #[test]
    fn test_path() {
        let alignment = Alignment {
            score: 5,
            xstart: 3,
            ystart: 0,
            xend: 9,
            yend: 10,
            ylen: 10,
            xlen: 10,
            operations: vec![Match, Match, Match, Subst, Ins, Ins, Del, Del],
            mode: AlignmentMode::Semiglobal,
        };
        assert_eq!(
            alignment.path(),
            [
                (4, 5, Match),
                (5, 6, Match),
                (6, 7, Match),
                (7, 8, Subst),
                (8, 8, Ins),
                (9, 8, Ins),
                (9, 9, Del),
                (9, 10, Del)
            ]
        )
    }
}
