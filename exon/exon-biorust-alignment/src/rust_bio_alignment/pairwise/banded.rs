// Copyright 2014-2015 Johannes Köster, Vadim Nazarov, Patrick Marks
// Licensed under the MIT license (http://opensource.org/licenses/MIT)
// This file may not be copied, modified, or distributed
// except according to those terms.

//! Banded Smith-Waterman alignment for fast comparison of long strings.
//! Use sparse dynamic programming to find a 'backbone' alignment from exact
//! k-mer matches, then compute the SW alignment in a 'band' surrounding the
//! backbone, with a configurable width w. This method is not guaranteed
//! to recover the Smith-Waterman alignment, but will usually find the same
//! alignment if a) there is a reasonable density of exact k-mer matches
//! between the sequences, and b) the width parameter w is larger than the
//! excursion of the alignment path from diagonal between successive kmer
//! matches.  This technique is employed in long-read aligners (e.g. BLASR and BWA)
//! to drastically reduce runtime compared to Smith Waterman.
//! Complexity roughly O(min(m,n) * w)
//!
//! # Example
//!
//! ```
//! use bio::alignment::pairwise::banded::*;
//! use bio::alignment::pairwise::{Scoring, MIN_SCORE};
//! use bio::alignment::sparse::hash_kmers;
//! use bio::alignment::AlignmentOperation::*;
//! use std::iter::repeat;
//!
//! let x = b"AGCACACGTGTGCGCTATACAGTAAGTAGTAGTACACGTGTCACAGTTGTACTAGCATGAC";
//! let y = b"AGCACACGTGTGCGCTATACAGTACACGTGTCACAGTTGTACTAGCATGAC";
//! let score = |a: u8, b: u8| if a == b { 1i32 } else { -1i32 };
//! let k = 8; // kmer match length
//! let w = 6; // Window size for creating the band
//! let mut aligner = Aligner::new(-5, -1, score, k, w);
//! let alignment = aligner.local(x, y);
//! // aligner.global(x, y), aligner.semiglobal(x, y) are also supported
//! assert_eq!(alignment.ystart, 0);
//! assert_eq!(alignment.xstart, 0);
//!
//! // For cases where the reference is reused multiple times, we can invoke the
//! // pre-hashed version of the solver
//! let x = b"AGCACAAGTGTGCGCTATACAGGAAGTAGGAGTACACGTGTCA";
//! let y = b"CAGTTGTACTAGCATGACCAGTTGTACTAGCATGACAGCACACGTGTGCGCTATACAGTAAGTAGTAGTACACGTGTCA\
//!     CAGTTGTACTAGCATGACCAGTTGTACTAGCATGAC";
//! let y_kmers_hash = hash_kmers(y, k);
//! let alignment = aligner.semiglobal_with_prehash(x, y, &y_kmers_hash);
//! assert_eq!(alignment.score, 37);
//!
//! // In addition to the standard modes (Global, Semiglobal and Local), a custom alignment
//! // mode is supported which supports a user-specified clipping penalty. Clipping is a
//! // special boundary condition where you are allowed to clip off the beginning/end of
//! // the sequence for a fixed penalty. See bio::alignment::pairwise for a more detailed
//! // explanation
//!
//! // The following example considers a modification of the semiglobal mode where you are allowed
//! // to skip a prefix of the target sequence x, for a penalty of -10, but you have to consume
//! // the rest of the string in the alignment
//!
//! let scoring = Scoring {
//!     gap_open: -5,
//!     gap_extend: -1,
//!     match_fn: |a: u8, b: u8| if a == b { 1i32 } else { -3i32 },
//!     match_scores: Some((1, -3)),
//!     xclip_prefix: -10,
//!     xclip_suffix: MIN_SCORE,
//!     yclip_prefix: 0,
//!     yclip_suffix: 0,
//! };
//! let x = b"GGGGGGACGTACGTACGTGTGCATCATCATGTGCGTATCATAGATAGATGTAGATGATCCACAGT";
//! let y = b"AAAAACGTACGTACGTGTGCATCATCATGTGCGTATCATAGATAGATGTAGATGATCCACAGTAAAA";
//! let mut aligner = Aligner::with_capacity_and_scoring(x.len(), y.len(), scoring, k, w);
//! let alignment = aligner.custom(x, y);
//! println!("{}", alignment.pretty(x, y, 80));
//! assert_eq!(alignment.score, 49);
//! let mut correct_ops = Vec::new();
//! correct_ops.push(Yclip(4));
//! correct_ops.push(Xclip(6));
//! correct_ops.extend(repeat(Match).take(59));
//! correct_ops.push(Yclip(4));
//! assert_eq!(alignment.operations, correct_ops);
//!
//! // aligner.custom_with_prehash(x, y, &y_kmers_hash) is also supported
//! ```

use super::TextSlice;
use std::cmp::{max, min, Ordering};
use std::collections::HashMap;
use std::hash::BuildHasherDefault;
use std::i32;
use std::ops::Range;

use super::*;

const MAX_CELLS: usize = 5_000_000;
const DEFAULT_MATCH_SCORE: i32 = 2;

pub type HashMapFx<K, V> = HashMap<K, V, BuildHasherDefault<fxhash::FxHasher>>;

/// A banded implementation of Smith-Waterman aligner (SWA).
/// Unlike the full SWA, this implementation computes the alignment between a pair of sequences
/// only inside a 'band' withing the dynamic programming matrix. The band is constructed using the
/// Sparse DP routine (see sparse::sdpkpp), which uses kmer matches to build the best common
/// subsequence (including gap penalties) between the two strings. The band is constructed around
/// this subsequence (using the window length 'w'), filling in the gaps.
///
/// In the case where there are no k-mer matches, the  aligner will fall back to a full alignment,
/// by setting the band to contain the full matrix.
///
/// Banded aligner will proceed to compute the alignment only when the total number of cells
/// in the band is less than MAX_CELLS (currently set to 10 million), otherwise it returns an
/// empty alignment
#[allow(non_snake_case)]
#[derive(Default, Clone, Eq, PartialEq, Hash, Debug, Serialize, Deserialize)]
pub struct Aligner<F: MatchFunc> {
    S: [Vec<i32>; 2],
    I: [Vec<i32>; 2],
    D: [Vec<i32>; 2],
    Lx: Vec<usize>,
    Ly: Vec<usize>,
    Sn: Vec<i32>,
    traceback: Traceback,
    scoring: Scoring<F>,

    band: Band,
    k: usize,
    w: usize,
}

const DEFAULT_ALIGNER_CAPACITY: usize = 200;

impl<F: MatchFunc> Aligner<F> {
    /// Create new aligner instance with given gap open and gap extend penalties
    /// and the score function.
    ///
    /// # Arguments
    ///
    /// * `gap_open` - the score for opening a gap (should be negative)
    /// * `gap_extend` - the score for extending a gap (should be negative)
    /// * `match_fn` - function that returns the score for substitutions (also see bio::scores)
    /// * `k` - kmer length used in constructing the band
    /// * `w` - width of the band
    pub fn new(gap_open: i32, gap_extend: i32, match_fn: F, k: usize, w: usize) -> Self {
        Aligner::with_capacity(
            DEFAULT_ALIGNER_CAPACITY,
            DEFAULT_ALIGNER_CAPACITY,
            gap_open,
            gap_extend,
            match_fn,
            k,
            w,
        )
    }

    /// Create new aligner instance. The size hints help to
    /// avoid unnecessary memory allocations.
    ///
    /// # Arguments
    ///
    /// * `m` - the expected size of x
    /// * `n` - the expected size of y
    /// * `gap_open` - the score for opening a gap (should be negative)
    /// * `gap_extend` - the score for extending a gap (should be negative)
    /// * `match_fn` - function that returns the score for substitutions (also see bio::scores)
    /// * `k` - kmer length used in constructing the band
    /// * `w` - width of the band
    pub fn with_capacity(
        m: usize,
        n: usize,
        gap_open: i32,
        gap_extend: i32,
        match_fn: F,
        k: usize,
        w: usize,
    ) -> Self {
        Aligner {
            band: Band::new(m, n),
            S: [Vec::with_capacity(m + 1), Vec::with_capacity(m + 1)],
            I: [Vec::with_capacity(m + 1), Vec::with_capacity(m + 1)],
            D: [Vec::with_capacity(m + 1), Vec::with_capacity(m + 1)],
            Lx: Vec::with_capacity(n + 1),
            Ly: Vec::with_capacity(m + 1),
            Sn: Vec::with_capacity(m + 1),
            traceback: Traceback::with_capacity(m, n),
            scoring: Scoring::new(gap_open, gap_extend, match_fn),
            k,
            w,
        }
    }

    /// Create new aligner instance with scoring and size hint. The size hints help to
    /// avoid unnecessary memory allocations.
    ///
    /// # Arguments
    ///
    /// * `m` - the expected size of x
    /// * `n` - the expected size of y
    /// * `scoring` - the scoring struct
    /// * `k` - kmer length used in constructing the band
    /// * `w` - width of the band
    pub fn with_capacity_and_scoring(
        m: usize,
        n: usize,
        scoring: Scoring<F>,
        k: usize,
        w: usize,
    ) -> Self {
        assert!(scoring.gap_open <= 0, "gap_open can't be positive");
        assert!(scoring.gap_extend <= 0, "gap_extend can't be positive");
        assert!(
            scoring.xclip_prefix <= 0,
            "Clipping penalty (x prefix) can't be positive"
        );
        assert!(
            scoring.xclip_suffix <= 0,
            "Clipping penalty (x suffix) can't be positive"
        );
        assert!(
            scoring.yclip_prefix <= 0,
            "Clipping penalty (y prefix) can't be positive"
        );
        assert!(
            scoring.yclip_suffix <= 0,
            "Clipping penalty (y suffix) can't be positive"
        );

        Aligner {
            band: Band::new(m, n),
            S: [Vec::with_capacity(m + 1), Vec::with_capacity(m + 1)],
            I: [Vec::with_capacity(m + 1), Vec::with_capacity(m + 1)],
            D: [Vec::with_capacity(m + 1), Vec::with_capacity(m + 1)],
            Lx: Vec::with_capacity(n + 1),
            Ly: Vec::with_capacity(m + 1),
            Sn: Vec::with_capacity(m + 1),
            traceback: Traceback::with_capacity(m, n),
            scoring,
            k,
            w,
        }
    }

    /// Create new aligner instance with scoring and size hint. The size hints help to
    /// avoid unnecessary memory allocations.
    ///
    /// # Arguments
    ///
    /// * `m` - the expected size of x
    /// * `n` - the expected size of y
    /// * `scoring` - the scoring struct
    /// * `k` - kmer length used in constructing the band
    /// * `w` - width of the band
    pub fn with_scoring(scoring: Scoring<F>, k: usize, w: usize) -> Self {
        Aligner::with_capacity_and_scoring(
            DEFAULT_ALIGNER_CAPACITY,
            DEFAULT_ALIGNER_CAPACITY,
            scoring,
            k,
            w,
        )
    }

    /// Return a mutable reference to scoring. Useful if you want to have a
    /// single aligner object but want to modify the scores within it for
    /// different cases
    pub fn get_mut_scoring(&mut self) -> &mut Scoring<F> {
        &mut self.scoring
    }

    /// Compute the alignment with custom clip penalties
    ///
    /// # Arguments
    ///
    /// * `x` - Textslice
    /// * `y` - Textslice
    pub fn custom(&mut self, x: TextSlice<'_>, y: TextSlice<'_>) -> Alignment {
        self.band = Band::create(x, y, self.k, self.w, &self.scoring);
        self.compute_alignment(x, y)
    }

    /// Compute the alignment with custom clip penalties with 'y' being pre-hashed
    /// (see sparse::hash_kmers)
    ///
    /// # Arguments
    ///
    /// * `x` - Textslice
    /// * `y` - Textslice
    pub fn custom_with_prehash(
        &mut self,
        x: TextSlice<'_>,
        y: TextSlice<'_>,
        y_kmer_hash: &HashMapFx<&[u8], Vec<u32>>,
    ) -> Alignment {
        self.band = Band::create_with_prehash(x, y, self.k, self.w, &self.scoring, y_kmer_hash);
        self.compute_alignment(x, y)
    }

    /// Compute the alignment with custom clip penalties with the kmer matches
    /// between x and y being pre-computed as a Vector of pairs (xpos, ypos)
    /// and sorted.
    ///
    /// # Arguments
    ///
    /// * `x` - Textslice
    /// * `y` - Textslice
    /// * `matches` - Vector of kmer matching pairs (xpos, ypos)
    pub fn custom_with_matches(
        &mut self,
        x: TextSlice<'_>,
        y: TextSlice<'_>,
        matches: &[(u32, u32)],
    ) -> Alignment {
        self.band = Band::create_with_matches(x, y, self.k, self.w, &self.scoring, matches);
        self.compute_alignment(x, y)
    }

    /// Compute the alignment with custom clip penalties by constructing
    /// a band along the `matches` as defined by `path`. This is only
    /// for advanced uses, where one would want to control the kmer
    /// backbone that is used for creating the band.
    ///
    /// # Arguments
    ///
    /// * `x` - Textslice
    /// * `y` - Textslice
    /// * `matches` - Vector of kmer matching pairs (xpos, ypos)
    /// * `path` - Vector of indices pointing to `matches` vector
    /// which defines a path. The validity of the path is not checked.
    pub fn custom_with_match_path(
        &mut self,
        x: TextSlice,
        y: TextSlice,
        matches: &[(u32, u32)],
        path: &[usize],
    ) -> Alignment {
        self.band =
            Band::create_from_match_path(x, y, self.k, self.w, &self.scoring, path, matches);
        self.compute_alignment(x, y)
    }

    // Computes the alignment. The band needs to be populated prior
    // to calling this function
    #[inline(never)]
    fn compute_alignment(&mut self, x: TextSlice<'_>, y: TextSlice<'_>) -> Alignment {
        if self.band.num_cells() > MAX_CELLS {
            // Too many cells in the band. Return an empty alignment
            return Alignment {
                score: MIN_SCORE,
                ystart: 0,
                xstart: 0,
                yend: 0,
                xend: 0,
                ylen: 0,
                xlen: 0,
                operations: Vec::new(),
                mode: AlignmentMode::Custom,
            };
        }

        let (m, n) = (x.len(), y.len());
        self.traceback.init(m, n);

        for k in 0..2 {
            self.I[k].clear();
            self.D[k].clear();
            self.S[k].clear();
            self.D[k].extend(repeat(MIN_SCORE).take(m + 1));
            self.I[k].extend(repeat(MIN_SCORE).take(m + 1));
            self.S[k].extend(repeat(MIN_SCORE).take(m + 1));
        }
        self.Lx.clear();
        self.Lx.extend(repeat(0usize).take(n + 1));
        self.Ly.clear();
        self.Ly.extend(repeat(0usize).take(m + 1));
        self.Sn.clear();
        self.Sn.extend(repeat(MIN_SCORE).take(m + 1));

        {
            // Handle j = 0
            let curr = 0;
            let i_start = self.band.ranges[0].start;
            let i_end = self.band.ranges[0].end;
            if i_start == 0 {
                self.S[curr][0] = 0;
            }

            for i in max(1, i_start)..i_end {
                let mut tb = TracebackCell::new();
                tb.set_all(TB_START);
                if i == 1 {
                    self.I[curr][i] = self.scoring.gap_open + self.scoring.gap_extend;
                    tb.set_i_bits(TB_START);
                } else {
                    // Insert all i characters
                    let i_score = self.scoring.gap_open + self.scoring.gap_extend * (i as i32);
                    let c_score =
                        self.scoring.xclip_prefix + self.scoring.gap_open + self.scoring.gap_extend; // Clip then insert
                    if i_score > c_score {
                        self.I[curr][i] = i_score;
                        tb.set_i_bits(TB_INS);
                    } else {
                        self.I[curr][i] = c_score;
                        tb.set_i_bits(TB_XCLIP_PREFIX);
                    }
                }

                if i == m {
                    tb.set_s_bits(TB_XCLIP_SUFFIX);
                }

                if self.I[curr][i] > self.S[curr][i] {
                    self.S[curr][i] = self.I[curr][i];
                    tb.set_s_bits(TB_INS);
                }

                if self.scoring.xclip_prefix > self.S[curr][i] {
                    self.S[curr][i] = self.scoring.xclip_prefix;
                    tb.set_s_bits(TB_XCLIP_PREFIX);
                }

                // Track the score if we do a suffix clip (x) after this character
                if self.S[curr][i] + self.scoring.xclip_suffix > self.S[curr][m] {
                    self.S[curr][m] = self.S[curr][i] + self.scoring.xclip_suffix;
                    self.Lx[0] = m - i;
                    self.traceback.get_mut(m, 0).set_s_bits(TB_XCLIP_SUFFIX);
                }

                self.traceback.set(i, 0, tb);
            }

            for i in i_end..min(m + 1, self.band.ranges[min(n, 1)].end) {
                self.S[curr][i] = MIN_SCORE;
                self.I[curr][i] = MIN_SCORE;
            }

            if i_end < (m + 1) {
                self.S[curr][m] = MIN_SCORE;
            }
            // Track the score if we do clip (y) from origin
            if self.scoring.yclip_prefix > self.scoring.yclip_suffix {
                self.Sn[0] = self.scoring.yclip_prefix;
                self.traceback.get_mut(0, n).set_s_bits(TB_YCLIP_PREFIX);
            } else {
                self.Sn[0] = self.scoring.yclip_suffix;
                self.Ly[0] = n;
                self.traceback.get_mut(0, n).set_s_bits(TB_YCLIP_SUFFIX);
            }
        }

        for j in 1..=n {
            let curr = j % 2;
            let prev = 1 - curr;

            let i_start = self.band.ranges[j].start;
            let i_end = self.band.ranges[j].end;

            if i_start == 0 {
                // Handle i = 0
                let mut tb = TracebackCell::new();
                self.I[curr][0] = MIN_SCORE;

                if j == 1 {
                    self.D[curr][0] = self.scoring.gap_open + self.scoring.gap_extend;
                    tb.set_d_bits(TB_START);
                } else {
                    // Delete all j characters
                    let d_score = self.scoring.gap_open + self.scoring.gap_extend * (j as i32);
                    let c_score =
                        self.scoring.yclip_prefix + self.scoring.gap_open + self.scoring.gap_extend;
                    if d_score > c_score {
                        self.D[curr][0] = d_score;
                        tb.set_d_bits(TB_DEL);
                    } else {
                        self.D[curr][0] = c_score;
                        tb.set_d_bits(TB_YCLIP_PREFIX);
                    }
                }

                if self.D[curr][0] > self.scoring.yclip_prefix {
                    self.S[curr][0] = self.D[curr][0];
                    tb.set_s_bits(TB_DEL);
                } else {
                    self.S[curr][0] = self.scoring.yclip_prefix;
                    tb.set_s_bits(TB_YCLIP_PREFIX);
                }

                // Track the score if we do suffix clip (y) from here
                if self.S[curr][0] + self.scoring.yclip_suffix > self.Sn[0] {
                    self.Sn[0] = self.S[curr][0] + self.scoring.yclip_suffix;
                    self.Ly[0] = n - j;
                    self.traceback.get_mut(0, n).set_s_bits(TB_YCLIP_SUFFIX);
                }
                self.traceback.set(0, j, tb);
            }

            for i in i_start.saturating_sub(1)..i_start {
                self.S[curr][i] = MIN_SCORE;
                self.I[curr][i] = MIN_SCORE;
                self.D[curr][i] = MIN_SCORE;
            }
            self.S[curr][m] = MIN_SCORE;

            let q = y[j - 1];
            let xclip_score = self.scoring.xclip_prefix
                + max(
                    if j == n {
                        max(self.scoring.yclip_prefix, self.Sn[0])
                    } else {
                        self.scoring.yclip_prefix
                    },
                    self.scoring.gap_open + self.scoring.gap_extend * (j as i32),
                );

            for i in max(1, i_start)..i_end {
                let p = x[i - 1];
                let mut tb = TracebackCell::new();

                let m_score = self.S[prev][i - 1] + self.scoring.match_fn.score(p, q);

                let i_score = self.I[curr][i - 1] + self.scoring.gap_extend;
                let s_score = self.S[curr][i - 1] + self.scoring.gap_open + self.scoring.gap_extend;
                let mut best_i_score;
                if i_score > s_score {
                    best_i_score = i_score;
                    tb.set_i_bits(TB_INS);
                } else {
                    best_i_score = s_score;
                    tb.set_i_bits(self.traceback.get(i - 1, j).get_s_bits());
                }
                if j == n {
                    let clip_score =
                        self.Sn[i - 1] + self.scoring.gap_open + self.scoring.gap_extend;
                    if clip_score > best_i_score {
                        best_i_score = clip_score;
                        tb.set_i_bits(TB_YCLIP_SUFFIX);
                    }
                }

                let d_score = self.D[prev][i] + self.scoring.gap_extend;
                let s_score = self.S[prev][i] + self.scoring.gap_open + self.scoring.gap_extend;
                let best_d_score;
                if d_score > s_score {
                    best_d_score = d_score;
                    tb.set_d_bits(TB_DEL);
                } else {
                    best_d_score = s_score;
                    tb.set_d_bits(self.traceback.get(i, j - 1).get_s_bits());
                }

                if i == m {
                    tb.set_s_bits(TB_XCLIP_SUFFIX);
                } else {
                    self.S[curr][i] = MIN_SCORE;
                }
                let mut best_s_score = self.S[curr][i];

                if m_score > best_s_score {
                    best_s_score = m_score;
                    tb.set_s_bits(if p == q { TB_MATCH } else { TB_SUBST });
                }

                if best_i_score > best_s_score {
                    best_s_score = best_i_score;
                    tb.set_s_bits(TB_INS);
                }

                if best_d_score > best_s_score {
                    best_s_score = best_d_score;
                    tb.set_s_bits(TB_DEL);
                }

                if xclip_score > best_s_score {
                    best_s_score = xclip_score;
                    tb.set_s_bits(TB_XCLIP_PREFIX);
                }

                let yclip_score = self.scoring.yclip_prefix
                    + self.scoring.gap_open
                    + self.scoring.gap_extend * (i as i32);
                if yclip_score > best_s_score {
                    best_s_score = yclip_score;
                    tb.set_s_bits(TB_YCLIP_PREFIX);
                }

                self.S[curr][i] = best_s_score;
                self.I[curr][i] = best_i_score;
                self.D[curr][i] = best_d_score;

                // Track the score if we do suffix clip (x) from here
                if self.S[curr][i] + self.scoring.xclip_suffix > self.S[curr][m] {
                    self.S[curr][m] = self.S[curr][i] + self.scoring.xclip_suffix;
                    self.Lx[j] = m - i;
                    self.traceback.get_mut(m, j).set_s_bits(TB_XCLIP_SUFFIX);
                }

                // Track the score if we do suffix clip (y) from here
                if self.S[curr][i] + self.scoring.yclip_suffix > self.Sn[i] {
                    self.Sn[i] = self.S[curr][i] + self.scoring.yclip_suffix;
                    self.Ly[i] = n - j;
                    self.traceback.get_mut(i, n).set_s_bits(TB_YCLIP_SUFFIX);
                }

                self.traceback.set(i, j, tb);
            }

            // Suffix clip (y) from i = m and reset Sn[m] if required
            if self.S[curr][m] + self.scoring.yclip_suffix > self.Sn[m] {
                self.Sn[m] = self.S[curr][m] + self.scoring.yclip_suffix;
                self.Ly[m] = n - j;
                self.traceback.get_mut(m, n).set_s_bits(TB_YCLIP_SUFFIX);
            }
            if i_end < (m + 1) {
                self.traceback.get_mut(m, j).set_s_bits(TB_XCLIP_SUFFIX);
                self.S[curr][m] = MIN_SCORE;
            }

            for i in i_end..min(m + 1, self.band.ranges[min(n, j + 1)].end) {
                self.S[curr][i] = MIN_SCORE;
                self.I[curr][i] = MIN_SCORE;
                self.D[curr][i] = MIN_SCORE;
            }
        }

        // Handle suffix clipping in the j=n case
        for i in 0..=m {
            let j = n;
            let curr = j % 2;
            // These entries are not set in the loop above and could contain leftover
            // values from previous columns. Reset them to MIN_SCORE
            if i != m && (i < self.band.ranges[j].start || i > self.band.ranges[j].end) {
                self.S[curr][i] = MIN_SCORE;
            }
            if self.Sn[i] > self.S[curr][i] {
                self.S[curr][i] = self.Sn[i];
                self.traceback.get_mut(i, j).set_s_bits(TB_YCLIP_SUFFIX);
            }
            if self.S[curr][i] + self.scoring.xclip_suffix > self.S[curr][m] {
                self.S[curr][m] = self.S[curr][i] + self.scoring.xclip_suffix;
                self.Lx[j] = m - i;
                self.traceback.get_mut(m, j).set_s_bits(TB_XCLIP_SUFFIX);
            }
        }

        // Since there could be a change in the last column of S,
        // recompute the last column of I as this could also change
        for i in max(1, self.band.ranges[n].start)..self.band.ranges[n].end {
            let j = n;
            let curr = j % 2;
            let s_score = self.S[curr][i - 1] + self.scoring.gap_open + self.scoring.gap_extend;
            if s_score > self.I[curr][i] {
                self.I[curr][i] = s_score;
                let s_bit = self.traceback.get(i - 1, j).get_s_bits();
                self.traceback.get_mut(i, j).set_i_bits(s_bit);
            }
            if s_score > self.S[curr][i] {
                self.S[curr][i] = s_score;
                self.traceback.get_mut(i, j).set_s_bits(TB_INS);
                if self.S[curr][i] + self.scoring.xclip_suffix > self.S[curr][m] {
                    self.S[curr][m] = self.S[curr][i] + self.scoring.xclip_suffix;
                    self.Lx[j] = m - i;
                    self.traceback.get_mut(m, j).set_s_bits(TB_XCLIP_SUFFIX);
                }
            }
        }

        for j in 1..=n {
            let d_score = self.scoring.gap_open + self.scoring.gap_extend * (j as i32);
            if d_score > self.scoring.yclip_prefix {
                self.traceback.get_mut(0, j).set_s_bits(TB_DEL);
            } else {
                self.traceback.get_mut(0, j).set_s_bits(TB_YCLIP_PREFIX);
            }
            if j == n {
                let mut best_score = max(d_score, self.scoring.yclip_prefix);
                if self.scoring.yclip_suffix > best_score {
                    best_score = self.scoring.yclip_suffix;
                    self.traceback.get_mut(0, j).set_s_bits(TB_YCLIP_SUFFIX);
                }
                if (self.scoring.xclip_suffix + best_score) > self.S[n % 2][m] {
                    self.S[n % 2][m] = self.scoring.xclip_suffix + best_score;
                    self.Lx[n] = m;
                    self.traceback.get_mut(m, n).set_s_bits(TB_XCLIP_SUFFIX);
                }
            }
        }

        for i in 1..=m {
            let c_score = self.scoring.gap_open + self.scoring.gap_extend * (i as i32);
            if c_score > self.scoring.xclip_prefix {
                self.traceback.get_mut(i, 0).set_s_bits(TB_INS);
            } else {
                self.traceback.get_mut(i, 0).set_s_bits(TB_XCLIP_PREFIX);
            }
            if i == m {
                let mut best_score = max(c_score, self.scoring.xclip_prefix);
                if self.scoring.xclip_suffix > best_score {
                    best_score = self.scoring.xclip_suffix;
                    self.traceback.get_mut(i, 0).set_s_bits(TB_XCLIP_SUFFIX);
                }
                if (self.scoring.yclip_suffix + best_score) > self.S[n % 2][m] {
                    self.S[n % 2][m] = self.scoring.yclip_suffix + best_score;
                    self.Ly[m] = n;
                    self.traceback.get_mut(m, n).set_s_bits(TB_YCLIP_SUFFIX);
                }
            }
        }

        let mut i = m;
        let mut j = n;
        let mut operations = Vec::with_capacity(x.len());
        let mut xstart: usize = 0usize;
        let mut ystart: usize = 0usize;
        let mut xend = m;
        let mut yend = n;

        let mut last_layer = self.traceback.get(i, j).get_s_bits();

        loop {
            let next_layer: u16;
            match last_layer {
                TB_START => break,
                TB_INS => {
                    operations.push(AlignmentOperation::Ins);
                    next_layer = self.traceback.get(i, j).get_i_bits();
                    i -= 1;
                }
                TB_DEL => {
                    operations.push(AlignmentOperation::Del);
                    next_layer = self.traceback.get(i, j).get_d_bits();
                    j -= 1;
                }
                TB_MATCH => {
                    operations.push(AlignmentOperation::Match);
                    next_layer = self.traceback.get(i - 1, j - 1).get_s_bits();
                    i -= 1;
                    j -= 1;
                }
                TB_SUBST => {
                    operations.push(AlignmentOperation::Subst);
                    next_layer = self.traceback.get(i - 1, j - 1).get_s_bits();
                    i -= 1;
                    j -= 1;
                }
                TB_XCLIP_PREFIX => {
                    operations.push(AlignmentOperation::Xclip(i));
                    xstart = i;
                    i = 0;
                    next_layer = self.traceback.get(0, j).get_s_bits();
                }
                TB_XCLIP_SUFFIX => {
                    operations.push(AlignmentOperation::Xclip(self.Lx[j]));
                    i -= self.Lx[j];
                    xend = i;
                    next_layer = self.traceback.get(i, j).get_s_bits();
                }
                TB_YCLIP_PREFIX => {
                    operations.push(AlignmentOperation::Yclip(j));
                    ystart = j;
                    j = 0;
                    next_layer = self.traceback.get(i, 0).get_s_bits();
                }
                TB_YCLIP_SUFFIX => {
                    operations.push(AlignmentOperation::Yclip(self.Ly[i]));
                    j -= self.Ly[i];
                    yend = j;
                    next_layer = self.traceback.get(i, j).get_s_bits();
                }
                _ => panic!("Dint expect this!"),
            }
            last_layer = next_layer;
            // println!("{} of {}, {} of {} - {}", i, m, j, n, last_layer);
        }

        // Handle the case when the traceback ends outside the band other than at (0, 0)
        if i != 0 {
            // Insert all i characters
            let i_score = self.scoring.gap_open + self.scoring.gap_extend * (i as i32);
            if i_score > self.scoring.xclip_prefix {
                operations.resize(operations.len() + i, AlignmentOperation::Ins);
                xstart = 0;
            } else {
                operations.push(AlignmentOperation::Xclip(i));
                xstart = i;
            }
        }
        if j != 0 {
            // Delete all j characters
            let d_score = self.scoring.gap_open + self.scoring.gap_extend * (j as i32);
            if d_score > self.scoring.yclip_prefix {
                operations.resize(operations.len() + j, AlignmentOperation::Del);
                ystart = 0;
            } else {
                operations.push(AlignmentOperation::Yclip(j));
                ystart = j;
            }
        }

        operations.reverse();
        Alignment {
            score: self.S[n % 2][m],
            ystart,
            xstart,
            yend,
            xend,
            ylen: n,
            xlen: m,
            operations,
            mode: AlignmentMode::Custom,
        }
    }

    /// Calculate global alignment of x against y.
    pub fn global(&mut self, x: TextSlice<'_>, y: TextSlice<'_>) -> Alignment {
        // Store the current clip penalties
        let clip_penalties = [
            self.scoring.xclip_prefix,
            self.scoring.xclip_suffix,
            self.scoring.yclip_prefix,
            self.scoring.yclip_suffix,
        ];

        // Temporarily Over-write the clip penalties
        self.scoring.xclip_prefix = MIN_SCORE;
        self.scoring.xclip_suffix = MIN_SCORE;
        self.scoring.yclip_prefix = MIN_SCORE;
        self.scoring.yclip_suffix = MIN_SCORE;

        // Compute the alignment
        let mut alignment = self.custom(x, y);
        alignment.mode = AlignmentMode::Global;

        // Set the clip penalties to the original values
        self.scoring.xclip_prefix = clip_penalties[0];
        self.scoring.xclip_suffix = clip_penalties[1];
        self.scoring.yclip_prefix = clip_penalties[2];
        self.scoring.yclip_suffix = clip_penalties[3];

        alignment
    }

    /// Calculate semiglobal alignment of x against y (x is global, y is local).
    pub fn semiglobal(&mut self, x: TextSlice<'_>, y: TextSlice<'_>) -> Alignment {
        // Store the current clip penalties
        let clip_penalties = [
            self.scoring.xclip_prefix,
            self.scoring.xclip_suffix,
            self.scoring.yclip_prefix,
            self.scoring.yclip_suffix,
        ];

        // Temporarily Over-write the clip penalties
        self.scoring.xclip_prefix = MIN_SCORE;
        self.scoring.xclip_suffix = MIN_SCORE;
        self.scoring.yclip_prefix = 0;
        self.scoring.yclip_suffix = 0;

        // Compute the alignment
        let mut alignment = self.custom(x, y);
        alignment.mode = AlignmentMode::Semiglobal;

        // Filter out Xclip and Yclip from alignment.operations
        alignment.filter_clip_operations();

        // Set the clip penalties to the original values
        self.scoring.xclip_prefix = clip_penalties[0];
        self.scoring.xclip_suffix = clip_penalties[1];
        self.scoring.yclip_prefix = clip_penalties[2];
        self.scoring.yclip_suffix = clip_penalties[3];

        alignment
    }

    /// Calculate semiglobal alignment of x against y (x is global, y is local).
    /// This function accepts the hash map of the kmers of y. This is useful
    /// in cases where we are interested in repeated alignment of different
    /// queries against the same reference. The user can precompute the HashMap
    /// using sparse::hash_kmers and invoke this function to speed up the
    /// alignment computation.
    pub fn semiglobal_with_prehash(
        &mut self,
        x: TextSlice<'_>,
        y: TextSlice<'_>,
        y_kmer_hash: &HashMapFx<&[u8], Vec<u32>>,
    ) -> Alignment {
        // Store the current clip penalties
        let clip_penalties = [
            self.scoring.xclip_prefix,
            self.scoring.xclip_suffix,
            self.scoring.yclip_prefix,
            self.scoring.yclip_suffix,
        ];

        // Temporarily Over-write the clip penalties
        self.scoring.xclip_prefix = MIN_SCORE;
        self.scoring.xclip_suffix = MIN_SCORE;
        self.scoring.yclip_prefix = 0;
        self.scoring.yclip_suffix = 0;

        // Compute the alignment
        let mut alignment = self.custom_with_prehash(x, y, y_kmer_hash);
        alignment.mode = AlignmentMode::Semiglobal;

        // Filter out Xclip and Yclip from alignment.operations
        alignment.filter_clip_operations();

        // Set the clip penalties to the original values
        self.scoring.xclip_prefix = clip_penalties[0];
        self.scoring.xclip_suffix = clip_penalties[1];
        self.scoring.yclip_prefix = clip_penalties[2];
        self.scoring.yclip_suffix = clip_penalties[3];

        alignment
    }

    /// Calculate local alignment of x against y.
    pub fn local(&mut self, x: TextSlice<'_>, y: TextSlice<'_>) -> Alignment {
        // Store the current clip penalties
        let clip_penalties = [
            self.scoring.xclip_prefix,
            self.scoring.xclip_suffix,
            self.scoring.yclip_prefix,
            self.scoring.yclip_suffix,
        ];

        // Temporarily Over-write the clip penalties
        self.scoring.xclip_prefix = 0;
        self.scoring.xclip_suffix = 0;
        self.scoring.yclip_prefix = 0;
        self.scoring.yclip_suffix = 0;

        // Compute the alignment
        let mut alignment = self.custom(x, y);
        alignment.mode = AlignmentMode::Local;

        // Filter out Xclip and Yclip from alignment.operations
        alignment.filter_clip_operations();

        // Set the clip penalties to the original values
        self.scoring.xclip_prefix = clip_penalties[0];
        self.scoring.xclip_suffix = clip_penalties[1];
        self.scoring.yclip_prefix = clip_penalties[2];
        self.scoring.yclip_suffix = clip_penalties[3];

        alignment
    }

    #[allow(dead_code)]
    pub fn visualize(&self, alignment: &Alignment) {
        // First populate the band
        let mut view = vec!['.'; self.band.rows * self.band.cols];
        let index = |i, j| i * self.band.cols + j;
        for j in 0..self.band.ranges.len() {
            let range = &self.band.ranges[j];
            for i in range.start..range.end {
                view[index(i, j)] = 'x';
            }
        }

        // Populate the path
        let path = alignment.path();
        for p in path {
            view[index(p.0, p.1)] = '\\';
        }

        for i in 0..self.band.rows {
            for j in 0..self.band.cols {
                print!("{}", view[index(i, j)]);
            }
            println!();
        }
    }
}

trait MatchPair {
    fn continues(&self, p: Option<(u32, u32)>) -> bool;
}

impl MatchPair for (u32, u32) {
    fn continues(&self, p: Option<(u32, u32)>) -> bool {
        match p {
            Some(_p) => self.0 == _p.0 + 1 && self.1 == _p.1 + 1,
            None => false,
        }
    }
}

#[derive(Default, Clone, Eq, PartialEq, Hash, Debug, Serialize, Deserialize)]
struct Band {
    rows: usize,
    cols: usize,
    ranges: Vec<Range<usize>>,
}

impl Band {
    // Create new Band instance with given size
    //
    // # Arguments
    //
    // * `m` - the expected size of x
    // * `n` - the expected size of y
    //
    fn new(m: usize, n: usize) -> Self {
        Band {
            rows: m + 1,
            cols: n + 1,
            ranges: vec![m + 1..0; n + 1],
        }
    }

    // Add cells around a kmer of length 'k', starting at 'start', which are within a
    // distance of 'w' in x or y directions to the band.
    fn add_kmer(&mut self, start: (u32, u32), k: usize, w: usize) {
        let (r, c) = (start.0 as usize, start.1 as usize);
        // println!("{} {} {}", r, k, self.rows);
        debug_assert!(r + k <= self.rows);
        debug_assert!(c + k <= self.cols);

        if k == 0 {
            return;
        }

        let i = r.saturating_sub(w);
        for j in c.saturating_sub(w)..min(c + w + 1, self.cols) {
            self.ranges[j].start = min(self.ranges[j].start, i);
        }

        let mut i = r.saturating_sub(w);
        for j in min(c + w, self.cols)..min(c + k + w, self.cols) {
            self.ranges[j].start = min(self.ranges[j].start, i);
            i += 1;
        }

        let mut i = r + w + k;
        let mut j = (c + k - 1).saturating_sub(w);
        loop {
            if j <= c.saturating_sub(w) {
                break;
            }
            j -= 1;
            i -= 1;
            self.ranges[j].end = max(self.ranges[j].end, min(i, self.rows));
        }

        let i = min(r + w + k, self.rows);
        for j in (c + k - 1).saturating_sub(w)..min(c + k + w, self.cols) {
            self.ranges[j].end = max(self.ranges[j].end, i);
        }
    }

    // Add cells around a specific position to the band. An cell which is within 'w' distance
    // in x or y directions are added
    fn add_entry(&mut self, pos: (u32, u32), w: usize) {
        let (r, c) = (pos.0 as usize, pos.1 as usize);

        let istart = r.saturating_sub(w);
        let iend = min(r + w + 1, self.rows);
        for j in c.saturating_sub(w)..min(c + w + 1, self.cols) {
            self.ranges[j].start = min(self.ranges[j].start, istart);
            self.ranges[j].end = max(self.ranges[j].end, iend);
        }
    }

    // Each gap generates a line from the start to end.
    fn add_gap(&mut self, start: (u32, u32), end: (u32, u32), w: usize) {
        let nrows = end.0 - start.0;
        let ncols = end.1 - start.1;
        if nrows > ncols {
            for r in start.0..end.0 {
                let c = start.1 + (end.1 - start.1) * (r - start.0) / (end.0 - start.0);
                self.add_entry((r, c), w);
            }
        } else {
            for c in start.1..end.1 {
                let r = start.0 + (end.0 - start.0) * (c - start.1) / (end.1 - start.1);
                self.add_entry((r, c), w);
            }
        }
    }

    // The band needs to start either at (0,0) or at a point that is zero score from (0,0).
    // This naturally sets the start positions correctly for global, semiglobal and local
    // modes. Similarly the band has to either end at (m,n) or at a point from which there is
    // a zero score path to (m,n).
    //
    // At the minimum, irrespective of the score (0,0)->start or end->(m,n), we extend the band
    // diagonally for a length "lazy_extend"(2k) or when it hits the corner, whichever happens first
    //
    // start - the index of the first matching kmer in LCSk++
    // end - the index of the last matching kmer in LCSk++
    //
    fn set_boundaries<F: MatchFunc>(
        &mut self,
        start: (u32, u32),
        end: (u32, u32),
        k: usize,
        w: usize,
        scoring: &Scoring<F>,
    ) {
        let lazy_extend: usize = 2 * k;

        // -------------- START --------------
        // Nothing to do if the start is already at (0,0)
        let (r, c) = (start.0 as usize, start.1 as usize);
        if !(r == 0usize && c == 0usize) {
            let mut score_to_start = if r > 0 { scoring.xclip_prefix } else { 0i32 };
            score_to_start += if c > 0 { scoring.yclip_prefix } else { 0i32 };

            if score_to_start == 0 {
                // Just do a "lazy_extend"
                // First diagonally
                let d = min(lazy_extend, min(r, c));
                self.add_kmer(((r - d) as u32, (c - d) as u32), d, w);

                // If we hit one of the edges before completing lazy_extend
                self.add_gap(
                    (
                        r.saturating_sub(lazy_extend) as u32,
                        c.saturating_sub(lazy_extend) as u32,
                    ),
                    ((r - d) as u32, (c - d) as u32),
                    w,
                );
            } else {
                // we need to find a zero cost cell

                // First try the diagonal
                let diagonal_score = match r.cmp(&c) {
                    // We will hit (r-c, 0)
                    Ordering::Greater => scoring.xclip_prefix,
                    // We will hit (0, c-r)
                    Ordering::Less => scoring.yclip_prefix,
                    Ordering::Equal => 0,
                };

                if diagonal_score == 0 {
                    let d = min(r, c);
                    self.add_kmer(((r - d) as u32, (c - d) as u32), d, w);
                    // Make sure we do at least "lazy_extend" extension
                    let start = (
                        r.saturating_sub(lazy_extend) as u32,
                        c.saturating_sub(lazy_extend) as u32,
                    );
                    let end = ((r - d) as u32, (c - d) as u32);
                    if (start.0 <= end.0) && (start.1 <= end.1) {
                        self.add_gap(start, end, w);
                    }
                } else {
                    // Band to origin
                    self.add_gap((0u32, 0u32), start, w);
                }
            }
        }

        // -------------- END --------------
        // Nothing to do if the last kmer ends at (m, n)
        let (r, c) = (end.0 as usize + k, end.1 as usize + k);
        debug_assert!(r <= self.rows);
        debug_assert!(c <= self.cols);
        if !(r == self.rows && c == self.cols) {
            let mut score_from_end = if r == self.rows {
                0
            } else {
                scoring.xclip_suffix
            };
            score_from_end += if c == self.cols {
                0
            } else {
                scoring.yclip_suffix
            };

            if score_from_end == 0 {
                // Just a lazy_extend
                let d = min(lazy_extend, min(self.rows - r, self.cols - c));
                self.add_kmer((r as u32, c as u32), d, w);

                let r1 = min(self.rows, r + d) - 1;
                let c1 = min(self.cols, c + d) - 1;
                let r2 = min(self.rows, r + lazy_extend);
                let c2 = min(self.cols, c + lazy_extend);
                if (r1 <= r2) && (c1 <= c2) {
                    self.add_gap((r1 as u32, c1 as u32), (r2 as u32, c2 as u32), w);
                }
            } else {
                // we need to find a zero cost cell

                // First try the diagonal
                let dr = self.rows - r;
                let dc = self.cols - c;
                let diagonal_score = match dr.cmp(&dc) {
                    // We will hit (r+dc, self.cols)
                    Ordering::Greater => scoring.xclip_suffix,
                    // We will hit (self.rows, c+dr)
                    Ordering::Less => scoring.yclip_suffix,
                    // We will hit the corner
                    Ordering::Equal => 0,
                };

                if diagonal_score == 0 {
                    let d = min(dr, dc);
                    self.add_kmer((r as u32, c as u32), d, w);
                    // Make sure we do at least "lazy_extend" extension
                    let r1 = min(self.rows, r + d) - 1;
                    let c1 = min(self.cols, c + d) - 1;
                    let r2 = min(self.rows, r + lazy_extend);
                    let c2 = min(self.cols, c + lazy_extend);
                    if (r1 <= r2) && (c1 <= c2) {
                        self.add_gap((r1 as u32, c1 as u32), (r2 as u32, c2 as u32), w);
                    }
                } else {
                    // Band to lower right corner
                    let rows = self.rows as u32;
                    let cols = self.cols as u32;
                    self.add_gap((r as u32, c as u32), (rows as u32, cols as u32), w);
                }
            }
        }
    }

    fn create<F: MatchFunc>(
        x: TextSlice<'_>,
        y: TextSlice<'_>,
        k: usize,
        w: usize,
        scoring: &Scoring<F>,
    ) -> Band {
        let matches = sparse::find_kmer_matches(x, y, k);
        Band::create_with_matches(x, y, k, w, scoring, &matches)
    }

    fn create_with_prehash<F: MatchFunc>(
        x: TextSlice<'_>,
        y: TextSlice<'_>,
        k: usize,
        w: usize,
        scoring: &Scoring<F>,
        y_kmer_hash: &HashMapFx<&[u8], Vec<u32>>,
    ) -> Band {
        let matches = sparse::find_kmer_matches_seq2_hashed(x, y_kmer_hash, k);
        Band::create_with_matches(x, y, k, w, scoring, &matches)
    }

    fn create_with_matches<F: MatchFunc>(
        x: TextSlice<'_>,
        y: TextSlice<'_>,
        k: usize,
        w: usize,
        scoring: &Scoring<F>,
        matches: &[(u32, u32)],
    ) -> Band {
        if matches.is_empty() {
            let mut band = Band::new(x.len(), y.len());
            band.full_matrix();
            return band;
        }

        let match_score = match scoring.match_scores {
            Some((m, _)) => m,
            None => DEFAULT_MATCH_SCORE,
        };

        let res = sparse::sdpkpp(
            matches,
            k,
            match_score as u32,
            scoring.gap_open,
            scoring.gap_extend,
        );
        Band::create_from_match_path(x, y, k, w, scoring, &res.path, matches)
    }

    fn create_from_match_path<F: MatchFunc>(
        x: TextSlice<'_>,
        y: TextSlice<'_>,
        k: usize,
        w: usize,
        scoring: &Scoring<F>,
        path: &[usize],
        matches: &[(u32, u32)],
    ) -> Band {
        let mut band = Band::new(x.len(), y.len());

        if matches.is_empty() {
            band.full_matrix();
            return band;
        }

        let ps = path[0];
        let pe = path[path.len() - 1];

        // Set the boundaries
        band.set_boundaries(matches[ps], matches[pe], k, w, scoring);
        let mut prev: Option<(u32, u32)> = None;

        for &idx in path {
            let curr = matches[idx];
            if curr.continues(prev) {
                let p = prev.unwrap();
                band.add_entry((p.0 + k as u32, p.1 + k as u32), w);
            } else {
                if let Some(p) = prev {
                    band.add_gap((p.0 + (k - 1) as u32, p.1 + (k - 1) as u32), curr, w)
                }
                band.add_kmer(curr, k, w);
            }
            prev = Some(curr);
        }
        band
    }

    fn full_matrix(&mut self) {
        self.ranges.clear();
        self.ranges.resize(self.cols, 0..self.rows);
    }

    fn num_cells(&self) -> usize {
        let mut banded_cells = 0;
        for j in 0..self.ranges.len() {
            banded_cells += self.ranges[j].end.saturating_sub(self.ranges[j].start);
        }
        banded_cells
    }

    #[allow(dead_code)]
    fn visualize(&self) {
        let mut view = vec!['.'; self.rows * self.cols];
        let index = |i, j| i * self.cols + j;
        for j in 0..self.ranges.len() {
            let range = &self.ranges[j];
            for i in range.start..range.end {
                view[index(i, j)] = 'x';
            }
        }

        for i in 0..self.rows {
            for j in 0..self.cols {
                print!("{}", view[index(i, j)]);
            }
            println!();
        }
    }

    #[allow(dead_code)]
    fn stat(&self) {
        let total_cells = self.rows * self.cols;
        let banded_cells = self.num_cells();
        let percent_cells = (banded_cells as f64) / (total_cells as f64) * 100.0;
        println!(
            " {} of {} cells are in the band ({2:.2}%)",
            banded_cells, total_cells, percent_cells
        );
    }
}
