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

use std::sync::Arc;

use arrow::array::{Array, StringArray};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use datafusion::logical_expr::{ColumnarValue, ScalarUDFImpl};
use exon::udfs::sequence::reverse_complement::ReverseComplement;
use rand::Rng;

fn generate_dna_sequence(length: usize) -> String {
    let bases = ['A', 'T', 'C', 'G'];
    let mut rng = rand::thread_rng();
    (0..length).map(|_| bases[rng.gen_range(0..4)]).collect()
}

// Function to generate a vector of random DNA sequences
fn generate_dna_sequences(num_sequences: usize) -> Vec<String> {
    let mut rng = rand::thread_rng();
    (0..num_sequences)
        .map(|_| {
            let length = rng.gen_range(50..=1000);
            generate_dna_sequence(length)
        })
        .collect()
}

fn bench_reverse_complement(c: &mut Criterion) {
    let mut group = c.benchmark_group("reverse_complement");

    let size = 500;
    let sequences = generate_dna_sequences(size);
    let sequence_array = Arc::new(StringArray::from(sequences));
    let sequence_column = ColumnarValue::Array(sequence_array.clone());

    let rc = ReverseComplement::default();

    group.bench_with_input(
        BenchmarkId::new("reverse_complement", sequence_array.len()),
        &sequence_column,
        |b, s| {
            b.iter(|| {
                rc.invoke_batch(&[s.clone()], size).unwrap();
            });
        },
    );

    group.finish();
}

criterion_group!(benches, bench_reverse_complement);
criterion_main!(benches);
