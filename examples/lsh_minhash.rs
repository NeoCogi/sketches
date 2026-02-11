// MIT License
//
// Copyright (c) 2026 Raja Lehtihet & Wael El Oraiby
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
//
use sketches::lsh_minhash::MinHashLshIndex;
use sketches::minhash::MinHash;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Use 128 hash components split across 32 bands (4 rows per band).
    let num_hashes = 128;
    let mut index = MinHashLshIndex::new(num_hashes, 32)?;

    // Create two indexed documents as MinHash signatures.
    let mut doc_a = MinHash::new(num_hashes)?;
    let mut doc_b = MinHash::new(num_hashes)?;

    // doc_a tokens: [0, 10_000)
    for token in 0_u64..10_000 {
        doc_a.add(&token);
    }

    // doc_b tokens: [20_000, 30_000) (mostly disjoint from query below).
    for token in 20_000_u64..30_000 {
        doc_b.add(&token);
    }

    // Index them by document ids.
    index.insert(1_u64, &doc_a)?;
    index.insert(2_u64, &doc_b)?;

    // Build a query overlapping heavily with doc_a: [1_000, 11_000).
    let mut query = MinHash::new(num_hashes)?;
    for token in 1_000_u64..11_000 {
        query.add(&token);
    }

    // LSH first retrieves likely neighbors, then we can rerank with Jaccard.
    let candidates = index.query_candidates(&query)?;
    println!("Candidates: {:?}", candidates);

    let ranked = index.query_top_k(&query, 5)?;
    println!("Top matches (id, est_jaccard): {:?}", ranked);

    Ok(())
}
