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

use sketches::vector_welford::VectorWelford;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Summarize two independently processed batches of paired measurements.
    let mut first = VectorWelford::new(2)?;
    let mut second = VectorWelford::new(2)?;
    for observation in [[1.0, 6.0], [2.0, 4.0]] {
        first.add(&observation)?;
    }
    for observation in [[3.0, 2.0], [4.0, 0.0]] {
        second.add(&observation)?;
    }
    first.merge(&second)?;

    println!("Observations: {}", first.count());
    println!("Mean: {:?}", first.mean().unwrap());
    println!("Sample variance: {:?}", first.sample_variance().unwrap());
    println!(
        "Sample covariance: {:?}",
        first.sample_covariance().unwrap()
    );
    Ok(())
}
