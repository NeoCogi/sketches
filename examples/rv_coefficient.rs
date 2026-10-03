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

use sketches::rv_coefficient::RvCoefficient;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Measure multivariate correlation between two 2-dimensional streams.
    let mut first = RvCoefficient::new(2, 2)?;
    let mut second = RvCoefficient::new(2, 2)?;

    // First batch: paired vector observations
    for (x, y) in [([1.0, 2.0], [2.0, 4.0]), ([2.0, 4.0], [4.0, 8.0])] {
        first.add(&x, &y)?;
    }

    // Second batch: paired vector observations
    for (x, y) in [([4.0, 1.0], [8.0, 2.0]), ([3.0, 5.0], [6.0, 10.0])] {
        second.add(&x, &y)?;
    }

    // Combine independently accumulated batches
    first.merge(&second)?;

    println!("Observations: {}", first.count());
    let rv = first.rv_coefficient().expect("sufficient variance");
    println!("Standard RV coefficient: {:.6}", rv);

    Ok(())
}
