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

use sketches::minmax_sketch::MinMaxSketch;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // MinMax stores compact ordered values such as quantile-bucket indices.
    // A fixed seed makes this example reproducible; shards that will be merged
    // must use the same dimensions and seed.
    let seed = 0x3C6E_F372_FE94_F82B;
    let mut sketch = MinMaxSketch::<u8>::new(32, 3, seed)?;

    sketch.insert(&702_u64, 1);
    sketch.insert(&735_u64, 3);
    sketch.insert(&1_244_u64, 2);

    // Estimates for inserted keys never exceed their original bucket index.
    let estimate = sketch.estimate(&735_u64).expect("735 was inserted");
    println!("Estimated bucket index for key 735: {estimate}");

    Ok(())
}
