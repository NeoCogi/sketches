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
use sketches::space_saving::SpaceSaving;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Track approximate heavy hitters with five counters.
    let mut sketch = SpaceSaving::new(5)?;

    // Insert weighted events for known frequent keys.
    sketch.add("apple".to_string(), 1_000);
    sketch.add("banana".to_string(), 700);
    sketch.add("carrot".to_string(), 300);

    // Add a long tail of one-off keys.
    for value in 0..2_000_u64 {
        sketch.insert(format!("noise-{value}"));
    }

    println!("Top 3 heavy hitters (item, estimate, max_error):");
    for (item, estimate, error) in sketch.top_k(3) {
        println!("  {item:>12}  {estimate:>8}  +/-{error}");
    }

    Ok(())
}
