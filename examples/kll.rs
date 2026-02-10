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
use sketches::kll::KllSketch;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Build a KLL sketch for quantile estimation.
    let mut sketch = KllSketch::new(200)?;

    // Feed a synthetic latency stream in milliseconds.
    for latency_ms in 1_u64..=20_000 {
        sketch.add(latency_ms as f64);
    }

    // Query common quantiles.
    let p50 = sketch.quantile(0.50)?;
    let p95 = sketch.quantile(0.95)?;
    let p99 = sketch.quantile(0.99)?;

    println!("Estimated p50: {:.2} ms", p50);
    println!("Estimated p95: {:.2} ms", p95);
    println!("Estimated p99: {:.2} ms", p99);

    Ok(())
}
