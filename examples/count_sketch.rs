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
use sketches::count_sketch::CountSketch;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Build a Count Sketch tuned for moderate error/confidence.
    let mut sketch = CountSketch::new(0.05, 0.01)?;

    // Simulate signed updates from a stream with increments and decrements.
    sketch.add(&"GET /v1/users", 120);
    sketch.add(&"GET /v1/health", 15);
    sketch.add(&"GET /v1/users", -20);

    // Query the signed frequency estimate.
    let users_estimate = sketch.estimate(&"GET /v1/users");
    println!("Estimated signed count for GET /v1/users: {users_estimate}");

    Ok(())
}
