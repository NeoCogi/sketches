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
use sketches::cuckoo_filter::CuckooFilter;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a cuckoo filter configured for about 10k elements.
    let mut filter = CuckooFilter::new(10_000, 0.01)?;

    // Insert a few keys.
    for user in ["alice", "bob", "charlie"] {
        let inserted = filter.insert(&user);
        println!("insert {user:>7}: {inserted}");
    }

    println!("contains alice: {}", filter.contains(&"alice"));
    println!("contains david: {}", filter.contains(&"david"));

    // Cuckoo filters support deletion.
    println!("delete bob: {}", filter.delete(&"bob"));
    println!("contains bob after delete: {}", filter.contains(&"bob"));

    Ok(())
}
