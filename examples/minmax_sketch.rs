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

use std::cmp::Reverse;

use sketches::SketchError;
use sketches::minmax_sketch::MinMaxSketch;

const SEED: u64 = 0x3C6E_F372_FE94_F82B;

// MinMax compares values only through Ord. Derived ordering follows declaration
// order, so collisions move estimates toward NearZero in this example.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord)]
enum MagnitudeBucket {
    #[default]
    NearZero,
    Medium,
    FarFromZero,
}

fn exact_single_key() -> Result<(), SketchError> {
    // With one key there is no collision noise, regardless of width or depth.
    let mut sketch = MinMaxSketch::<MagnitudeBucket>::new(8, 1, SEED)?;
    sketch.insert_u64(0, MagnitudeBucket::FarFromZero);
    assert_eq!(sketch.estimate_u64(0), Some(MagnitudeBucket::FarFromZero));
    println!("1. one key: exact FarFromZero estimate");
    Ok(())
}

fn forced_collision_uses_value_order() -> Result<(), SketchError> {
    // Width one has a single cell per row, so every key collides. Derived Ord
    // gives NearZero < Medium < FarFromZero, and insertion retains the minimum.
    let mut sketch = MinMaxSketch::<MagnitudeBucket>::new(1, 1, SEED)?;
    sketch.insert_u64(0, MagnitudeBucket::FarFromZero);
    sketch.insert_u64(1, MagnitudeBucket::Medium);

    assert_eq!(sketch.estimate_u64(0), Some(MagnitudeBucket::Medium));
    println!("2. forced collision: FarFromZero lowered to Medium");
    Ok(())
}

fn wider_rows_reduce_collisions() -> Result<(), SketchError> {
    // The same two keys collide when width=1. With this seed they select
    // different cells when width=16, so the original value remains exact.
    let mut narrow = MinMaxSketch::<MagnitudeBucket>::new(1, 1, SEED)?;
    let mut wide = MinMaxSketch::<MagnitudeBucket>::new(16, 1, SEED)?;
    for sketch in [&mut narrow, &mut wide] {
        sketch.insert_u64(0, MagnitudeBucket::FarFromZero);
        sketch.insert_u64(1, MagnitudeBucket::Medium);
    }

    assert_eq!(narrow.estimate_u64(0), Some(MagnitudeBucket::Medium));
    assert_eq!(wide.estimate_u64(0), Some(MagnitudeBucket::FarFromZero));
    println!("3. width 1 -> Medium; width 16 -> exact FarFromZero");
    Ok(())
}

fn more_depth_recovers_from_a_partial_collision() -> Result<(), SketchError> {
    // For width=4 and this seed, keys 0 and 9 collide in the first row but not
    // in the later rows. At depth=1 the only candidate is lowered to Medium.
    // At depth=3 at least one clean row retains FarFromZero, and query's maximum
    // over [Medium, FarFromZero, FarFromZero] recovers the original value.
    let mut shallow = MinMaxSketch::<MagnitudeBucket>::new(4, 1, SEED)?;
    let mut deep = MinMaxSketch::<MagnitudeBucket>::new(4, 3, SEED)?;
    for sketch in [&mut shallow, &mut deep] {
        sketch.insert_u64(0, MagnitudeBucket::FarFromZero);
        sketch.insert_u64(9, MagnitudeBucket::Medium);
    }

    assert_eq!(shallow.estimate_u64(0), Some(MagnitudeBucket::Medium));
    assert_eq!(deep.estimate_u64(0), Some(MagnitudeBucket::FarFromZero));
    println!("4. depth 1 -> Medium; depth 3 -> exact FarFromZero");
    Ok(())
}

fn order_is_defined_by_the_value_type() -> Result<(), SketchError> {
    // Reverse changes which value Ord considers smaller. This is useful when
    // the conservative direction of an encoded domain runs toward larger raw
    // codes rather than smaller ones.
    let mut sketch = MinMaxSketch::<Reverse<MagnitudeBucket>>::new(1, 1, SEED)?;
    sketch.insert_u64(0, Reverse(MagnitudeBucket::NearZero));
    sketch.insert_u64(1, Reverse(MagnitudeBucket::FarFromZero));
    assert_eq!(
        sketch.estimate_u64(0),
        Some(Reverse(MagnitudeBucket::FarFromZero))
    );
    println!("5. Reverse<...>: FarFromZero is the retained minimum");
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    exact_single_key()?;
    forced_collision_uses_value_order()?;
    wider_rows_reduce_collisions()?;
    more_depth_recovers_from_a_partial_collision()?;
    order_is_defined_by_the_value_type()?;
    Ok(())
}
