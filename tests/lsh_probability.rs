//! Independent reference and structural checks for the public LSH model APIs.
//!
//! No private state is manufactured. Configurations reserve only empty band
//! tables; represented signature widths do not allocate actual signatures.
//! References come from the original real formulas at 900/1200 decimal digits,
//! while sweeps exercise ordinary rounding, quantization, and monotonicity.

use sketches::SketchError;
use sketches::minhash_lsh_index::MinHashLshIndex;

/// Returns exactly 2^-exponent through all normal and subnormal binary64 bins.
/// Constructing bits avoids an unrelated reciprocal-power overflow in a test
/// oracle and preserves the minimum subnormal without decimal conversion.
fn negative_power_of_two(exponent: u32) -> f64 {
    assert!(exponent <= 1074);
    if exponent <= 1022 {
        f64::from_bits(u64::from(1023 - exponent) << 52)
    } else {
        f64::from_bits(1_u64 << (1074 - exponent))
    }
}

/// Produces an ordered set spanning every binary exponent plus ordinary-grid
/// and deterministic mantissa samples. The seed is fixture-owned and is used
/// for coverage, not to assert a statistical or cryptographic guarantee.
fn ordered_inputs() -> Vec<f64> {
    let mut inputs = vec![0.0, 1.0];
    for exponent in 1..=1074 {
        let bits = negative_power_of_two(exponent).to_bits();
        inputs.extend([bits - 1, bits, bits + 1].map(f64::from_bits));
    }
    inputs.extend((0..=1024).map(|i| f64::from(i) / 1024.0));
    let mut word = 0xA076_1D64_78BD_642F_u64;
    for _ in 0..4096 {
        word ^= word << 13;
        word ^= word >> 7;
        word ^= word << 17;
        inputs.push(f64::from_bits(word % 1.0_f64.to_bits()));
    }
    inputs.sort_unstable_by(f64::total_cmp);
    inputs.dedup();
    inputs
}

/// Checks one helper on adjacent representable inputs around a transition.
/// Plateaus are valid quantization; downward steps and nonfinite outputs are
/// rejected. Bounds are clipped before constructing values outside [0,1].
fn check_neighbors(index: &MinHashLshIndex<u64>, center: f64, inverse: bool) -> usize {
    let first = center.to_bits().saturating_sub(128);
    let last = center.to_bits().saturating_add(128).min(1.0_f64.to_bits());
    let mut previous = 0.0;
    for bits in first..=last {
        let input = f64::from_bits(bits);
        let actual = if inverse {
            index.similarity_for_candidate_probability(input).unwrap()
        } else {
            index.candidate_probability(input).unwrap()
        };
        assert!(actual.is_finite() && (0.0..=1.0).contains(&actual));
        assert!(
            actual >= previous,
            "bands={} rows={} inverse={inverse} input_bits={bits:016x} previous={previous:e} actual={actual:e}",
            index.bands(),
            index.rows_per_band()
        );
        previous = actual;
    }
    (last - first + 1) as usize
}

#[test]
fn public_helpers_match_independent_high_precision_references() {
    let mut count = 0;
    let mut max_forward_ulps = 0;
    let mut max_inverse_relative_error = 0.0_f64;
    for line in include_str!("data/lsh_probability_reference.csv").lines() {
        if line.starts_with('#') {
            continue;
        }
        let fields: Vec<_> = line.split(',').collect();
        assert_eq!(fields.len(), 5);
        let bands: usize = fields[0].parse().unwrap();
        let rows: usize = fields[1].parse().unwrap();
        let input = f64::from_bits(u64::from_str_radix(fields[3], 16).unwrap());
        let expected_bits = u64::from_str_radix(fields[4], 16).unwrap();
        let expected = f64::from_bits(expected_bits);
        let index = MinHashLshIndex::<u64>::new(bands * rows, bands).unwrap();
        let inverse = match fields[2] {
            "inverse" => true,
            "forward" => false,
            other => panic!("unknown reference direction {other}"),
        };
        let actual = if inverse {
            index.similarity_for_candidate_probability(input).unwrap()
        } else {
            index.candidate_probability(input).unwrap()
        };
        assert!(
            actual.is_finite() && (0.0..=1.0).contains(&actual),
            "{line}"
        );
        let ulps = actual.to_bits().abs_diff(expected_bits);
        if expected < f64::MIN_POSITIVE {
            // Final subnormal rounding has little relative precision. Permit
            // two final ULPs, including adjacent rounding-midpoint cases;
            // explicit unit regressions pin the original six-ULP example.
            assert!(ulps <= 2, "{line} actual={actual:e} ulps={ulps}");
        } else if inverse {
            // Logarithm error is amplified by exp in proportion to the output
            // exponent. This scale-sensitive budget stays strict near one and
            // rejects a zero at any normal reference magnitude. It is a test
            // budget, not a correctly-rounded transcendental guarantee.
            let relative_error = ((actual - expected) / expected).abs();
            let budget = (2.0 * f64::EPSILON * (1.0 + expected.ln().abs())).max(8.0 * f64::EPSILON);
            assert!(
                relative_error <= budget,
                "{line} actual={actual:e} relative_error={relative_error:e} budget={budget:e}"
            );
            max_inverse_relative_error = max_inverse_relative_error.max(relative_error);
        } else {
            assert!(ulps <= 8, "{line} actual={actual:e} ulps={ulps}");
            max_forward_ulps = max_forward_ulps.max(ulps);
        }
        count += 1;
    }
    assert!(count >= 1000, "reference vectors unexpectedly omitted");
    println!(
        "{count} 900/1200-digit references passed; normal forward max ULP error={max_forward_ulps}; normal inverse max relative error={max_inverse_relative_error:e}"
    );
}

#[test]
fn every_binary_exponent_and_sampled_mantissa_preserves_range_and_monotonicity() {
    let inputs = ordered_inputs();
    let mut count = 0;
    for bands in [1, 2, 3, 4, 7, 8, 16, 31, 32, 63] {
        for rows in [1, 2, 3, 4, 5, 7, 8, 16, 32, 64, 127, 128, 1024, 2048] {
            let index = MinHashLshIndex::<u64>::new(bands * rows, bands).unwrap();
            let mut previous_forward = 0.0;
            let mut previous_inverse = 0.0;
            for &input in &inputs {
                let forward = index.candidate_probability(input).unwrap();
                let inverse = index.similarity_for_candidate_probability(input).unwrap();
                assert!(forward.is_finite() && (0.0..=1.0).contains(&forward));
                assert!(inverse.is_finite() && (0.0..=1.0).contains(&inverse));
                assert!(
                    forward >= previous_forward,
                    "forward bands={bands} rows={rows} input={input:e}"
                );
                assert!(
                    inverse >= previous_inverse,
                    "inverse bands={bands} rows={rows} input={input:e}"
                );
                if input > 0.0 && rows > 1 {
                    // For these legal dimensions, even the smallest input's
                    // root is normal. Zero would be premature intermediate loss.
                    assert!(inverse.is_normal());
                }
                previous_forward = forward;
                previous_inverse = inverse;
                count += 2;
            }
        }
    }
    println!("{count} public evaluations over every binary exponent and sampled mantissas passed");
}

#[test]
fn dense_configurations_preserve_adjacent_float_transition_ordering() {
    let mut count = 0;
    for bands in 2..=64 {
        for rows in 2..=128 {
            let index = MinHashLshIndex::<u64>::new(bands * rows, bands).unwrap();
            let forward_normal = f64::MIN_POSITIVE.powf(1.0 / rows as f64);
            let inverse_normal = bands as f64 * f64::MIN_POSITIVE;
            let forward_zero =
                ((f64::from_bits(1).ln() - (bands as f64).ln() - std::f64::consts::LN_2)
                    / rows as f64)
                    .exp();
            count += check_neighbors(&index, forward_normal, false);
            count += check_neighbors(&index, forward_zero, false);
            count += check_neighbors(&index, inverse_normal, true);
            if bands <= 32 {
                // The ordinary-range t=1 neighborhood also exercises p close
                // to one as bands grows, where an opposing-log correction
                // would otherwise lose precision.
                let inverse_correction = (-(bands as f64)).exp_m1().abs();
                count += check_neighbors(&index, inverse_correction, true);
            }
        }
    }
    println!("{count} adjacent-float evaluations across 8,001 configurations passed");
}

#[test]
fn first_4096_positive_subnormal_probabilities_survive_amplifying_roots() {
    let mut count = 0;
    for bands in [1, 2, 3, 7, 32, 64] {
        for rows in [2, 3, 4, 7, 16, 64, 128, 1024] {
            let index = MinHashLshIndex::<u64>::new(bands * rows, bands).unwrap();
            let mut previous = 0.0;
            for bits in 1..=4096 {
                let actual = index
                    .similarity_for_candidate_probability(f64::from_bits(bits))
                    .unwrap();
                assert!(actual.is_normal() && actual <= 1.0);
                assert!(actual >= previous);
                previous = actual;
                count += 1;
            }
        }
    }
    println!("{count} positive-subnormal inverse evaluations passed");
}

#[test]
fn increasing_bands_and_rows_moves_the_model_in_the_expected_direction() {
    let inputs = [
        f64::from_bits(1),
        f64::MIN_POSITIVE,
        1e-300,
        1e-100,
        1e-12,
        0.001,
        0.1,
        0.5,
        0.9,
        f64::from_bits(1.0_f64.to_bits() - 1),
    ];
    for rows in [1, 2, 3, 4, 7, 16, 64, 128] {
        for &input in &inputs {
            let mut previous_forward = 0.0;
            let mut previous_inverse = 1.0;
            for bands in 1..=64 {
                let index = MinHashLshIndex::<u64>::new(bands * rows, bands).unwrap();
                let forward = index.candidate_probability(input).unwrap();
                let inverse = index.similarity_for_candidate_probability(input).unwrap();
                assert!(forward >= previous_forward);
                assert!(
                    inverse <= previous_inverse,
                    "bands={bands} rows={rows} input={input:e}"
                );
                previous_forward = forward;
                previous_inverse = inverse;
            }
        }
    }
    for bands in [1, 2, 3, 7, 32, 64] {
        for &input in &inputs {
            let mut previous_forward = 1.0;
            let mut previous_inverse = 0.0;
            for rows in 1..=128 {
                let index = MinHashLshIndex::<u64>::new(bands * rows, bands).unwrap();
                let forward = index.candidate_probability(input).unwrap();
                let inverse = index.similarity_for_candidate_probability(input).unwrap();
                assert!(forward <= previous_forward);
                assert!(inverse >= previous_inverse);
                previous_forward = forward;
                previous_inverse = inverse;
            }
        }
    }
}

#[test]
fn quantized_roundtrips_cover_tiny_and_ordinary_probabilities() {
    for bands in [1, 2, 3, 7, 32, 64] {
        for rows in [1, 2, 3, 4, 7, 16, 64, 128] {
            let index = MinHashLshIndex::<u64>::new(bands * rows, bands).unwrap();
            for probability in ordered_inputs() {
                let similarity = index
                    .similarity_for_candidate_probability(probability)
                    .unwrap();
                let recovered = index.candidate_probability(similarity).unwrap();
                if probability < f64::MIN_POSITIVE {
                    // A single-row inverse's final quantization is amplified
                    // by b. With a root, relative arithmetic error can span
                    // multiple subnormal units near the normal boundary, so
                    // add its scale-sensitive budget to final quantization.
                    let budget = if rows == 1 {
                        bands as u64
                    } else if probability == 0.0 {
                        0
                    } else {
                        let relative_budget =
                            8.0 * f64::EPSILON * (rows as f64 + probability.ln().abs() + 1.0);
                        (relative_budget * probability.to_bits() as f64).ceil() as u64 + 2
                    };
                    assert!(
                        recovered.to_bits().abs_diff(probability.to_bits()) <= budget,
                        "bands={bands} rows={rows} p={probability:e} recovered={recovered:e} ulps={} budget={budget}",
                        recovered.to_bits().abs_diff(probability.to_bits())
                    );
                } else {
                    let relative_error = ((recovered - probability) / probability).abs();
                    // Account for final similarity quantization amplified by r
                    // and for logarithmic tail arithmetic. Neither a global
                    // absolute tolerance nor exact roundtrip equality is valid.
                    let budget = 8.0 * f64::EPSILON * (rows as f64 + probability.ln().abs() + 1.0);
                    assert!(
                        relative_error <= budget,
                        "bands={bands} rows={rows} p={probability:e} recovered={recovered:e} error={relative_error:e} budget={budget:e}"
                    );
                }
            }
        }
    }
}

#[test]
fn endpoint_neighbors_and_invalid_inputs_keep_the_public_contract() {
    for (bands, rows) in [(1, 1), (1, 4), (2, 1), (32, 4)] {
        let index = MinHashLshIndex::<u64>::new(bands * rows, bands).unwrap();
        for endpoint in [-0.0, 0.0, 1.0] {
            assert_eq!(index.candidate_probability(endpoint).unwrap(), endpoint);
            assert_eq!(
                index
                    .similarity_for_candidate_probability(endpoint)
                    .unwrap(),
                endpoint
            );
        }
        for invalid in [
            -f64::from_bits(1),
            -1.0,
            f64::NEG_INFINITY,
            f64::from_bits(1.0_f64.to_bits() + 1),
            f64::INFINITY,
            f64::NAN,
        ] {
            assert!(matches!(
                index.candidate_probability(invalid),
                Err(SketchError::InvalidParameter(_))
            ));
            assert!(matches!(
                index.similarity_for_candidate_probability(invalid),
                Err(SketchError::InvalidParameter(_))
            ));
        }
        let next_to_one = f64::from_bits(1.0_f64.to_bits() - 1);
        assert!(index.candidate_probability(next_to_one).unwrap() <= 1.0);
        assert!(
            index
                .similarity_for_candidate_probability(next_to_one)
                .unwrap()
                <= 1.0
        );
    }
}

#[test]
fn large_valid_widths_keep_model_outputs_finite_and_ordered() {
    // These constructors allocate only O(bands) empty index configuration,
    // never the represented signature or a corpus of items.
    let mut rows_to_check = vec![1_usize << (usize::BITS / 2)];
    if usize::BITS == 64 {
        rows_to_check.extend([
            usize::try_from(1_u64 << 40).unwrap(),
            usize::try_from((1_u64 << 53) + 1).unwrap(),
        ]);
    }
    let inputs = ordered_inputs();
    for bands in [1, 2, 7, 32] {
        for &rows in &rows_to_check {
            let index = MinHashLshIndex::<u64>::new(bands * rows, bands).unwrap();
            let mut previous_forward = 0.0;
            let mut previous_inverse = 0.0;
            for &input in &inputs {
                let forward = index.candidate_probability(input).unwrap();
                let inverse = index.similarity_for_candidate_probability(input).unwrap();
                assert!(forward.is_finite() && (0.0..=1.0).contains(&forward));
                assert!(inverse.is_finite() && (0.0..=1.0).contains(&inverse));
                assert!(forward >= previous_forward);
                assert!(inverse >= previous_inverse);
                previous_forward = forward;
                previous_inverse = inverse;
            }
        }
    }
}
