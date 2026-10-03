//! Exercises allocation failures and workspace bounds through public sketch APIs.
//!
//! The failure budget belongs to the current test thread. Allocator callbacks
//! cannot borrow a caller-owned counter, so a thread-local Cell provides that
//! narrowly scoped callback state without affecting concurrently running tests.
//! Allocation-request accounting uses the same callback boundary; it records
//! requested bytes, not allocator overhead, resident memory or peak live bytes.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::ptr;

use sketches::SketchError;
use sketches::bloom_filter::BloomFilter;
use sketches::cuckoo_filter::CuckooFilter;
use sketches::hyperloglog::HyperLogLog;
use sketches::minmax_sketch::MinMaxSketch;
use sketches::reservoir_sampling::ReservoirSampling;
use sketches::rv_coefficient::RvCoefficient;
use sketches::space_saving::SpaceSaving;
use sketches::ultraloglog::UltraLogLog;

thread_local! {
    /// Remaining successful allocation requests; None delegates normally.
    static ALLOCATION_BUDGET: Cell<Option<usize>> = const { Cell::new(None) };
    /// Optional accounting restricted to the current operation and test thread.
    static REQUEST_ACCOUNTING: Cell<Option<AllocationRequests>> = const { Cell::new(None) };
    /// Value callbacks are thread-local so concurrent tests cannot mix counts.
    static VALUE_INITIALIZATION: Cell<InitializationCalls> = const {
        Cell::new(InitializationCalls { defaults: 0, clones: 0 })
    };
}

/// Observable work performed by MinMax's generic value initialization.
#[derive(Clone, Copy, Default, Debug, PartialEq, Eq)]
struct InitializationCalls {
    /// Calls to the value type's Default implementation.
    defaults: usize,
    /// Calls to Clone when filling additional value cells.
    clones: usize,
}

/// Lawful value with either a zero-sized or allocated layout, instrumented only
/// through test-thread counters. Its ordering and copied value are unchanged.
#[derive(Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct CountedValue<T: Copy>(T);

impl<T: Copy + Default> Default for CountedValue<T> {
    fn default() -> Self {
        VALUE_INITIALIZATION.with(|counter| {
            let mut calls = counter.get();
            calls.defaults += 1;
            counter.set(calls);
        });
        Self(T::default())
    }
}

// The returned value is exactly *self as Copy requires. This test must observe
// the otherwise redundant Clone calls made by Vec initialization.
#[expect(
    clippy::non_canonical_clone_impl,
    reason = "counts value-fill callbacks"
)]
impl<T: Copy> Clone for CountedValue<T> {
    fn clone(&self) -> Self {
        VALUE_INITIALIZATION.with(|counter| {
            let mut calls = counter.get();
            calls.clones += 1;
            counter.set(calls);
        });
        *self
    }
}

/// Layout sizes requested through the allocator, including realloc destinations.
#[derive(Clone, Copy, Default, Debug)]
struct AllocationRequests {
    /// Number of allocation requests; deallocations do not contribute.
    requests: usize,
    /// Sum of requested layout sizes; this is not a peak-memory measurement.
    bytes: usize,
}

/// Delegates memory ownership to System while allowing a scoped null result.
struct FailingAllocator;

impl FailingAllocator {
    /// Records a request without allocating, borrowing or changing ownership.
    fn record_request(bytes: usize) {
        let _ = REQUEST_ACCOUNTING.try_with(|accounting| {
            if let Some(mut requests) = accounting.get() {
                requests.requests = requests.requests.saturating_add(1);
                requests.bytes = requests.bytes.saturating_add(bytes);
                accounting.set(Some(requests));
            }
        });
    }

    /// Consumes one request on the current thread without allocating itself.
    fn should_fail() -> bool {
        ALLOCATION_BUDGET
            .try_with(|budget| match budget.get() {
                None => false,
                Some(0) => true,
                Some(remaining) => {
                    budget.set(Some(remaining - 1));
                    false
                }
            })
            .unwrap_or(false)
    }
}

// SAFETY: Successful requests and all deallocations preserve System's layouts
// and pointer ownership. Failure returns null, as required by GlobalAlloc.
unsafe impl GlobalAlloc for FailingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        Self::record_request(layout.size());
        if Self::should_fail() {
            ptr::null_mut()
        } else {
            // SAFETY: The caller supplies a valid allocation layout.
            unsafe { System.alloc(layout) }
        }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        Self::record_request(layout.size());
        if Self::should_fail() {
            ptr::null_mut()
        } else {
            // SAFETY: The caller supplies a valid allocation layout.
            unsafe { System.alloc_zeroed(layout) }
        }
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        Self::record_request(new_size);
        if Self::should_fail() {
            ptr::null_mut()
        } else {
            // SAFETY: The pointer, original layout and new size are forwarded
            // unchanged under the caller's realloc preconditions.
            unsafe { System.realloc(pointer, layout, new_size) }
        }
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: Every successful allocation originates from System, and
        // the caller supplies that allocation's pointer and original layout.
        unsafe { System.dealloc(pointer, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: FailingAllocator = FailingAllocator;

/// Restores normal allocation before assertions or panic formatting run.
struct FailureScope;

impl FailureScope {
    /// Allows the given number of requests before refusing further requests.
    fn after(successful_requests: usize) -> Self {
        ALLOCATION_BUDGET.with(|budget| {
            assert!(budget.get().is_none(), "failure scopes cannot nest");
            budget.set(Some(successful_requests));
        });
        Self
    }
}

impl Drop for FailureScope {
    fn drop(&mut self) {
        ALLOCATION_BUDGET.with(|budget| budget.set(None));
    }
}

/// Ends operation-local accounting before assertion formatting can allocate.
struct RequestScope;

impl RequestScope {
    /// Begins a non-nested accounting scope without modifying failure policy.
    fn start() -> Self {
        REQUEST_ACCOUNTING.with(|accounting| {
            assert!(accounting.get().is_none(), "request scopes cannot nest");
            accounting.set(Some(AllocationRequests::default()));
        });
        Self
    }

    /// Takes the measurements; Drop restores normal delegation on every path.
    fn finish(self) -> AllocationRequests {
        REQUEST_ACCOUNTING.with(|accounting| accounting.get().unwrap())
    }
}

impl Drop for RequestScope {
    fn drop(&mut self) {
        REQUEST_ACCOUNTING.with(|accounting| accounting.set(None));
    }
}

/// Measures only the query, ending accounting before result checks can allocate.
fn without_allocation<T>(query: impl FnOnce() -> T) -> T {
    let scope = RequestScope::start();
    let result = query();
    let requests = scope.finish();
    assert_eq!(requests.requests, 0, "scalar query allocated");
    assert_eq!(requests.bytes, 0);
    result
}

#[test]
fn hll_scalar_set_queries_need_no_heap_workspace() {
    for precision in [4, 10, 18] {
        let mut left = HyperLogLog::new(precision).unwrap();
        let mut right = HyperLogLog::new(precision).unwrap();
        for value in 0..1_000_u64 {
            left.add(&value);
            right.add(&(value + 500));
        }
        for (a, b) in [(&left, &right), (&right, &left), (&left, &left)] {
            assert!(
                without_allocation(|| a.union_estimate(b))
                    .unwrap()
                    .is_finite()
            );
            assert!(without_allocation(|| a.intersection_estimate(b)).is_ok());
            assert!(without_allocation(|| a.jaccard_index(b)).is_ok());
        }
        let incompatible = HyperLogLog::new(if precision == 4 { 5 } else { 4 }).unwrap();
        assert!(without_allocation(|| left.union_estimate(&incompatible)).is_err());
        assert!(without_allocation(|| left.intersection_estimate(&incompatible)).is_err());
        assert!(without_allocation(|| left.jaccard_index(&incompatible)).is_err());
    }
}

#[test]
fn ull_scalar_set_queries_need_no_heap_workspace() {
    for (left_precision, right_precision) in [(3, 3), (10, 10), (16, 18)] {
        let mut left = UltraLogLog::new(left_precision).unwrap();
        let mut right = UltraLogLog::new(right_precision).unwrap();
        for value in 0..1_000_u64 {
            left.add(&value);
            right.add(&(value + 500));
        }
        for (a, b) in [(&left, &right), (&right, &left), (&left, &left)] {
            assert!(without_allocation(|| a.union_estimate(b)).is_finite());
            assert!(without_allocation(|| a.intersection_estimate(b)).is_ok());
            assert!(without_allocation(|| a.jaccard_index(b)).is_ok());
        }
    }
    let saturated = UltraLogLog::from_state(vec![255; 8]).unwrap();
    let empty = UltraLogLog::new(4).unwrap();
    for (a, b) in [(&saturated, &empty), (&empty, &saturated)] {
        assert!(without_allocation(|| a.union_estimate(b)).is_infinite());
        assert_eq!(
            without_allocation(|| a.intersection_estimate(b)),
            Err(SketchError::EstimateUnavailable)
        );
        assert_eq!(
            without_allocation(|| a.jaccard_index(b)),
            Err(SketchError::EstimateUnavailable)
        );
    }

    // APIs returning register owners still allocate independent storage.
    let scope = RequestScope::start();
    let merged = saturated.merged(&empty);
    let downsized = empty.downsize(3).unwrap();
    let requests = scope.finish();
    assert!(requests.requests >= 2);
    assert_eq!(merged.precision(), 3);
    assert_eq!(downsized.precision(), 3);
}

#[test]
fn filter_and_reservoir_constructors_report_failed_reservations() {
    let constructors: [fn() -> Result<(), SketchError>; 5] = [
        || BloomFilter::new(100, 0.01).map(|_| ()),
        || BloomFilter::with_size(65, 2).map(|_| ()),
        || CuckooFilter::new(100, 0.01).map(|_| ()),
        || CuckooFilter::with_parameters(8, 6, 1).map(|_| ()),
        || ReservoirSampling::<u64>::new(8, 7).map(|_| ()),
    ];
    for construct in constructors {
        let result = {
            let _scope = FailureScope::after(0);
            construct()
        };
        assert!(matches!(result, Err(SketchError::InvalidParameter(_))));
        construct().unwrap();
    }
}

#[test]
fn zero_sized_reservoir_needs_no_reservation_allocation() {
    let result = {
        let _scope = FailureScope::after(0);
        ReservoirSampling::<()>::new(usize::MAX, 7)
    };
    let mut reservoir = result.unwrap();
    reservoir.extend([(); 4]);
    assert_eq!(reservoir.capacity(), usize::MAX);
    assert_eq!(reservoir.len(), 4);
}

/// Walks every MinMax reservation boundary for an actual generic layout.
/// Failure policies end before assertions, successful reuse and value updates.
fn check_minmax_initialization_order<T: Copy + Default + Ord + std::fmt::Debug>() {
    let errors = if size_of::<T>() == 0 {
        &[
            "occupancy table is too large to allocate",
            "depth is too large to allocate",
        ][..]
    } else {
        &[
            "value table is too large to allocate",
            "occupancy table is too large to allocate",
            "depth is too large to allocate",
        ][..]
    };
    let expected_requests = errors.len();
    // Include both sides of occupancy-word boundaries and several row counts.
    for width in [1, 63, 64, 65, 257] {
        for depth in [1, 2, 3, 5] {
            for (allowed, error) in errors
                .iter()
                .copied()
                .map(Some)
                .chain(std::iter::once(None))
                .enumerate()
            {
                VALUE_INITIALIZATION.with(|counter| counter.set(InitializationCalls::default()));
                let accounting = RequestScope::start();
                let result = {
                    let _failure = FailureScope::after(allowed);
                    MinMaxSketch::<CountedValue<T>>::new(width, depth, 7)
                };
                let requests = accounting.finish();
                let calls = VALUE_INITIALIZATION.with(Cell::get);
                if let Some(error) = error {
                    assert_eq!(result.unwrap_err(), SketchError::InvalidParameter(error));
                    assert_eq!(requests.requests, allowed + 1);
                    assert_eq!(
                        calls,
                        InitializationCalls::default(),
                        "width={width} depth={depth} allowed={allowed}"
                    );
                } else {
                    let sketch = result.unwrap();
                    assert_eq!(requests.requests, expected_requests);
                    assert_eq!(
                        calls,
                        InitializationCalls {
                            defaults: 1,
                            clones: width * depth - 1
                        }
                    );
                    assert_eq!(
                        (sketch.width(), sketch.depth(), sketch.seed()),
                        (width, depth, 7)
                    );
                    assert!(sketch.is_empty());
                }
                // Failed and successful attempts leave allocation delegation
                // usable. The resulting owner supports insert, merge and clear.
                let mut receiver = MinMaxSketch::<CountedValue<T>>::new(width, depth, 7).unwrap();
                let mut donor = MinMaxSketch::<CountedValue<T>>::new(width, depth, 7).unwrap();
                let value = CountedValue(T::default());
                donor.insert_u64(42, value);
                receiver.merge(&donor).unwrap();
                assert_eq!(receiver.estimate_u64(42), Some(value));
                assert_eq!(donor.estimate_u64(42), Some(value));
                assert_eq!(receiver.occupied_cells(), depth);
                receiver.clear();
                assert!(receiver.is_empty());
                assert_eq!(receiver.estimate_u64(42), None);
                receiver.insert_u64(43, value);
                assert_eq!(receiver.estimate_u64(43), Some(value));
            }
        }
    }
}

#[test]
fn minmax_reserves_all_capacities_before_initializing_allocated_values() {
    check_minmax_initialization_order::<u64>();
}

#[test]
fn minmax_reserves_all_capacities_before_initializing_zero_sized_values() {
    check_minmax_initialization_order::<()>();
}

#[test]
fn minmax_invalid_shapes_return_before_reservation_or_value_initialization() {
    for (width, depth) in [(0, 1), (1, 0), (usize::MAX, 2), (usize::MAX, 1)] {
        VALUE_INITIALIZATION.with(|counter| counter.set(InitializationCalls::default()));
        let accounting = RequestScope::start();
        let result = {
            let _failure = FailureScope::after(0);
            MinMaxSketch::<CountedValue<u64>>::new(width, depth, 7)
        };
        let requests = accounting.finish();
        assert!(matches!(result, Err(SketchError::InvalidParameter(_))));
        assert_eq!(requests.requests, 0);
        assert_eq!(
            VALUE_INITIALIZATION.with(Cell::get),
            InitializationCalls::default()
        );
    }
}

#[test]
fn minmax_maximum_zero_sized_table_fails_before_initialization() {
    // This actual ZST layout needs no value allocation. Deny the occupancy
    // request through valid allocator storage, without permitting a huge fill.
    VALUE_INITIALIZATION.with(|counter| counter.set(InitializationCalls::default()));
    let accounting = RequestScope::start();
    let result = {
        let _failure = FailureScope::after(0);
        MinMaxSketch::<CountedValue<()>>::new(usize::MAX, 1, 7)
    };
    let requests = accounting.finish();
    assert_eq!(
        result.unwrap_err(),
        SketchError::InvalidParameter("occupancy table is too large to allocate")
    );
    assert_eq!(requests.requests, 1);
    assert_eq!(
        VALUE_INITIALIZATION.with(Cell::get),
        InitializationCalls::default()
    );
}

/// Owned public state used to compare a summary before and after an operation.
#[derive(Debug, PartialEq, Eq)]
struct SummarySnapshot {
    /// Configured maximum number of retained counters.
    capacity: usize,
    /// Saturating observation count, independent of retained counter sums.
    total_count: u64,
    /// Item, estimate and error triples sorted independently of bucket ties.
    entries: Vec<(u64, u64, u64)>,
}

/// Captures the observable count/error state independently of bucket tie order.
fn summary_snapshot(summary: &SpaceSaving<u64>) -> SummarySnapshot {
    let mut entries = summary.top_k(summary.capacity());
    entries.sort_unstable();
    SummarySnapshot {
        capacity: summary.capacity(),
        total_count: summary.total_count(),
        entries,
    }
}

#[test]
fn space_saving_constructor_reports_each_failed_initial_reservation() {
    let mut failures = 0;
    let mut succeeded = false;
    for allowed_requests in 0..16 {
        let result = {
            let _scope = FailureScope::after(allowed_requests);
            SpaceSaving::<u64>::new(4)
        };
        match result {
            Err(SketchError::InvalidParameter(_)) => failures += 1,
            Err(error) => panic!("unexpected constructor error: {error}"),
            Ok(mut summary) => {
                summary.insert(7);
                assert_eq!(summary.estimate(&7), Some(1));
                succeeded = true;
                break;
            }
        }
    }
    assert!(succeeded);
    assert!(
        failures >= 2,
        "lookup and counter reservations must be exercised"
    );
}

#[test]
fn space_saving_merge_reservation_failures_preserve_both_summaries_and_reuse() {
    for (capacity, left_len, right_len) in [
        (1, 0, 0),
        (1, 1, 1),
        (3, 1, 2),
        (3, 3, 3),
        (8, 8, 8),
        (64, 0, 0),
        (64, 1, 0),
        (64, 0, 1),
        (64, 1, 1),
        (64, 1, 2),
        (64, 63, 64),
        (64, 64, 64),
        (1024, 1, 1),
        (4096, 0, 0),
    ] {
        let mut original = SpaceSaving::new(capacity).unwrap();
        let mut donor = SpaceSaving::new(capacity).unwrap();
        for index in 0..left_len {
            for _ in 0..(index + 1) * 3 {
                original.insert(index as u64);
            }
        }
        for index in 0..right_len {
            for _ in 0..(index + 1) * 5 {
                donor.insert((index + left_len / 2) as u64);
            }
        }
        let donor_before = summary_snapshot(&donor);
        let mut expected = original.clone();
        expected.merge(&donor).unwrap();
        let expected_after = summary_snapshot(&expected);
        let mut failures = 0;
        let mut succeeded = false;

        // Walk the actual allocation sequence, refusing every request from
        // this point onward. Stop only when the complete merge can succeed.
        for allowed_requests in 0..32 {
            let mut receiver = original.clone();
            let before = summary_snapshot(&receiver);
            let internal_before = format!("{receiver:?}");
            let result = {
                let _scope = FailureScope::after(allowed_requests);
                receiver.merge(&donor)
            };
            assert_eq!(summary_snapshot(&donor), donor_before);
            match result {
                Err(SketchError::InvalidParameter(_)) => {
                    failures += 1;
                    assert_eq!(summary_snapshot(&receiver), before);
                    assert_eq!(format!("{receiver:?}"), internal_before);
                    // Successful reuse exercises the retained links as well
                    // as the immediate public snapshot after the failure.
                    receiver.merge(&donor).unwrap();
                    assert_eq!(summary_snapshot(&receiver), expected_after);
                }
                Err(error) => panic!("unexpected merge error: {error}"),
                Ok(()) => {
                    assert_eq!(summary_snapshot(&receiver), expected_after);
                    succeeded = true;
                    break;
                }
            }
        }
        assert!(succeeded);
        assert!(failures >= 2);
        eprintln!(
            "capacity={capacity} left={left_len} right={right_len}: {failures} reservation failures verified"
        );
    }
}

#[test]
fn space_saving_sparse_merge_requests_capacity_sized_replacement_storage() {
    for (left_len, right_len, overlap) in [
        (0, 0, false),
        (1, 0, false),
        (0, 1, false),
        (1, 1, true),
        (1, 1, false),
    ] {
        let mut previous_bytes = None;
        for capacity in [64, 256, 1024, 4096] {
            let mut receiver = SpaceSaving::<u64>::new(capacity).unwrap();
            let mut donor = SpaceSaving::<u64>::new(capacity).unwrap();
            for _ in 0..left_len * 3 {
                receiver.insert(7);
            }
            let donor_item = if overlap { 7 } else { 11 };
            for _ in 0..right_len * 5 {
                donor.insert(donor_item);
            }
            let before = summary_snapshot(&donor);
            let scope = RequestScope::start();
            let result = receiver.merge(&donor);
            let requests = scope.finish();
            result.unwrap();
            assert_eq!(summary_snapshot(&donor), before);
            assert_eq!(receiver.capacity(), capacity);
            assert_eq!(
                receiver.total_count(),
                (left_len * 3 + right_len * 5) as u64
            );
            let expected_items = left_len + right_len - usize::from(overlap);
            assert_eq!(receiver.tracked_items(), expected_items);
            if left_len > 0 {
                assert_eq!(
                    receiver.estimate_with_error(&7),
                    Some((3 + if overlap { 5 } else { 0 }, 0))
                );
            }
            if right_len > 0 {
                assert_eq!(
                    receiver.estimate_with_error(&donor_item),
                    Some((5 + if overlap { 3 } else { 0 }, 0))
                );
            }
            // No internal layout coefficients are pinned. The fixed occupancy
            // cases must request capacity-sized storage, including empty merge;
            // quadrupling C should scale the measured requests approximately
            // fourfold, with slack for table rounding and fixed bookkeeping.
            assert!(requests.requests >= 2);
            assert!(requests.bytes >= capacity * size_of::<u64>());
            if let Some(previous) = previous_bytes {
                assert!((3 * previous..=5 * previous).contains(&requests.bytes));
            }
            previous_bytes = Some(requests.bytes);
            eprintln!(
                "capacity={capacity} left={left_len} right={right_len} overlap={overlap}: {requests:?}"
            );
            // The rebuilt owner still supports growth up to its logical bound.
            for item in 100..100 + capacity as u64 {
                receiver.insert(item);
            }
            assert_eq!(receiver.tracked_items(), capacity);
            receiver.clear();
            receiver.insert(99);
            assert_eq!(receiver.estimate_with_error(&99), Some((1, 0)));
        }
    }
}

#[test]
fn space_saving_merge_validates_compatibility_before_reserving() {
    let mut receiver = SpaceSaving::new(3).unwrap();
    receiver.insert(7_u64);
    let donor = SpaceSaving::<u64>::new(4).unwrap();
    let before = summary_snapshot(&receiver);
    let result = {
        let _scope = FailureScope::after(0);
        receiver.merge(&donor)
    };
    assert!(matches!(result, Err(SketchError::IncompatibleSketches(_))));
    assert_eq!(summary_snapshot(&receiver), before);
    assert!(donor.is_empty());
}

#[test]
fn rv_coefficient_queries_need_no_heap_workspace() {
    for (p, q) in [(1, 1), (2, 2), (3, 2), (4, 4)] {
        let mut rv = RvCoefficient::new(p, q).unwrap();
        for step in 0..20 {
            let x: Vec<f64> = (0..p).map(|i| (step * p + i) as f64).collect();
            let y: Vec<f64> = (0..q).map(|i| ((step + 1) * q + i) as f64).collect();
            rv.add(&x, &y).unwrap();
        }
        assert!(without_allocation(|| rv.rv_coefficient()).is_some());
    }
}

#[test]
fn rv_coefficient_constructor_reports_failed_reservations() {
    for allowed in 0..5 {
        let result = {
            let _scope = FailureScope::after(allowed);
            RvCoefficient::new(4, 4)
        };
        assert!(matches!(result, Err(SketchError::InvalidParameter(_))));
    }
    assert!(RvCoefficient::new(4, 4).is_ok());
}
