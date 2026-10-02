//! Exercises recoverable allocation failures through public sketch APIs.
//!
//! The failure budget belongs to the current test thread. Allocator callbacks
//! cannot borrow a caller-owned counter, so a thread-local Cell provides that
//! narrowly scoped callback state without affecting concurrently running tests.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::ptr;

use sketches::SketchError;
use sketches::bloom_filter::BloomFilter;
use sketches::cuckoo_filter::CuckooFilter;
use sketches::reservoir_sampling::ReservoirSampling;
use sketches::space_saving::SpaceSaving;

thread_local! {
    /// Remaining successful allocation requests; None delegates normally.
    static ALLOCATION_BUDGET: Cell<Option<usize>> = const { Cell::new(None) };
}

/// Delegates memory ownership to System while allowing a scoped null result.
struct FailingAllocator;

impl FailingAllocator {
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
        if Self::should_fail() {
            ptr::null_mut()
        } else {
            // SAFETY: The caller supplies a valid allocation layout.
            unsafe { System.alloc(layout) }
        }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        if Self::should_fail() {
            ptr::null_mut()
        } else {
            // SAFETY: The caller supplies a valid allocation layout.
            unsafe { System.alloc_zeroed(layout) }
        }
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
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
    for (capacity, left_len, right_len) in [(1, 0, 0), (1, 1, 1), (3, 1, 2), (3, 3, 3), (8, 8, 8)] {
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
