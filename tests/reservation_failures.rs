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
