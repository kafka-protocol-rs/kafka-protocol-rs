//! Observe Rust allocation requests during decoding. Native codec allocations
//! are outside this test's scope; this is not a process-memory/RSS assertion.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

use bytes::{Bytes, BytesMut};
use kafka_protocol::compression::{self, BoundedDecompressor, Compressor};
use kafka_protocol::records::{DecodeBudget, DecodeLimitExceeded, DecodeLimits};

thread_local! {
    static LARGEST: Cell<Option<usize>> = const { Cell::new(None) };
}

struct ObservedAllocator;

fn observe(size: usize) {
    let _ = LARGEST.try_with(|largest| {
        if let Some(previous) = largest.get() {
            largest.set(Some(previous.max(size)));
        }
    });
}

// Forward every allocation unchanged to System; only record sizes on the test
// thread during an explicitly measured interval, without allocating in the hook.
unsafe impl GlobalAlloc for ObservedAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        observe(layout.size());
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        observe(layout.size());
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        observe(new_size);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: ObservedAllocator = ObservedAllocator;

fn check<C>()
where
    C: Compressor<BytesMut, BufMut = BytesMut> + BoundedDecompressor<Bytes, Buf = Bytes>,
{
    let mut input = BytesMut::new();
    C::compress(&mut input, |output| {
        output.resize(512 * 1024, b'a');
        Ok(())
    })
    .unwrap();
    let mut input = input.freeze();
    let mut budget = DecodeBudget::new(DecodeLimits {
        decoded_bytes: 4096,
        ..DecodeLimits::default()
    });
    LARGEST.with(|largest| largest.set(Some(0)));
    let result = C::decompress_with_budget(&mut input, &mut budget, |_, _| Ok(()));
    let largest = LARGEST.with(|largest| largest.replace(None).unwrap());
    assert!(result.unwrap_err().is::<DecodeLimitExceeded>());
    // Allows fixed Rust scratch buffers (e.g. Zstd's ~128 KiB input reader),
    // but detects a regression that allocates the full 512 KiB decoded payload.
    assert!(
        largest < 192 * 1024,
        "{} requested {largest} bytes",
        std::any::type_name::<C>()
    );
}

#[test]
fn output_is_bounded_before_full_payload_allocation() {
    check::<compression::None>();
    #[cfg(feature = "gzip")]
    check::<compression::Gzip>();
    #[cfg(feature = "snappy")]
    check::<compression::Snappy>();
    #[cfg(feature = "lz4")]
    check::<compression::Lz4>();
    #[cfg(feature = "zstd")]
    check::<compression::Zstd>();
}
