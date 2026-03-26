use std::alloc::{alloc, dealloc, Layout};
use std::ptr::NonNull;
use std::sync::atomic::{AtomicUsize, Ordering};

/// A simple bump allocator.
///
/// Memory is allocated in large slabs. Individual allocations cannot be freed
/// — the entire arena is dropped at once when the MemTable is flushed.
/// This design enables lock-free allocation under concurrent write load.
pub struct Arena {
    /// Current slab pointer and remaining bytes
    current_ptr: *mut u8,
    current_remaining: usize,
    /// All allocated slabs (for deallocation)
    slabs: Vec<(*mut u8, Layout)>,
    /// Total bytes allocated (including slab overhead)
    total_allocated: AtomicUsize,
    slab_size: usize,
}

// Safety: Arena is only used inside a Mutex<MemTable>; we never share raw ptrs
// across threads without synchronization.
unsafe impl Send for Arena {}
unsafe impl Sync for Arena {}

const DEFAULT_SLAB_SIZE: usize = 4 * 1024 * 1024; // 4 MiB

impl Arena {
    pub fn new() -> Self {
        Arena::with_slab_size(DEFAULT_SLAB_SIZE)
    }

    pub fn with_slab_size(slab_size: usize) -> Self {
        Arena {
            current_ptr: std::ptr::null_mut(),
            current_remaining: 0,
            slabs: Vec::new(),
            total_allocated: AtomicUsize::new(0),
            slab_size,
        }
    }

    /// Allocate `size` bytes aligned to `align`. Returns a pointer to the allocation.
    ///
    /// # Panics
    /// Panics if `size == 0` or the system allocator returns null.
    pub fn alloc(&mut self, size: usize, align: usize) -> NonNull<u8> {
        assert!(size > 0, "Arena: cannot allocate 0 bytes");

        // Align the current pointer
        let ptr = self.current_ptr as usize;
        let aligned = (ptr + align - 1) & !(align - 1);
        let wasted = aligned - ptr;

        if wasted + size > self.current_remaining {
            self.allocate_slab(size.max(self.slab_size));
            return self.alloc(size, align);
        }

        let result = aligned as *mut u8;
        self.current_ptr = unsafe { result.add(size) };
        self.current_remaining -= wasted + size;
        self.total_allocated.fetch_add(size, Ordering::Relaxed);

        NonNull::new(result).expect("Arena: null pointer after allocation")
    }

    /// Total bytes allocated from this arena (not counting slab overhead)
    pub fn memory_usage(&self) -> usize {
        self.total_allocated.load(Ordering::Relaxed)
    }

    fn allocate_slab(&mut self, min_size: usize) {
        let slab_size = min_size.max(self.slab_size);
        let layout = Layout::from_size_align(slab_size, 8)
            .expect("Arena: invalid slab layout");

        let ptr = unsafe { alloc(layout) };
        assert!(!ptr.is_null(), "Arena: system allocator returned null");

        self.slabs.push((ptr, layout));
        self.current_ptr = ptr;
        self.current_remaining = slab_size;
    }
}

impl Default for Arena {
    fn default() -> Self {
        Arena::new()
    }
}

impl Drop for Arena {
    fn drop(&mut self) {
        for (ptr, layout) in self.slabs.drain(..) {
            unsafe { dealloc(ptr, layout) };
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_allocation() {
        let mut arena = Arena::with_slab_size(1024);
        let p1 = arena.alloc(16, 8);
        let p2 = arena.alloc(32, 8);
        assert_ne!(p1.as_ptr(), p2.as_ptr());
        assert!(arena.memory_usage() >= 48);
    }

    #[test]
    fn oversized_allocation() {
        // Allocation larger than default slab triggers a new slab
        let mut arena = Arena::with_slab_size(64);
        let _p = arena.alloc(256, 8);
        assert!(arena.memory_usage() >= 256);
    }
}
