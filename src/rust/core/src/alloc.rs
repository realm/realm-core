use std::{pin::Pin, ptr::NonNull};

use sys::MemRef;

use crate::Error;

use super::sys;

/// The default system allocator.
///
/// Thread-safe.
pub struct DefaultAllocator {
    alloc: NonNull<sys::Allocator>,
}

unsafe impl Send for DefaultAllocator {}
unsafe impl Sync for DefaultAllocator {}

impl DefaultAllocator {
    pub fn new() -> DefaultAllocator {
        DefaultAllocator {
            alloc: NonNull::new(sys::get_default_allocator())
                .expect("Allocator::get_default() somehow returned NULL"),
        }
    }
}

/// The allocator associated with a Realm transaction.
///
/// Not thread-safe.
pub struct SlabAlloc {
    alloc: NonNull<sys::Allocator>,
}

pub trait Allocator {
    fn get_sys(&self) -> &sys::Allocator;

    unsafe fn translate(&self, ref_: usize) -> *mut u8 {
        self.get_sys().translate(ref_) as _
    }

    fn alloc(&self, size: usize) -> Result<MemRef, Error> {
        Ok(sys::allocator_alloc(self.get_sys(), size)?)
    }

    unsafe fn free(&self, mem: MemRef) {
        sys::allocator_free(self.get_sys(), mem)
    }

    fn is_read_only(&self, ref_: usize) -> bool {
        self.get_sys().is_read_only(ref_)
    }
}

impl Allocator for DefaultAllocator {
    fn get_sys(&self) -> &sys::Allocator {
        unsafe { self.alloc.as_ref() }
    }
}

impl Allocator for SlabAlloc {
    fn get_sys(&self) -> &sys::Allocator {
        unsafe { self.alloc.as_ref() }
    }
}
