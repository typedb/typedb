/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    fmt,
    hint::spin_loop,
    ptr::null,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicPtr, Ordering},
    },
};

pub struct AtomicArcOption<T> {
    busy: AtomicBool,
    ptr: AtomicPtr<T>,
}

impl<T> AtomicArcOption<T> {
    /// Constructs a new `AtomicArc<T>`.
    pub fn new(value: T) -> Self {
        Self::from_arc(Arc::new(value))
    }

    /// Converts an `Arc<T>` into an `AtomicArc<T>`.
    pub fn from_arc(arc: Arc<T>) -> Self {
        // `cast_mut` is necessary because AtomicPtr expects a `*mut T`.
        // This is safe because `AtomicArc` behaves the same as `Arc`, i.e. it only exposes `*const T`.
        Self { ptr: AtomicPtr::new(Arc::into_raw(arc).cast_mut()), busy: AtomicBool::new(false) }
    }

    /// Returns the inner `Arc<T>`, atomically replacing it with `None`.
    pub fn take(&self) -> Option<Arc<T>> {
        self.swap_impl(None)
    }

    /// Atomically replaces the inner `Arc<T>`, returning the old value.
    pub fn swap(&self, arc: Arc<T>) -> Option<Arc<T>> {
        self.swap_impl(Some(arc))
    }

    /// Stores another `Arc<T>` in `self`, atomically, dropping the old value.
    pub fn store(&self, arc: Arc<T>) {
        drop(self.swap(arc));
    }

    fn swap_impl(&self, arc: Option<Arc<T>>) -> Option<Arc<T>> {
        let old_ptr = {
            let new_ptr = arc.map(Arc::into_raw).unwrap_or(null()).cast_mut();
            // Make sure no other internals can be reading the old ptr.
            let _guard = self.acquire();
            self.ptr.swap(new_ptr, Ordering::AcqRel)
        };

        if old_ptr.is_null() {
            return None;
        }

        // SAFETY: the inner `ptr`, if not null, could only have have come from an `Arc<T>::into_raw`.
        unsafe { Some(Arc::from_raw(old_ptr)) }
    }

    /// Clones the inner `Arc<T>`.
    pub fn clone_arc(&self) -> Option<Arc<T>> {
        // Make sure the old ptr isn't read and the `Arc` dropped while we're doing this.
        let _guard = self.acquire();
        let ptr = self.ptr.load(Ordering::Acquire).cast_const();
        if ptr.is_null() {
            return None;
        }
        unsafe {
            // SAFETY: the inner `ptr` could only have have come from an `Arc<T>::into_raw`.
            // This is a manual implementation of `Arc::clone` that keeps the raw pointer accounted for in the strong count.
            Arc::increment_strong_count(ptr);
            Some(Arc::from_raw(ptr))
        }
    }

    fn acquire(&self) -> Guard<'_> {
        while self.busy.compare_exchange_weak(false, true, Ordering::SeqCst, Ordering::SeqCst).is_err() {
            spin_loop();
        }
        Guard(&self.busy)
    }
}

#[must_use]
struct Guard<'a>(&'a AtomicBool);

impl Drop for Guard<'_> {
    fn drop(&mut self) {
        // The flag is only set to `true` by CAS(false, true).
        self.0.store(false, Ordering::SeqCst);
    }
}

impl<T: fmt::Debug> fmt::Debug for AtomicArcOption<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let _guard = self.acquire();
        let ptr = self.ptr.load(Ordering::Acquire);
        if ptr.is_null() { write!(f, "None") } else { write!(f, "Some({:?})", unsafe { &*ptr }) }
    }
}

impl<T> From<Arc<T>> for AtomicArcOption<T> {
    fn from(value: Arc<T>) -> Self {
        Self::from_arc(value)
    }
}

impl<T> Drop for AtomicArcOption<T> {
    fn drop(&mut self) {
        let ptr = self.ptr.load(Ordering::Acquire);
        if !ptr.is_null() {
            // SAFETY: the inner `ptr` could only have have come from an `Arc<T>::into_raw`.
            unsafe { drop(Arc::from_raw(ptr)) };
        }
    }
}
