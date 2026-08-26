/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    fmt,
    hint::spin_loop,
    marker::PhantomData,
    ptr::null,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicPtr, Ordering},
    },
};

pub struct AtomicArcOption<T> {
    // A spin lock used to prevent interleaving `swap_impl` and `clone_arc`.
    // `ptr` must not be read with the intention of dereferencing, unless the `busy` flag is acquired.
    busy: AtomicBool,
    // Invariant: either null, or pointing to a `T` in a live `Arc` store.
    ptr: AtomicPtr<T>,
    // `AtomicArcOption<T>` behaves as if it contains an `Arc<T>`
    _pd: PhantomData<Arc<T>>,
}

impl<T> AtomicArcOption<T> {
    /// Constructs a new `AtomicArcOption<T>`.
    pub fn new(value: T) -> Self {
        Self::from_arc(Arc::new(value))
    }

    /// Converts an `Arc<T>` into an `AtomicArcOption<T>`.
    pub fn from_arc(arc: Arc<T>) -> Self {
        // `cast_mut` is necessary because AtomicPtr expects a `*mut T`.
        // This is safe because `AtomicArcOption` behaves the same as `Arc`, i.e. it only exposes `*const T`.
        Self { ptr: AtomicPtr::new(Arc::into_raw(arc).cast_mut()), busy: AtomicBool::new(false), _pd: PhantomData }
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

    /// Returns `true` if this `AtomicArcOption<T>` is `Some`.
    ///
    /// Note: another thread can take or swap the inner `Arc<T>` at any time, including potentially
    /// between calling this method and acting on the result.
    pub fn is_some(&self) -> bool {
        !self.is_none()
    }

    /// Returns `true` if this `AtomicArcOption<T>` is `None`.
    ///
    /// Note: another thread can take or swap the inner `Arc<T>` at any time, including potentially
    /// between calling this method and acting on the result.
    pub fn is_none(&self) -> bool {
        self.ptr.load(Ordering::Relaxed).is_null()
    }

    fn acquire(&self) -> Guard<'_> {
        // At the longest this waits for an atomic load and a fetch_add (in `clone_arc`)
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
        self.take();
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc, Barrier,
            atomic::{AtomicUsize, Ordering},
        },
        thread,
    };

    use super::*;

    const NUM_THREADS: usize = 16;
    const OPS_PER_THREAD: usize = 10_000;

    macro_rules! run_threads {
        ($body:expr) => {
            let barrier = Arc::new(Barrier::new(NUM_THREADS));

            thread::scope(|s| {
                for _ in 0..NUM_THREADS {
                    s.spawn(|| {
                        barrier.wait();
                        $body
                    });
                }
            })
        };
    }

    #[test]
    fn test_concurrent_operations() {
        let atomic_opt = Arc::new(AtomicArcOption::from_arc(Arc::new(String::from("initial"))));

        run_threads! {
            for j in 0..OPS_PER_THREAD  {
                _ = atomic_opt.clone_arc();
                _ = atomic_opt.take();
                _ = atomic_opt.swap(Arc::new(format!("swap_{j}")));
                atomic_opt.store(Arc::new(format!("store_{j}")));
            }
        }
    }

    #[test]
    fn test_concurrent_clone_arc_stress() {
        let atomic_opt = Arc::new(AtomicArcOption::from_arc(Arc::new(String::from("initial"))));
        let total_clones = Arc::new(AtomicUsize::new(0));
        run_threads! {
            for _ in 0..OPS_PER_THREAD {
                if let Some(arc) = atomic_opt.clone_arc() {
                    assert_eq!(*arc, "initial");
                    total_clones.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        assert_eq!(total_clones.load(Ordering::Relaxed), NUM_THREADS * OPS_PER_THREAD);
    }

    #[test]
    fn test_concurrent_take_and_swap() {
        let atomic_opt = Arc::new(AtomicArcOption::from_arc(Arc::new(String::from("initial"))));
        let ops_count = Arc::new(AtomicUsize::new(0));
        run_threads! {
            for j in 0..OPS_PER_THREAD {
                _ = atomic_opt.take();
                _ = atomic_opt.swap(Arc::new(format!("{j}")));
                ops_count.fetch_add(1, Ordering::Relaxed);
            }
        }
        assert_eq!(ops_count.load(Ordering::Relaxed), NUM_THREADS * OPS_PER_THREAD);
    }

    #[test]
    fn test_external_arc_drop() {
        let initial = Arc::new(42);
        let atomic_opt = Arc::new(AtomicArcOption::from_arc(initial.clone()));
        run_threads! {
            for _ in 0..OPS_PER_THREAD {
                drop(atomic_opt.clone_arc());
            }
        }
        drop(initial);
        assert!(atomic_opt.take().is_some());
    }

    #[test]
    fn test_clone_drop() {
        let atomic_opt = Arc::new(AtomicArcOption::from_arc(Arc::new(99)));
        let clone = atomic_opt.clone_arc().unwrap();
        run_threads! {
            for _ in 0..OPS_PER_THREAD {
                drop(atomic_opt.clone_arc());
            }
        }
        assert!(atomic_opt.take().is_some());
        drop(clone);
    }

    #[test]
    fn test_concurrent_clone_arc_weak_upgrade_stress() {
        let atomic_opt = Arc::new(AtomicArcOption::from_arc(Arc::new(42)));
        run_threads! {
            for _ in 0..OPS_PER_THREAD {
                if let Some(arc) = atomic_opt.clone_arc() {
                    let weak = Arc::downgrade(&arc);
                    drop(arc);
                    let _ = weak.upgrade();
                }
            }
        }
    }
}
