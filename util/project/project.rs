/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{ops::Deref, sync::RwLockReadGuard};

pub struct RwLockReadGuardProject<'a, T: ?Sized + 'a, U: ?Sized + 'a> {
    projection: &'a U,
    guard: RwLockReadGuard<'a, T>,
}

impl<'a, T: ?Sized + 'a, U: ?Sized + 'a> RwLockReadGuardProject<'a, T, U> {
    /// # Safety
    /// TODO
    pub unsafe fn new(projection: &'a U, _guard: RwLockReadGuard<'a, T>) -> Self {
        Self { projection, guard: _guard }
    }
}

impl<'a, T: ?Sized + 'a, U: ?Sized + 'a> Deref for RwLockReadGuardProject<'a, T, U> {
    type Target = U;
    fn deref(&self) -> &Self::Target {
        self.projection
    }
}

pub trait ReadGuardWrap<'a, T: ?Sized + 'a> {
    fn into_guard(self) -> RwLockReadGuard<'a, T>;
}

impl<'a, T: ?Sized + 'a> ReadGuardWrap<'a, T> for RwLockReadGuard<'a, T> {
    fn into_guard(self) -> RwLockReadGuard<'a, T> {
        self
    }
}

impl<'a, T: ?Sized + 'a, U: ?Sized + 'a> ReadGuardWrap<'a, T> for RwLockReadGuardProject<'a, T, U> {
    fn into_guard(self) -> RwLockReadGuard<'a, T> {
        self.guard
    }
}

pub trait ReadGuard<'a, T: ?Sized + 'a>: Deref<Target = T> {}
impl<'a, T: ?Sized + 'a> ReadGuard<'a, T> for RwLockReadGuard<'a, T> {}
impl<'a, T: ?Sized + 'a, U: ?Sized + 'a> ReadGuard<'a, U> for RwLockReadGuardProject<'a, T, U> {}

#[macro_export]
macro_rules! read_guard_project {
    ($guard:expr => $field:ident) => {
        unsafe {
            // SAFETY: this fn takes in a ref and ensures the pointer is aligned and safe to turn back into ref
            // addr_of!() does not provide that guarantee
            fn as_ptr<T>(t: &T) -> *const T {
                t as *const T
            }
            let guard = $guard;
            $crate::RwLockReadGuardProject::new(
                as_ptr(&guard.$field).as_ref().unwrap(),
                $crate::ReadGuardWrap::into_guard(guard),
            )
        }
    };
    ($guard:ident => $projection:expr) => {
        unsafe {
            // SAFETY: $projection is already a reference and is therefor properly aligned
            let ptr = std::ptr::addr_of!(*$projection);
            $crate::RwLockReadGuardProject::new(ptr.as_ref().unwrap(), $crate::ReadGuardWrap::into_guard($guard))
        }
    };
}

#[cfg(test)]
mod test {
    use std::sync::{RwLock, RwLockReadGuard};

    use super::{read_guard_project, RwLockReadGuardProject};

    #[test]
    fn field_projection_test() {
        struct Test {
            foo: u8,
            bar: u8,
        }

        let rwlock = RwLock::new(Test { foo: 0xDE, bar: 0xAD });
        let foo = read_guard_project!(rwlock.read().unwrap() => foo);
        assert_eq!(*foo, 0xDE);
        assert!(rwlock.try_write().is_err());

        let bar = read_guard_project!(rwlock.read().unwrap() => bar);
        assert_eq!(*bar, 0xAD);
        assert!(rwlock.try_write().is_err());

        drop(foo);
        assert!(rwlock.try_write().is_err()); // bar is still holding a read lock

        drop(bar);
        assert!(rwlock.try_write().is_ok());
    }

    #[test]
    fn enum_projection_test() {
        enum Test {
            Foo(u8),
            Bar(u8, u16),
        }

        fn project(guard: RwLockReadGuard<'_, Test>) -> RwLockReadGuardProject<'_, Test, u8> {
            read_guard_project!(guard => {
                match &*guard {
                    Test::Foo(a) => a,
                    Test::Bar(a, _) => a,
                }
            })
        }

        let rwlock = RwLock::new(Test::Foo(0xAB));
        let foo = project(rwlock.read().unwrap());
        assert_eq!(*foo, 0xAB);
        assert!(rwlock.try_write().is_err());
    }
}
