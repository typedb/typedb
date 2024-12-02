/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{ops::Deref, sync::Mutex};

pub trait Poolable {}

pub struct SinglePool<T: Poolable> {
    pool: Mutex<Vec<T>>,
}

impl<T: Poolable> SinglePool<T> {
    pub fn new() -> Self {
        Self { pool: Mutex::new(Vec::new()) }
    }

    pub fn get_or_create(&self, default: impl FnOnce() -> T) -> PoolRecycleGuard<'_, T> {
        let mut unlocked = self.pool.try_lock().unwrap();
        if let Some(item) = unlocked.pop() {
            PoolRecycleGuard { item: Some(item), pool: self }
        } else {
            drop(unlocked);
            PoolRecycleGuard { item: Some(default()), pool: self }
        }
    }

    fn recycle(&self, item: T) {
        let mut unlocked = self.pool.try_lock().unwrap();
        unlocked.push(item);
    }
}

pub struct PoolRecycleGuard<'pool, T: Poolable> {
    item: Option<T>,
    pool: &'pool SinglePool<T>,
}

impl<'pool, T: Poolable> Deref for PoolRecycleGuard<'pool, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.item.as_ref().unwrap()
    }
}

impl<'pool, T: Poolable> Drop for PoolRecycleGuard<'pool, T> {
    fn drop(&mut self) {
        debug_assert!(self.item.is_some());
        let item = self.item.take().unwrap();
        self.pool.recycle(item)
    }
}
