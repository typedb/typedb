/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    ops::{Deref, DerefMut},
    sync::{Arc, Mutex},
};

pub trait Poolable {}

#[derive(Debug)]
pub struct LIFOPool<T> {
    pool: Arc<Mutex<Vec<T>>>,
    size_cap: Option<usize>,
}

impl<T> Default for LIFOPool<T> {
    fn default() -> Self {
        Self { pool: Default::default(), size_cap: None }
    }
}

impl<T> Clone for LIFOPool<T> {
    fn clone(&self) -> Self {
        Self { pool: self.pool.clone(), size_cap: self.size_cap.clone() }
    }
}

impl<T: Poolable> LIFOPool<T> {
    pub fn new_capped(size: usize) -> Self {
        Self {
            pool: Default::default(),
            size_cap: Some(size)
        }
    }

    pub fn get_or_create(&self, create_fn: impl FnOnce() -> T) -> PoolRecycleGuard<T> {
        let mut unlocked = self.pool.lock().unwrap();
        if let Some(item) = unlocked.pop() {
            PoolRecycleGuard { item: Some(item), pool: self.clone() }
        } else {
            drop(unlocked);
            PoolRecycleGuard { item: Some(create_fn()), pool: self.clone() }
        }
    }

    fn recycle(&self, item: T) {
        let mut unlocked = self.pool.lock().unwrap();
        if self.size_cap.is_some_and(|cap| unlocked.len() < cap) {
            unlocked.push(item);
        }
    }
}

pub struct PoolRecycleGuard<T: Poolable> {
    item: Option<T>,
    pool: LIFOPool<T>,
}

impl<T: Poolable> Deref for PoolRecycleGuard<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.item.as_ref().unwrap()
    }
}

impl<T: Poolable> DerefMut for PoolRecycleGuard<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.item.as_mut().unwrap()
    }
}

impl<T: Poolable> Drop for PoolRecycleGuard<T> {
    fn drop(&mut self) {
        debug_assert!(self.item.is_some());
        let item = self.item.take().unwrap();
        self.pool.recycle(item)
    }
}
