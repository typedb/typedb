/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::HashMap,
    sync::{Arc, LockResult, RwLock, RwLockReadGuard, RwLockWriteGuard},
};

use storage::durability_client::WALClient;

use crate::Database;

pub(crate) type DatabasesMap = HashMap<String, Arc<Database<WALClient>>>;

#[derive(Debug)]
pub(crate) struct Databases {
    pub(crate) served: DatabasesMap,
    pub(crate) staged: DatabasesMap,
}

pub(crate) type DatabasesReadLock<'a> = RwLockReadGuard<'a, Databases>;
pub(crate) type DatabasesWriteLock<'a> = RwLockWriteGuard<'a, Databases>;

#[derive(Debug)]
pub(crate) struct DatabaseRegistry {
    databases: RwLock<Databases>,
}

impl DatabaseRegistry {
    pub(crate) fn new(served: DatabasesMap, staged: DatabasesMap) -> Self {
        Self { databases: RwLock::new(Databases { served, staged }) }
    }

    pub(crate) fn read(&self) -> DatabasesReadLock<'_> {
        self.databases.read().unwrap()
    }

    pub(crate) fn write(&self) -> LockResult<DatabasesWriteLock<'_>> {
        self.databases.write()
    }

    pub(crate) fn served(&self, name: &str) -> Option<Arc<Database<WALClient>>> {
        self.read().served.get(name).cloned()
    }

    pub(crate) fn served_names(&self) -> Vec<String> {
        self.read().served.keys().cloned().collect()
    }

    pub(crate) fn staged(&self, name: &str) -> Option<Arc<Database<WALClient>>> {
        self.read().staged.get(name).cloned()
    }

    pub(crate) fn staged_names(&self) -> Vec<String> {
        self.read().staged.keys().cloned().collect()
    }
}
