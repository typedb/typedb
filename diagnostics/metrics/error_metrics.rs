/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::HashMap,
    sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard},
};

use crate::{DatabaseId, metrics::get_delta, reports::ErrorReport};

#[derive(Debug)]
pub(crate) struct ErrorMetrics {
    /// `None` denotes the server-level (no-database) metrics record.
    database_id: Option<Arc<DatabaseId>>,
    errors: RwLock<HashMap<String, ErrorInfo>>,
    errors_snapshot: RwLock<HashMap<String, ErrorInfo>>,
    errors_snapshot_backup: RwLock<HashMap<String, ErrorInfo>>,
}

impl ErrorMetrics {
    pub fn new(database_id: Option<Arc<DatabaseId>>) -> Self {
        Self {
            database_id,
            errors: RwLock::new(HashMap::new()),
            errors_snapshot: RwLock::new(HashMap::new()),
            errors_snapshot_backup: RwLock::new(HashMap::new()),
        }
    }

    pub fn database_id(&self) -> Option<&Arc<DatabaseId>> {
        self.database_id.as_ref()
    }

    pub fn submit(&self, error_code: String) {
        self.get_errors_mut().entry(error_code).or_insert(ErrorInfo::new()).submit();
    }

    fn get_count_delta(&self, error_code: &str) -> i64 {
        get_delta(
            self.get_errors().get(error_code).unwrap_or(&ErrorInfo::default()).count,
            self.get_errors_snapshot().get(error_code).unwrap_or(&ErrorInfo::default()).count,
        )
    }

    pub fn take_snapshot(&mut self) {
        let errors = self.get_errors();
        let mut snapshot = self.get_errors_snapshot_mut();
        let mut backup = self.get_errors_snapshot_backup_mut();

        *backup = snapshot.clone();
        for (code, info) in errors.iter() {
            snapshot.insert(code.clone(), info.clone());
        }
    }

    pub fn restore_snapshot(&self) {
        let backup = self.get_errors_snapshot_backup().clone();
        let mut snapshot = self.get_errors_snapshot_mut();
        *snapshot = backup;
    }

    pub fn to_diff_reports(&self) -> Vec<ErrorReport> {
        let mut errors = vec![];
        for code in self.get_errors().keys() {
            let count = self.get_count_delta(code);
            if count == 0 {
                continue;
            }
            errors.push(ErrorReport { database: self.database_id.clone(), code: code.clone(), count });
        }
        errors
    }

    pub fn to_state_reports(&self) -> Vec<ErrorReport> {
        let mut errors = vec![];
        for (code, info) in self.get_errors().iter() {
            assert_ne!(info.count, 0, "Error count cannot be 0");
            errors.push(ErrorReport {
                database: self.database_id.clone(),
                code: code.clone(),
                count: info.count as i64,
            });
        }
        errors
    }

    pub fn get_errors(&self) -> RwLockReadGuard<'_, HashMap<String, ErrorInfo>> {
        self.errors.read().expect("Expected error metrics read lock acquisition")
    }

    pub fn get_errors_mut(&self) -> RwLockWriteGuard<'_, HashMap<String, ErrorInfo>> {
        self.errors.write().expect("Expected error metrics write lock acquisition")
    }

    pub fn get_errors_snapshot(&self) -> RwLockReadGuard<'_, HashMap<String, ErrorInfo>> {
        self.errors_snapshot.read().expect("Expected error metrics snapshot read lock acquisition")
    }

    pub fn get_errors_snapshot_mut(&self) -> RwLockWriteGuard<'_, HashMap<String, ErrorInfo>> {
        self.errors_snapshot.write().expect("Expected error metrics snapshot write lock acquisition")
    }

    pub fn get_errors_snapshot_backup(&self) -> RwLockReadGuard<'_, HashMap<String, ErrorInfo>> {
        self.errors_snapshot_backup.read().expect("Expected error metrics snapshot backup read lock acquisition")
    }

    pub fn get_errors_snapshot_backup_mut(&self) -> RwLockWriteGuard<'_, HashMap<String, ErrorInfo>> {
        self.errors_snapshot_backup.write().expect("Expected error metrics snapshot backup write lock acquisition")
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ErrorInfo {
    count: u64,
}

impl ErrorInfo {
    pub const fn new() -> Self {
        Self::default()
    }

    pub const fn default() -> Self {
        Self { count: 0 }
    }

    pub fn submit(&mut self) {
        self.count += 1;
    }
}
