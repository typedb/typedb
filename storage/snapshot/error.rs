/*
 * Copyright (C) 2023 Vaticle
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

use std::error::Error;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

use crate::error::MVCCStorageError;

#[derive(Debug)]
pub struct SnapshotError {
    pub kind: SnapshotErrorKind,
}

#[derive(Debug)]
pub enum SnapshotErrorKind {
    FailedIterate { source: Arc<SnapshotError> },
    FailedGet { source: MVCCStorageError },
    FailedPut { source: MVCCStorageError },
    FailedMVCCStorageIterate { source: MVCCStorageError },
    FailedCommit { source: MVCCStorageError },
}

impl Display for SnapshotError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self.kind {
            SnapshotErrorKind::FailedIterate { source, .. } => write!(f, "SnapshotError.FailedIterate caused by: {}", source),
            SnapshotErrorKind::FailedGet { source, .. } => write!(f, "SnapshotError.FailedGet caused by: {}", source),
            SnapshotErrorKind::FailedPut { source, .. } => write!(f, "SnapshotError.FailedPut caused by: {}", source),
            SnapshotErrorKind::FailedMVCCStorageIterate { source, .. } => write!(f, "SnapshotError.FailedMVCCStorageIterate caused by: {}", source),
            SnapshotErrorKind::FailedCommit { source, .. } => write!(f, "SnapshotError.FailedCommit caused by: {}", source),
        }
    }
}

impl Error for SnapshotError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self.kind {
            SnapshotErrorKind::FailedIterate { source, .. } => Some(source),
            SnapshotErrorKind::FailedGet { source, .. } => Some(source),
            SnapshotErrorKind::FailedPut { source, .. } => Some(source),
            SnapshotErrorKind::FailedMVCCStorageIterate { source, .. } => Some(source),
            SnapshotErrorKind::FailedCommit { source, .. } => Some(source),
        }
    }
}

