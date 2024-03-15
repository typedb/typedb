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

use std::{
    error::Error,
    fmt::{Debug, Display, Formatter},
    sync::Arc,
};

use durability::DurabilityError;

use crate::{
    isolation_manager::IsolationError,
    keyspace::keyspace::{KeyspaceError, KeyspaceId},
};

#[derive(Debug)]
pub struct MVCCStorageError {
    pub storage_name: String,
    pub kind: MVCCStorageErrorKind,
}

#[derive(Debug)]
pub enum MVCCStorageErrorKind {
    FailedToDeleteStorage { source: std::io::Error },
    KeyspaceNameExists { keyspace: String },
    KeyspaceIdReserved { keyspace: String, keyspace_id: KeyspaceId },
    KeyspaceIdTooLarge { keyspace: String, keyspace_id: KeyspaceId, max_keyspace_id: KeyspaceId },
    KeyspaceIdExists { new_keyspace: String, keyspace_id: KeyspaceId, existing_keyspace: String },
    KeyspaceError { source: Arc<KeyspaceError>, keyspace: String },
    KeyspaceDeleteError { source: KeyspaceError },
    IsolationError { source: IsolationError },
    DurabilityError { source: DurabilityError },
}

impl Display for MVCCStorageError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self.kind {
            MVCCStorageErrorKind::FailedToDeleteStorage { source, .. } => {
                write!(f, "MVCCStorageError.FailedToDeleteStorage caused by: '{}'", source)
            }
            MVCCStorageErrorKind::KeyspaceNameExists { keyspace, .. } => {
                write!(f, "MVCCStorageError.KeyspaceNameExists: '{}'", keyspace)
            }
            MVCCStorageErrorKind::KeyspaceIdReserved { keyspace, keyspace_id, .. } => {
                write!(f, "MVCCStorageError.KeyspaceIdReserved: reserved keyspace id '{}' cannot be used for new keyspace '{}'.", keyspace_id, keyspace)
            }
            MVCCStorageErrorKind::KeyspaceIdTooLarge { keyspace, keyspace_id, max_keyspace_id: maximum, .. } => {
                write!(f, "MVCCStorageError.KeyspaceIdTooLarge: keyspace id '{}' cannot be used for new keyspace '{}' since it is larger than maximum keyspace id '{}'.", keyspace_id, keyspace, maximum)
            }
            MVCCStorageErrorKind::KeyspaceIdExists { new_keyspace, keyspace_id, existing_keyspace, .. } => {
                write!(f, "MVCCStorageError.KeyspaceIdExists: keyspace id '{}' cannot be used for new keyspace '{}' since it is already used by keyspace '{}'", keyspace_id, new_keyspace, existing_keyspace)
            }
            MVCCStorageErrorKind::KeyspaceError { source, keyspace, .. } => {
                write!(f, "MVCCStorageError.KeyspaceError in keyspace '{}' caused by: '{}'", keyspace, source)
            }
            MVCCStorageErrorKind::KeyspaceDeleteError { source, .. } => {
                write!(f, "MVCCStorageError.KeyspaceDeleteError caused by: '{}'", source)
            }
            MVCCStorageErrorKind::IsolationError { source, .. } => {
                write!(f, "MVCCStorageError.IsolationError caused by: '{}'", source)
            }
            MVCCStorageErrorKind::DurabilityError { source, .. } => {
                write!(f, "MVCCStorageError.DurabilityError caused by: '{}'", source)
            }
        }
    }
}

impl Error for MVCCStorageError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self.kind {
            MVCCStorageErrorKind::FailedToDeleteStorage { source, .. } => Some(source),
            MVCCStorageErrorKind::KeyspaceError { source, .. } => Some(source),
            MVCCStorageErrorKind::IsolationError { source, .. } => Some(source),
            MVCCStorageErrorKind::DurabilityError { source, .. } => Some(source),
            MVCCStorageErrorKind::KeyspaceNameExists { .. } => None,
            MVCCStorageErrorKind::KeyspaceIdReserved { .. } => None,
            MVCCStorageErrorKind::KeyspaceIdTooLarge { .. } => None,
            MVCCStorageErrorKind::KeyspaceIdExists { .. } => None,
            MVCCStorageErrorKind::KeyspaceDeleteError { source } => Some(source),
        }
    }
}
