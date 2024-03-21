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

use std::{error::Error, fmt, io, path::PathBuf};

use storage::error::MVCCStorageError;

#[derive(Debug)]
pub struct DatabaseError {
    pub database_name: String,
    pub kind: DatabaseErrorKind,
}

#[derive(Debug)]
pub enum DatabaseErrorKind {
    FailedToCreateDirectory { at: PathBuf, source: io::Error },
    FailedToCreateStorage(MVCCStorageError),
    FailedToSetupStorage(MVCCStorageError),
}

impl fmt::Display for DatabaseError {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl Error for DatabaseError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self.kind {
            DatabaseErrorKind::FailedToCreateDirectory { source, .. } => Some(source),
            DatabaseErrorKind::FailedToCreateStorage(storage_error) => Some(storage_error),
            DatabaseErrorKind::FailedToSetupStorage(storage_error) => Some(storage_error),
        }
    }
}
