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
use std::fmt::{Debug, Display, Formatter};
use crate::isolation_manager::IsolationError;

use crate::SectionError;

#[derive(Debug)]
pub struct StorageError {
    pub storage_name: String,
    pub kind: StorageErrorKind,
}

#[derive(Debug)]
pub enum StorageErrorKind {
    FailedToDeleteStorage { source: std::io::Error },
    SectionError { source: SectionError },
    IsolationError { source: IsolationError },
}

impl Display for StorageError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl Error for StorageError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self.kind {
            StorageErrorKind::FailedToDeleteStorage { source, .. } => Some(source),
            StorageErrorKind::SectionError { source, .. } => Some(source),
            StorageErrorKind::IsolationError { source, .. } => Some(source),
        }
    }
}
