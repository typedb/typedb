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
 *
 */

use std::error::Error;
use std::fmt::{Debug, Display, Formatter};

#[derive(Debug)]
pub(crate) struct StorageError {
    pub(crate) storage_name: String,
    pub(crate) kind: StorageErrorKind,
}

#[derive(Debug)]
pub(crate) enum StorageErrorKind {
    FailedToCreateSection { section_name: String, source: speedb::Error },
    FailedToGetSectionHandle { section_name: String },
    FailedToDeleteStorage { section_name:  source: std::io::Error },
}

impl Display for StorageError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl Error for StorageError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self.kind {
            StorageErrorKind::FailedToCreateSection { source, .. } => Some(source),
            StorageErrorKind::FailedToGetSectionHandle { .. } => None,
            StorageErrorKind::FailedToDeleteStorage { source, .. } => Some(source),
            StorageErrorKind::FailedToDeleteSection { source, .. } => Some(source),
        }
    }
}
