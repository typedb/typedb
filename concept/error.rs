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

use std::{error::Error, fmt};

use storage::snapshot::error::SnapshotError;

#[derive(Debug)]
pub struct ConceptError {
    pub kind: ConceptErrorKind,
}

#[derive(Debug)]
pub enum ConceptErrorKind {
    SnapshotError { source: SnapshotError },
}

impl fmt::Display for ConceptError {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl Error for ConceptError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self.kind {
            ConceptErrorKind::SnapshotError { source, .. } => Some(source),
        }
    }
}
