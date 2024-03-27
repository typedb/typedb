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

use encoding::{error::EncodingWriteError, value::value_type::ValueType};
use storage::snapshot::{SnapshotError, SnapshotGetError, SnapshotPutError};

#[derive(Debug)]
pub struct ConceptError {
    pub kind: ConceptErrorKind,
}

#[derive(Debug)]
pub enum ConceptErrorKind {
    SnapshotError { source: SnapshotError },
    AttributeValueTypeMismatch { attribute_type_value_type: Option<ValueType>, provided_value_type: ValueType },
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
            ConceptErrorKind::AttributeValueTypeMismatch { .. } => None,
        }
    }
}

#[derive(Debug)]
pub enum ConceptWriteError {
    SnapshotPut { source: SnapshotPutError },
    SnapshotGet { source: SnapshotGetError },

    EncodingWrite { source: EncodingWriteError },
    ValueTypeMismatch { expected: Option<ValueType>, provided: ValueType },
}

impl fmt::Display for ConceptWriteError {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl Error for ConceptWriteError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::SnapshotPut { source, .. } => Some(source),
            Self::SnapshotGet { source, .. } => Some(source),
            Self::EncodingWrite { source, .. } => Some(source),
            Self::ValueTypeMismatch {..} => todo!(),
        }
    }
}

impl From<ConceptReadError> for ConceptWriteError {
    fn from(error: ConceptReadError) -> Self {
        match error {
            ConceptReadError::SnapshotGet { source } => Self::SnapshotGet { source },
        }
    }
}

#[derive(Debug)]
pub enum ConceptReadError {
    SnapshotGet { source: SnapshotGetError },
}

impl fmt::Display for ConceptReadError {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl Error for ConceptReadError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::SnapshotGet { source, .. } => Some(source),
        }
    }
}
