/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{error::Error, fmt, str::Utf8Error};

use storage::snapshot::iterator::SnapshotIteratorError;

use crate::layout::prefix::Prefix;

#[derive(Debug, Clone)]
pub enum EncodingError {
    UFT8Decode { bytes: Box<[u8]>, source: Utf8Error },
    SchemaIDAllocate { source: std::sync::Arc<SnapshotIteratorError> },
    ExistingTypesRead { source: std::sync::Arc<SnapshotIteratorError> },
    TypeIDsExhausted { kind: crate::graph::type_::Kind },
    UnexpectedPrefix { expected_prefix: Prefix, actual_prefix: Prefix },
    DefinitionIDsExhausted { prefix: Prefix },
}

impl fmt::Display for EncodingError {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl Error for EncodingError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::UFT8Decode { source, .. } => Some(source),
            Self::SchemaIDAllocate { source, .. } => Some(source),
            Self::ExistingTypesRead { source, .. } => Some(source),
            Self::TypeIDsExhausted { .. } => None,
            Self::DefinitionIDsExhausted { .. } => None,
            Self::UnexpectedPrefix { .. } => None,
        }
    }
}
