/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{error::Error, fmt, str::Utf8Error};
use storage::snapshot::iterator::SnapshotIteratorError;

#[derive(Debug)]
pub struct EncodingError {
    pub kind: EncodingErrorKind,
}

#[derive(Debug)]
pub enum EncodingErrorKind {
    FailedUFT8Decode { bytes: Box<[u8]>, source: Utf8Error },
    FailedTypeIDAllocation {source:  std::sync::Arc<SnapshotIteratorError> },
    ExhaustedTypeIDs
}

impl fmt::Display for EncodingError {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl Error for EncodingError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self.kind {
            EncodingErrorKind::FailedUFT8Decode { source, .. } => Some(source),
            EncodingErrorKind::FailedTypeIDAllocation { source, .. } => Some(source),
            EncodingErrorKind::ExhaustedTypeIDs => None,
        }
    }
}
