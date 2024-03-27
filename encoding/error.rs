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

use std::{error::Error, fmt, str::Utf8Error};

use storage::snapshot::SnapshotPutError;

#[derive(Debug)]
pub struct EncodingError {
    pub kind: EncodingErrorKind,
}

#[derive(Debug)]
pub enum EncodingErrorKind {
    FailedUFT8Decode { bytes: Box<[u8]>, source: Utf8Error },
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
        }
    }
}

#[derive(Debug)]
pub enum EncodingWriteError {
    SnapshotPut { source: SnapshotPutError },
}

impl fmt::Display for EncodingWriteError {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl Error for EncodingWriteError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self {
            Self::SnapshotPut { source, .. } => Some(source),
        }
    }
}
