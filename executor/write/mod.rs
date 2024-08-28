/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    error::Error,
    fmt::{Display, Formatter},
};

use concept::error::ConceptWriteError;

pub mod common;
pub mod delete;
pub mod insert;
pub(crate) mod write_instruction;

#[derive(Debug, Clone)]
pub enum WriteError {
    ConceptWrite { source: ConceptWriteError },
}

impl Display for WriteError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl Error for WriteError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::ConceptWrite { source, .. } => Some(source),
        }
    }
}
