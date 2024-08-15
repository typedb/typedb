/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{error::Error, fmt};

use compiler::match_::inference::TypeInferenceError;
use encoding::error::EncodingError;
use ir::program::{FunctionDefinitionError, FunctionReadError};
use storage::{snapshot::SnapshotGetError, ReadSnapshotOpenError};

pub mod function;
pub mod function_cache;
pub mod function_manager;

#[derive(Debug)]
pub enum FunctionError {
    SnapshotOpen { source: ReadSnapshotOpenError },
    SnapshotGet { source: SnapshotGetError },
    TypeInference { source: TypeInferenceError },
    FunctionDefinition { source: FunctionDefinitionError },
    FunctionAlreadyExists { name: String },
    Encoding { source: EncodingError },
    FunctionRead { source: FunctionReadError },
    ParseError { source: typeql::common::error::Error },
}

impl fmt::Display for FunctionError {
    fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl Error for FunctionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::SnapshotOpen { source } => Some(source),
            Self::SnapshotGet { source } => Some(source),
            Self::FunctionDefinition { source } => Some(source),
            Self::Encoding { source } => Some(source),
            Self::FunctionRead { source } => Some(source),
            Self::ParseError { source } => Some(source),
            Self::TypeInference { source } => Some(source),
            Self::FunctionAlreadyExists { .. } => None,
        }
    }
}
