/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{error::Error, fmt, sync::Arc};

use error::typedb_error;
use storage::snapshot::{iterator::SnapshotIteratorError, SnapshotGetError};
use typeql::schema::definable::function::{Function, ReturnStream};

use crate::{program::function_signature::FunctionID, PatternDefinitionError};

pub mod block;
pub mod function;
pub mod function_signature;
pub mod modifier;
pub mod reduce;

#[derive(Debug, Clone)]
pub enum FunctionReadError {
    FunctionNotFound { function_id: FunctionID },
    FunctionRetrieval { source: SnapshotGetError },
    FunctionsScan { source: Arc<SnapshotIteratorError> },
}

impl fmt::Display for FunctionReadError {
    fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl Error for FunctionReadError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::FunctionRetrieval { source } => Some(source),
            Self::FunctionsScan { source } => Some(source),
            Self::FunctionNotFound { .. } => None,
        }
    }
}

typedb_error!(
    pub FunctionRepresentationError(component = "Function representation", prefix = "FRP") {
        FunctionArgumentUnused(
            1,
            "Function argument variable '{argument_variable}' is unused.\nSource:\n{declaration}",
            argument_variable: String,
            declaration: Function
        ),
        ReturnVariableUnavailable(
            2,
            "Function return variable '{return_variable}' is not available or defined.\nSource:\n{declaration:?}", // TODO: formatted
            return_variable: String,
            declaration: ReturnStream
        ),
        PatternDefinition(
            3,
            "Function pattern contains an error.\nSource:\n{declaration}",
            declaration: Function,
            ( typedb_source : PatternDefinitionError )
        ),
    }
);
