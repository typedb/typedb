/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{error::Error, fmt, fmt::Display, sync::Arc};

use storage::snapshot::{iterator::SnapshotIteratorError, SnapshotGetError};

use crate::{program::function_signature::FunctionID, PatternDefinitionError};

pub mod block;
pub mod function;
pub mod function_signature;
pub mod modifier;
// mod pipeline;

#[derive(Debug)]
pub enum FunctionReadError {
    FunctionNotFound { function_id: FunctionID },
    SnapshotGet { source: SnapshotGetError },
    SnapshotIterate { source: Arc<SnapshotIteratorError> },
}

impl Display for FunctionReadError {
    fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl Error for FunctionReadError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::SnapshotGet { source } => Some(source),
            Self::SnapshotIterate { source } => Some(source),
            Self::FunctionNotFound { .. } => None,
        }
    }
}

#[derive(Debug)]
pub enum ProgramDefinitionError {
    FunctionRead { source: FunctionReadError },
    FunctionDefinition { source: FunctionDefinitionError },
    PatternDefinition { source: PatternDefinitionError },
}

impl Display for ProgramDefinitionError {
    fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl Error for ProgramDefinitionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            ProgramDefinitionError::FunctionRead { source } => Some(source),
            ProgramDefinitionError::FunctionDefinition { source } => Some(source),
            ProgramDefinitionError::PatternDefinition { source } => Some(source),
        }
    }
}

#[derive(Debug)]
pub enum FunctionDefinitionError {
    FunctionArgumentUnused { argument_variable: String },
    ReturnVariableUnavailable { variable: String },
    PatternDefinition { source: PatternDefinitionError },
    ParseError { source: typeql::common::error::Error },
}

impl fmt::Display for FunctionDefinitionError {
    fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl Error for FunctionDefinitionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::FunctionArgumentUnused { .. } => None,
            Self::ReturnVariableUnavailable { .. } => None,
            Self::PatternDefinition { source } => Some(source),
            Self::ParseError { source } => Some(source),
        }
    }
}
