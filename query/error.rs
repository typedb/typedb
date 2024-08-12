/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{error::Error, fmt};

use compiler::{inference::TypeInferenceError, CompileError};
use concept::error::ConceptReadError;
use function::FunctionError;
use ir::{program::FunctionDefinitionError, PatternDefinitionError};
use typeql::query::stage::Match;

use crate::define::DefineError;

#[derive(Debug)]
pub enum QueryError {
    ParseError { typeql_query: String, source: typeql::common::Error },
    ReadError { source: ConceptReadError },
    Define { source: DefineError },
    Pattern { source: PatternDefinitionError },
    Function { source: FunctionError },
    PipelineFunctionDefinition { source: FunctionDefinitionError },
    MatchWithFunctionsTypeInferenceFailure { clause: Match, source: TypeInferenceError },
    CompileError { source: CompileError },
}

impl fmt::Display for QueryError {
    fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl Error for QueryError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::ParseError { source, .. } => Some(source),
            Self::ReadError { source, .. } => Some(source),
            Self::Define { source, .. } => Some(source),
            Self::Pattern { source, .. } => Some(source),
            Self::Function { source, .. } => Some(source),
            Self::PipelineFunctionDefinition { source, .. } => Some(source),
            Self::MatchWithFunctionsTypeInferenceFailure { source, .. } => Some(source),
            Self::CompileError { source } => Some(source),
        }
    }
}
