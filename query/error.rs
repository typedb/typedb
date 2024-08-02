/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{error::Error, fmt};
use typeql::query::stage::Match;
use compiler::inference::TypeInferenceError;
use ir::PatternDefinitionError;
use ir::program::FunctionDefinitionError;

use crate::define::DefineError;

#[derive(Debug)]
pub enum QueryError {
    ParseError { typeql_query: String, source: typeql::common::Error },
    Define { source: DefineError },
    Pattern { source: PatternDefinitionError },
    PipelineFunctionDefinition { source: FunctionDefinitionError },
    MatchWithFunctionsTypeInferenceFailure { clause: Match, source: TypeInferenceError },
}

impl fmt::Display for QueryError {
    fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl Error for QueryError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            QueryError::ParseError { source, .. } => Some(source),
            QueryError::Define { source, .. } => Some(source),
            QueryError::Pattern { source, .. } => Some(source),
            QueryError::PipelineFunctionDefinition { source, .. } => Some(source),
            QueryError::MatchWithFunctionsTypeInferenceFailure { source, .. } => Some(source),
        }
    }
}
