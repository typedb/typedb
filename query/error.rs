/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{error::Error, fmt};

use compiler::{
    expression::ExpressionCompileError, insert::WriteCompilationError, match_::inference::TypeInferenceError,
};
use ir::{program::FunctionDefinitionError, PatternDefinitionError};

use crate::define::DefineError;

#[derive(Debug)]
pub enum QueryError {
    ParseError { typeql_query: String, source: typeql::common::Error },
    Define { source: DefineError },
    FunctionDefinition { source: FunctionDefinitionError },
    PatternDefinition { source: PatternDefinitionError },
    TypeInference { source: TypeInferenceError },
    WriteCompilation { source: WriteCompilationError },
    ExpressionCompilation { source: ExpressionCompileError },
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
            QueryError::FunctionDefinition { source, .. } => Some(source),
            QueryError::PatternDefinition { source, .. } => Some(source),
            QueryError::TypeInference { source } => Some(source),
            QueryError::WriteCompilation { source } => Some(source),
            QueryError::ExpressionCompilation { source } => Some(source),
        }
    }
}
