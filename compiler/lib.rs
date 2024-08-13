/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{error::Error, fmt};

use crate::{expression::ExpressionCompileError, inference::TypeInferenceError};

pub mod compiler;
pub mod expression;
pub mod inference;
pub mod instruction;
mod optimisation;
pub mod planner;
pub mod write;

#[derive(Debug)]
pub enum CompileError {
    ProgramTypeInference { source: TypeInferenceError },
    ExpressionCompile { source: ExpressionCompileError },
}

impl fmt::Display for CompileError {
    fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl Error for CompileError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            CompileError::ProgramTypeInference { source, .. } => Some(source),
            CompileError::ExpressionCompile { source, .. } => Some(source),
        }
    }
}
