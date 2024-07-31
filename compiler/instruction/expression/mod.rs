/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    error::Error,
    fmt::{Debug, Display, Formatter},
};

pub mod builtins;
pub mod evaluator;
pub mod op_codes;

pub enum ExpressionEvaluationError {
    CheckedOperationFailed,
    CastFailed,
    ListIndexOutOfRange,
}

impl Debug for ExpressionEvaluationError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl Display for ExpressionEvaluationError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl Error for ExpressionEvaluationError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::CheckedOperationFailed => None,
            Self::CastFailed => None,
            Self::ListIndexOutOfRange => None,
        }
    }
}
