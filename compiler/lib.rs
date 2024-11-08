/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#![deny(elided_lifetimes_in_paths)]
#![allow(clippy::result_large_err)]

use std::fmt;
use std::fmt::Formatter;

use answer::variable::Variable;
use ir::pattern::IrID;

pub mod annotation;
pub mod executable;
mod optimisation;

macro_rules! filter_variants {
    ($variant:path : $iterable:expr) => {
        $iterable.iter().filter_map(|item| if let $variant(inner) = item { Some(inner) } else { None })
    };
}
pub(crate) use filter_variants;

#[derive(Copy, Clone, Eq, PartialEq, Hash, PartialOrd, Ord)]
pub enum ExecutorVariable {
    RowPosition(VariablePosition),
    Internal(Variable),
}

impl ExecutorVariable {
    pub fn new_position(position: u32) -> Self {
        Self::RowPosition(VariablePosition { position })
    }

    pub fn new_internal(variable: Variable) -> Self {
        Self::Internal(variable)
    }

    /// Returns `true` if the executor variable is [`Output`].
    ///
    /// [`Output`]: ExecutorVariable::Output
    #[must_use]
    pub fn is_output(&self) -> bool {
        matches!(self, Self::RowPosition(..))
    }

    /// Returns `true` if the executor variable is [`Internal`].
    ///
    /// [`Internal`]: ExecutorVariable::Internal
    #[must_use]
    pub fn is_internal(&self) -> bool {
        matches!(self, Self::Internal(..))
    }

    pub fn as_position(&self) -> Option<VariablePosition> {
        match *self {
            Self::RowPosition(v) => Some(v),
            _ => None,
        }
    }

    pub fn as_internal(&self) -> Option<Variable> {
        match *self {
            Self::Internal(v) => Some(v),
            _ => None,
        }
    }
}

impl fmt::Debug for ExecutorVariable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExecutorVariable::RowPosition(position) => write!(f, "{position}"),
            ExecutorVariable::Internal(var) => write!(f, "__{var}__")
        }
    }
}

impl fmt::Display for ExecutorVariable {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

impl IrID for ExecutorVariable {}

#[derive(Copy, Clone, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct VariablePosition {
    position: u32,
}

impl VariablePosition {
    pub fn new(position: u32) -> Self {
        VariablePosition { position }
    }

    pub fn as_usize(&self) -> usize {
        self.position as usize
    }
}

impl fmt::Debug for VariablePosition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "p{}", self.position)
    }
}

impl fmt::Display for VariablePosition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

impl IrID for VariablePosition {}
