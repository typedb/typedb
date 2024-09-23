/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{HashMap, HashSet},
    error::Error,
    fmt,
};

use answer::variable::Variable;

#[derive(Debug, Clone)]
pub enum Modifier {
    Select(Select),
    Sort(Sort),
    Offset(Offset),
    Limit(Limit),
}

#[derive(Debug, Clone)]
pub struct Select {
    pub variables: HashSet<Variable>,
}

impl Select {
    // TODO: Bit trange that this takes str
    pub(crate) fn new(variables: HashSet<Variable>) -> Self {
        Self { variables }
    }
}

#[derive(Debug, Clone)]
pub struct Sort {
    pub variables: Vec<SortVariable>,
}

impl Sort {
    pub(crate) fn new(variables: Vec<(Variable, bool)>) -> Self {
        let mut sort_variables = Vec::with_capacity(variables.len());
        for (var, is_ascending) in variables {
            if is_ascending {
                sort_variables.push(SortVariable::Ascending(var.clone()));
            } else {
                sort_variables.push(SortVariable::Descending(var.clone()));
            }
        }
        Self { variables: sort_variables }
    }
}

#[derive(Debug, Copy, Clone)]
pub enum SortVariable {
    Ascending(Variable),
    Descending(Variable),
}

#[derive(Debug, Copy, Clone)]
pub struct Offset {
    offset: u64,
}

impl Offset {
    pub(crate) fn new(offset: u64) -> Self {
        Self { offset }
    }

    pub fn offset(&self) -> u64 {
        self.offset
    }
}

#[derive(Debug, Copy, Clone)]
pub struct Limit {
    limit: u64,
}

impl Limit {
    pub(crate) fn new(limit: u64) -> Self {
        Self { limit }
    }

    pub fn limit(&self) -> u64 {
        self.limit
    }
}

#[derive(Debug, Clone)]
pub enum ModifierDefinitionError {
    SelectedVariableNotAvailable { name: String },
    SortVariableNotAvailable { name: String },
}

impl fmt::Display for ModifierDefinitionError {
    fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl Error for ModifierDefinitionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::SelectedVariableNotAvailable { .. } => None,
            Self::SortVariableNotAvailable { .. } => None,
        }
    }
}
