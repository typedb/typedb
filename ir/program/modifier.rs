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
    Filter(Filter),
    Sort(Sort),
    Offset(Offset),
    Limit(Limit),
}

#[derive(Debug, Clone)]
pub struct Filter {
    pub variables: HashSet<Variable>,
}

impl Filter {
    pub(crate) fn new(
        variables: Vec<&str>,
        variable_index: &HashMap<String, Variable>,
    ) -> Result<Self, ModifierDefinitionError> {
        use ModifierDefinitionError::FilterVariableNotAvailable;
        let mut filter_variables = HashSet::with_capacity(variables.len());
        for name in variables {
            match variable_index.get(name) {
                None => Err(FilterVariableNotAvailable { name: name.to_string() })?,
                Some(var) => filter_variables.insert(var.clone()),
            };
        }
        Ok(Self { variables: filter_variables })
    }
}

#[derive(Debug, Clone)]
pub struct Sort {
    pub variables: Vec<SortVariable>,
}

impl Sort {
    pub(crate) fn new(
        variables: Vec<(&str, bool)>,
        variable_lookup: &HashMap<String, Variable>
    ) -> Result<Self, ModifierDefinitionError> {
        use ModifierDefinitionError::SortVariableNotAvailable;
        let mut sort_variables = Vec::with_capacity(variables.len());
        for (name, is_ascending) in variables {
            match variable_lookup(name) {
                None => Err(SortVariableNotAvailable { name: name.to_string() })?,
                Some(var) => {
                    if is_ascending {
                        sort_variables.push(SortVariable::Ascending(var));
                    } else {
                        sort_variables.push(SortVariable::Descending(var));
                    }
                }
            };
        }
        Ok(Self { variables: sort_variables })
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
    FilterVariableNotAvailable { name: String },
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
            Self::FilterVariableNotAvailable { .. } => None,
            Self::SortVariableNotAvailable { .. } => None,
        }
    }
}
