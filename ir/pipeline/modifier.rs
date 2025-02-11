/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashSet, mem};

use answer::variable::Variable;
use structural_equality::StructuralEquality;
use typeql::common::Span;

#[derive(Debug, Clone)]
pub enum Operator {
    Select(Select),
    Sort(Sort),
    Offset(Offset),
    Limit(Limit),
    Require(Require),
}

#[derive(Debug, Clone)]
pub struct Select {
    pub variables: HashSet<Variable>,
    source_span: Option<Span>,
}

impl Select {
    pub(crate) fn new(variables: HashSet<Variable>, source_span: Option<Span>) -> Self {
        Self { variables, source_span }
    }

    pub fn source_span(&self) -> Option<Span> {
        self.source_span
    }
}

impl StructuralEquality for Select {
    fn hash(&self) -> u64 {
        self.variables.hash()
    }

    fn equals(&self, other: &Self) -> bool {
        self.variables.equals(&other.variables)
    }
}

#[derive(Debug, Clone)]
pub struct Sort {
    pub variables: Vec<SortVariable>,
    source_span: Option<Span>,
}

impl Sort {
    pub(crate) fn new(variables: Vec<(Variable, bool)>, source_span: Option<Span>) -> Self {
        let mut sort_variables = Vec::with_capacity(variables.len());
        for (var, is_ascending) in variables {
            if is_ascending {
                sort_variables.push(SortVariable::Ascending(var));
            } else {
                sort_variables.push(SortVariable::Descending(var));
            }
        }
        Self { variables: sort_variables, source_span }
    }

    pub fn source_span(&self) -> Option<Span> {
        self.source_span
    }
}

impl StructuralEquality for Sort {
    fn hash(&self) -> u64 {
        self.variables.hash()
    }

    fn equals(&self, other: &Self) -> bool {
        self.variables.equals(&other.variables)
    }
}

#[derive(Copy, Clone, Debug)]
pub enum SortVariable {
    Ascending(Variable),
    Descending(Variable),
}

impl SortVariable {
    pub fn variable(&self) -> Variable {
        match *self {
            SortVariable::Ascending(var) => var,
            SortVariable::Descending(var) => var,
        }
    }
}

impl StructuralEquality for SortVariable {
    fn hash(&self) -> u64 {
        mem::discriminant(self).hash() ^ self.variable().hash()
    }

    fn equals(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Ascending(var), Self::Ascending(other_var)) => var == other_var,
            (Self::Descending(var), Self::Descending(other_var)) => var == other_var,
            // note: this style forces updating the match when the variants change
            (Self::Ascending { .. }, _) | (Self::Descending { .. }, _) => false,
        }
    }
}

#[derive(Debug, Copy, Clone)]
pub struct Offset {
    offset: u64,
    source_span: Option<Span>,
}

impl Offset {
    pub(crate) fn new(offset: u64, source_span: Option<Span>) -> Self {
        Self { offset, source_span }
    }

    pub fn offset(&self) -> u64 {
        self.offset
    }

    pub fn source_span(&self) -> Option<Span> {
        self.source_span
    }
}

impl StructuralEquality for Offset {
    fn hash(&self) -> u64 {
        self.offset.hash()
    }

    fn equals(&self, other: &Self) -> bool {
        self.offset.equals(&other.offset)
    }
}

#[derive(Debug, Copy, Clone)]
pub struct Limit {
    limit: u64,
    source_span: Option<Span>,
}

impl Limit {
    pub(crate) fn new(limit: u64, source_span: Option<Span>) -> Self {
        Self { limit, source_span }
    }

    pub fn limit(&self) -> u64 {
        self.limit
    }

    pub fn source_span(&self) -> Option<Span> {
        self.source_span
    }
}

impl StructuralEquality for Limit {
    fn hash(&self) -> u64 {
        self.limit.hash()
    }

    fn equals(&self, other: &Self) -> bool {
        self.limit.equals(&other.limit)
    }
}

#[derive(Debug, Clone)]
pub struct Require {
    pub variables: HashSet<Variable>,
    source_span: Option<Span>,
}

impl Require {
    pub(crate) fn new(variables: HashSet<Variable>, source_span: Option<Span>) -> Self {
        Self { variables, source_span }
    }

    pub fn source_span(&self) -> Option<Span> {
        self.source_span
    }
}

impl StructuralEquality for Require {
    fn hash(&self) -> u64 {
        self.variables.hash()
    }

    fn equals(&self, other: &Self) -> bool {
        self.variables.equals(&other.variables)
    }
}

#[derive(Debug, Clone)]
pub struct Distinct;

impl StructuralEquality for Distinct {
    fn hash(&self) -> u64 {
        0
    }

    fn equals(&self, other: &Self) -> bool {
        true
    }
}
