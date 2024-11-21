/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::{HashMap, HashSet};

use answer::variable::Variable;
use ir::pipeline::modifier::SortVariable;
use crate::executable::next_executable_id;

use crate::VariablePosition;

#[derive(Debug)]
pub struct SelectExecutable {
    pub executable_id: u64,
    pub retained_positions: HashSet<VariablePosition>,
    pub output_row_mapping: HashMap<Variable, VariablePosition>,
}

impl SelectExecutable {
    pub(crate) fn new(retained_positions: HashSet<VariablePosition>, output_row_mapping: HashMap<Variable, VariablePosition>) -> Self {
        Self {
            executable_id: next_executable_id(),
            retained_positions,
            output_row_mapping,
        }
    }
}

#[derive(Debug)]
pub struct SortExecutable {
    pub executable_id: u64,
    pub sort_on: Vec<SortVariable>,
    pub output_row_mapping: HashMap<Variable, VariablePosition>,
}

impl SortExecutable {
    pub(crate) fn new(sort_on: Vec<SortVariable>, output_row_mapping: HashMap<Variable, VariablePosition>) -> Self {
        Self {
            executable_id: next_executable_id(),
            sort_on,
            output_row_mapping,
        }
    }
}

#[derive(Debug)]
pub struct OffsetExecutable {
    pub executable_id: u64,
    pub offset: u64,
    pub output_row_mapping: HashMap<Variable, VariablePosition>,
}

impl OffsetExecutable {
    pub(crate) fn new(offset: u64, output_row_mapping: HashMap<Variable, VariablePosition>) -> Self {
        Self {
            executable_id: next_executable_id(),
            offset,
            output_row_mapping,
        }
    }
}

#[derive(Debug)]
pub struct LimitExecutable {
    pub executable_id: u64,
    pub limit: u64,
    pub output_row_mapping: HashMap<Variable, VariablePosition>,
}

impl LimitExecutable {
    pub(crate) fn new(limit: u64, output_row_mapping: HashMap<Variable, VariablePosition>) -> Self {
        Self {
            executable_id: next_executable_id(),
            limit,
            output_row_mapping,
        }
    }
}

#[derive(Debug)]
pub struct RequireExecutable {
    pub executable_id: u64,
    pub required: HashSet<VariablePosition>,
    pub output_row_mapping: HashMap<Variable, VariablePosition>,
}

impl RequireExecutable {
    pub(crate) fn new(required: HashSet<VariablePosition>, output_row_mapping: HashMap<Variable, VariablePosition>) -> Self {
        Self {
            executable_id: next_executable_id(),
            required,
            output_row_mapping,
        }
    }
}
