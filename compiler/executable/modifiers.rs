/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::{HashMap, HashSet};

use answer::variable::Variable;
use ir::pipeline::modifier::SortVariable;

use crate::VariablePosition;

#[derive(Debug)]
pub struct SelectExecutable {
    pub retained_positions: HashSet<VariablePosition>,
    pub output_row_mapping: HashMap<Variable, VariablePosition>,
}

#[derive(Debug)]
pub struct SortExecutable {
    pub sort_on: Vec<SortVariable>,
    pub output_row_mapping: HashMap<Variable, VariablePosition>,
}

#[derive(Debug)]
pub struct OffsetExecutable {
    pub offset: u64,
    pub output_row_mapping: HashMap<Variable, VariablePosition>,
}

#[derive(Debug)]
pub struct LimitExecutable {
    pub limit: u64,
    pub output_row_mapping: HashMap<Variable, VariablePosition>,
}

#[derive(Debug)]
pub struct RequireExecutable {
    pub required: HashSet<VariablePosition>,
    pub output_row_mapping: HashMap<Variable, VariablePosition>,
}
