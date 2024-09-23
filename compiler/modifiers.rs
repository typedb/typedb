/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::{HashMap, HashSet};

use answer::variable::Variable;
use ir::program::function::Reducer;
use ir::program::modifier::SortVariable;
use ir::program::reduce::Reduce;

use crate::VariablePosition;

pub struct SelectProgram {
    pub retained_positions: HashSet<VariablePosition>,
    pub output_row_mapping: HashMap<Variable, VariablePosition>,
}

pub struct SortProgram {
    pub sort_on: Vec<SortVariable>,
    pub output_row_mapping: HashMap<Variable, VariablePosition>,
}

pub struct OffsetProgram {
    pub offset: u64,
    pub output_row_mapping: HashMap<Variable, VariablePosition>,
}

pub struct LimitProgram {
    pub limit: u64,
    pub output_row_mapping: HashMap<Variable, VariablePosition>,
}

// TODO: Maybe move to its own file
pub struct ReduceProgram {
    pub reduction_inputs: Vec<Reducer<VariablePosition>>,
    pub input_group_positions: Vec<VariablePosition>,
    pub output_row_mapping: HashMap<Variable, VariablePosition>, // output_row = (group_vars, reduce_outputs)
}
