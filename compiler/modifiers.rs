/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    sync::Arc,
};

use answer::{variable::Variable, Type};
use encoding::value::value_type::ValueTypeCategory;
use ir::{
    pattern::IrID,
    program::{function::Reducer, modifier::SortVariable, reduce::Reduce},
};

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
