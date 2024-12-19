/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, sync::Arc};

use answer::variable::Variable;
use encoding::value::value_type::ValueType;
use ir::pattern::IrID;

use crate::{executable::next_executable_id, VariablePosition};

#[derive(Debug, Clone)]
pub struct ReduceExecutable {
    pub executable_id: u64,
    pub reduce_rows_executable: Arc<ReduceRowsExecutable>,
    pub output_row_mapping: HashMap<Variable, VariablePosition>, // output_row = (group_vars, reduce_outputs)
}

impl ReduceExecutable {
    pub(crate) fn new(
        rows_executable: ReduceRowsExecutable,
        output_row_mapping: HashMap<Variable, VariablePosition>,
    ) -> Self {
        Self {
            executable_id: next_executable_id(),
            reduce_rows_executable: Arc::new(rows_executable),
            output_row_mapping,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ReduceRowsExecutable {
    pub reductions: Vec<ReduceInstruction<VariablePosition>>,
    pub input_group_positions: Vec<VariablePosition>,
}

#[derive(Debug, Clone)]
pub enum ReduceInstruction<ID: IrID> {
    Count,
    CountVar(ID),
    SumInteger(ID),
    SumDouble(ID),
    MaxInteger(ID),
    MaxDouble(ID),
    MinInteger(ID),
    MinDouble(ID),
    MeanInteger(ID),
    MeanDouble(ID),
    MedianInteger(ID),
    MedianDouble(ID),
    StdInteger(ID),
    StdDouble(ID),
}

impl<ID: IrID> ReduceInstruction<ID> {
    pub fn id(&self) -> Option<ID> {
        match *self {
            Self::Count => None,

            Self::CountVar(id)
            | Self::SumInteger(id)
            | Self::SumDouble(id)
            | Self::MaxInteger(id)
            | Self::MaxDouble(id)
            | Self::MinInteger(id)
            | Self::MinDouble(id)
            | Self::MeanInteger(id)
            | Self::MeanDouble(id)
            | Self::MedianInteger(id)
            | Self::MedianDouble(id)
            | Self::StdInteger(id)
            | Self::StdDouble(id) => Some(id),
        }
    }

    pub fn output_type(&self) -> ValueType {
        match self {
            Self::Count => ValueType::Integer,
            Self::CountVar(_) => ValueType::Integer,
            Self::SumInteger(_) => ValueType::Integer,
            Self::SumDouble(_) => ValueType::Double,
            Self::MaxInteger(_) => ValueType::Integer,
            Self::MaxDouble(_) => ValueType::Double,
            Self::MinInteger(_) => ValueType::Integer,
            Self::MinDouble(_) => ValueType::Double,
            Self::MeanInteger(_) => ValueType::Double,
            Self::MeanDouble(_) => ValueType::Double,
            Self::MedianInteger(_) => ValueType::Double,
            Self::MedianDouble(_) => ValueType::Double,
            Self::StdInteger(_) => ValueType::Double,
            Self::StdDouble(_) => ValueType::Double,
        }
    }

    pub fn map<T: IrID>(self, mapping: &HashMap<ID, T>) -> ReduceInstruction<T> {
        match self {
            ReduceInstruction::Count => ReduceInstruction::Count,
            ReduceInstruction::CountVar(id) => ReduceInstruction::CountVar(mapping[&id]),
            ReduceInstruction::SumInteger(id) => ReduceInstruction::SumInteger(mapping[&id]),
            ReduceInstruction::SumDouble(id) => ReduceInstruction::SumDouble(mapping[&id]),
            ReduceInstruction::MaxInteger(id) => ReduceInstruction::MaxInteger(mapping[&id]),
            ReduceInstruction::MaxDouble(id) => ReduceInstruction::MaxDouble(mapping[&id]),
            ReduceInstruction::MinInteger(id) => ReduceInstruction::MinInteger(mapping[&id]),
            ReduceInstruction::MinDouble(id) => ReduceInstruction::MinDouble(mapping[&id]),
            ReduceInstruction::MeanInteger(id) => ReduceInstruction::MeanInteger(mapping[&id]),
            ReduceInstruction::MeanDouble(id) => ReduceInstruction::MeanDouble(mapping[&id]),
            ReduceInstruction::MedianInteger(id) => ReduceInstruction::MedianInteger(mapping[&id]),
            ReduceInstruction::MedianDouble(id) => ReduceInstruction::MedianDouble(mapping[&id]),
            ReduceInstruction::StdInteger(id) => ReduceInstruction::StdInteger(mapping[&id]),
            ReduceInstruction::StdDouble(id) => ReduceInstruction::StdDouble(mapping[&id]),
        }
    }
}
