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

#[rustfmt::skip]
#[derive(Debug, Clone)]
pub enum ReduceInstruction<ID: IrID> {
    Count, CountVar(ID),
    SumInteger(ID), MaxInteger(ID), MinInteger(ID), MeanInteger(ID), MedianInteger(ID), StdInteger(ID),
    SumDouble(ID), MaxDouble(ID), MinDouble(ID), MeanDouble(ID), MedianDouble(ID), StdDouble(ID),
    SumDecimal(ID), MaxDecimal(ID), MinDecimal(ID), MeanDecimal(ID), MedianDecimal(ID), StdDecimal(ID),
    MaxString(ID), MinString(ID),
    MaxDate(ID), MinDate(ID), // MeanDate(ID), MedianDate(ID), StdDate(ID),
    MaxDateTime(ID), MinDateTime(ID), // MeanDateTime(ID), MedianDateTime(ID), StdDateTime(ID),
    MaxDateTimeTZ(ID), MinDateTimeTZ(ID), // MeanDateTimeTZ(ID), MedianDateTimeTZ(ID), StdDateTimeTZ(ID),
}

impl<ID: IrID> ReduceInstruction<ID> {
    pub fn id(&self) -> Option<ID> {
        match *self {
            Self::Count => None,

            | ReduceInstruction::CountVar(id)
            | ReduceInstruction::SumInteger(id)
            | ReduceInstruction::MaxInteger(id)
            | ReduceInstruction::MinInteger(id)
            | ReduceInstruction::MeanInteger(id)
            | ReduceInstruction::MedianInteger(id)
            | ReduceInstruction::StdInteger(id)
            | ReduceInstruction::SumDouble(id)
            | ReduceInstruction::MaxDouble(id)
            | ReduceInstruction::MinDouble(id)
            | ReduceInstruction::MeanDouble(id)
            | ReduceInstruction::MedianDouble(id)
            | ReduceInstruction::StdDouble(id)
            | ReduceInstruction::SumDecimal(id)
            | ReduceInstruction::MaxDecimal(id)
            | ReduceInstruction::MinDecimal(id)
            | ReduceInstruction::MeanDecimal(id)
            | ReduceInstruction::MedianDecimal(id)
            | ReduceInstruction::StdDecimal(id)
            | ReduceInstruction::MaxString(id)
            | ReduceInstruction::MinString(id)
            | ReduceInstruction::MaxDate(id)
            | ReduceInstruction::MinDate(id)
            | ReduceInstruction::MaxDateTime(id)
            | ReduceInstruction::MinDateTime(id)
            | ReduceInstruction::MaxDateTimeTZ(id)
            | ReduceInstruction::MinDateTimeTZ(id) => Some(id),
        }
    }

    pub fn output_type(&self) -> ValueType {
        match self {
            Self::Count => ValueType::Integer,
            Self::CountVar(_) => ValueType::Integer,

            Self::SumInteger(_) => ValueType::Integer,
            Self::MaxInteger(_) => ValueType::Integer,
            Self::MinInteger(_) => ValueType::Integer,
            Self::MeanInteger(_) => ValueType::Double,
            Self::MedianInteger(_) => ValueType::Double,
            Self::StdInteger(_) => ValueType::Double,

            Self::SumDouble(_) => ValueType::Double,
            Self::MaxDouble(_) => ValueType::Double,
            Self::MinDouble(_) => ValueType::Double,
            Self::MeanDouble(_) => ValueType::Double,
            Self::MedianDouble(_) => ValueType::Double,
            Self::StdDouble(_) => ValueType::Double,

            Self::SumDecimal(_) => ValueType::Decimal,
            Self::MaxDecimal(_) => ValueType::Decimal,
            Self::MinDecimal(_) => ValueType::Decimal,
            Self::MeanDecimal(_) => ValueType::Decimal,
            Self::MedianDecimal(_) => ValueType::Decimal,
            Self::StdDecimal(_) => ValueType::Double,

            Self::MaxString(_) => ValueType::String,
            Self::MinString(_) => ValueType::String,

            Self::MaxDate(_) => ValueType::Date,
            Self::MinDate(_) => ValueType::Date,

            Self::MaxDateTime(_) => ValueType::DateTime,
            Self::MinDateTime(_) => ValueType::DateTime,

            Self::MaxDateTimeTZ(_) => ValueType::DateTimeTZ,
            Self::MinDateTimeTZ(_) => ValueType::DateTimeTZ,
        }
    }

    pub fn map<T: IrID>(self, mapping: &HashMap<ID, T>) -> ReduceInstruction<T> {
        match self {
            ReduceInstruction::Count => ReduceInstruction::Count,
            ReduceInstruction::CountVar(id) => ReduceInstruction::CountVar(mapping[&id]),
            ReduceInstruction::SumInteger(id) => ReduceInstruction::SumInteger(mapping[&id]),
            ReduceInstruction::MaxInteger(id) => ReduceInstruction::MaxInteger(mapping[&id]),
            ReduceInstruction::MinInteger(id) => ReduceInstruction::MinInteger(mapping[&id]),
            ReduceInstruction::MeanInteger(id) => ReduceInstruction::MeanInteger(mapping[&id]),
            ReduceInstruction::MedianInteger(id) => ReduceInstruction::MedianInteger(mapping[&id]),
            ReduceInstruction::StdInteger(id) => ReduceInstruction::StdInteger(mapping[&id]),
            ReduceInstruction::SumDouble(id) => ReduceInstruction::SumDouble(mapping[&id]),
            ReduceInstruction::MaxDouble(id) => ReduceInstruction::MaxDouble(mapping[&id]),
            ReduceInstruction::MinDouble(id) => ReduceInstruction::MinDouble(mapping[&id]),
            ReduceInstruction::MeanDouble(id) => ReduceInstruction::MeanDouble(mapping[&id]),
            ReduceInstruction::MedianDouble(id) => ReduceInstruction::MedianDouble(mapping[&id]),
            ReduceInstruction::StdDouble(id) => ReduceInstruction::StdDouble(mapping[&id]),
            ReduceInstruction::SumDecimal(id) => ReduceInstruction::SumDecimal(mapping[&id]),
            ReduceInstruction::MaxDecimal(id) => ReduceInstruction::MaxDecimal(mapping[&id]),
            ReduceInstruction::MinDecimal(id) => ReduceInstruction::MinDecimal(mapping[&id]),
            ReduceInstruction::MeanDecimal(id) => ReduceInstruction::MeanDecimal(mapping[&id]),
            ReduceInstruction::MedianDecimal(id) => ReduceInstruction::MedianDecimal(mapping[&id]),
            ReduceInstruction::StdDecimal(id) => ReduceInstruction::StdDecimal(mapping[&id]),
            ReduceInstruction::MaxString(id) => ReduceInstruction::MaxString(mapping[&id]),
            ReduceInstruction::MinString(id) => ReduceInstruction::MinString(mapping[&id]),
            ReduceInstruction::MaxDate(id) => ReduceInstruction::MaxDate(mapping[&id]),
            ReduceInstruction::MinDate(id) => ReduceInstruction::MinDate(mapping[&id]),
            ReduceInstruction::MaxDateTime(id) => ReduceInstruction::MaxDateTime(mapping[&id]),
            ReduceInstruction::MinDateTime(id) => ReduceInstruction::MinDateTime(mapping[&id]),
            ReduceInstruction::MaxDateTimeTZ(id) => ReduceInstruction::MaxDateTimeTZ(mapping[&id]),
            ReduceInstruction::MinDateTimeTZ(id) => ReduceInstruction::MinDateTimeTZ(mapping[&id]),
        }
    }
}
