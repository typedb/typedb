/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;

use answer::variable::Variable;
use encoding::value::value_type::ValueTypeCategory;
use ir::pattern::IrID;

use crate::VariablePosition;

pub struct ReduceProgram {
    pub reductions: Vec<ReduceInstruction<VariablePosition>>,
    pub input_group_positions: Vec<VariablePosition>,
    pub output_row_mapping: HashMap<Variable, VariablePosition>, // output_row = (group_vars, reduce_outputs)
}

#[derive(Debug, Clone)]
pub enum ReduceInstruction<ID: IrID> {
    Count,
    CountVar(ID),
    SumLong(ID),
    SumDouble(ID),
    MaxLong(ID),
    MaxDouble(ID),
    MinLong(ID),
    MinDouble(ID),
    MeanLong(ID),
    MeanDouble(ID),
    MedianLong(ID),
    MedianDouble(ID),
    StdLong(ID),
    StdDouble(ID),
}

impl<ID: IrID> ReduceInstruction<ID> {
    pub fn output_type(&self) -> ValueTypeCategory {
        match self {
            Self::Count => ValueTypeCategory::Long,
            Self::CountVar(_) => ValueTypeCategory::Long,
            Self::SumLong(_) => ValueTypeCategory::Long,
            Self::SumDouble(_) => ValueTypeCategory::Double,
            Self::MaxLong(_) => ValueTypeCategory::Long,
            Self::MaxDouble(_) => ValueTypeCategory::Double,
            Self::MinLong(_) => ValueTypeCategory::Long,
            Self::MinDouble(_) => ValueTypeCategory::Double,
            Self::MeanLong(_) => ValueTypeCategory::Double,
            Self::MeanDouble(_) => ValueTypeCategory::Double,
            Self::MedianLong(_) => ValueTypeCategory::Double,
            Self::MedianDouble(_) => ValueTypeCategory::Double,
            Self::StdLong(_) => ValueTypeCategory::Double,
            Self::StdDouble(_) => ValueTypeCategory::Double,
        }
    }
}
