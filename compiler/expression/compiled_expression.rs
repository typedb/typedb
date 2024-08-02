/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use answer::variable::Variable;
use encoding::value::{value::Value, value_type::ValueTypeCategory};
use ir::pattern::variable_category::VariableCategory;

use crate::instruction::expression::op_codes::ExpressionOpCode;

pub struct CompiledExpression {
    pub(super) instructions: Vec<ExpressionOpCode>,
    pub(super) variables: Vec<Variable>,
    pub(super) constants: Vec<Value<'static>>,
    pub(super) return_type: ExpressionValueType,
}

impl CompiledExpression {
    pub fn instructions(&self) -> &Vec<ExpressionOpCode> {
        &self.instructions
    }

    pub fn variables(&self) -> &[Variable] {
        self.variables.as_slice()
    }

    pub fn constants(&self) -> &[Value<'static>] {
        self.constants.as_slice()
    }

    pub fn return_type(&self) -> ExpressionValueType {
        self.return_type
    }
}

#[derive(Debug, Copy, Clone)]
pub enum ExpressionValueType {
    // TODO: we haven't implemented ConceptList, only ValueList right now.
    // TODO: this should hold an actual ValueType, not a Category!
    Single(ValueTypeCategory),
    List(ValueTypeCategory),
}

impl ExpressionValueType {
    pub fn value_type(&self) -> ValueTypeCategory {
        match self {
            ExpressionValueType::Single(category) => *category,
            ExpressionValueType::List(category) => *category,
        }
    }
}
