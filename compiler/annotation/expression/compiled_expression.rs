/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use answer::variable::Variable;
use encoding::value::value_type::{ValueType, ValueTypeCategory};
use ir::pattern::ParameterID;

use crate::annotation::expression::instructions::op_codes::ExpressionOpCode;

#[derive(Debug, Clone)]
pub struct ExecutableExpression {
    pub(crate) instructions: Vec<ExpressionOpCode>,
    pub(crate) variables: Vec<Variable>,
    pub(crate) constants: Vec<ParameterID>,
    pub(crate) return_type: ExpressionValueType,
}

impl ExecutableExpression {
    pub fn instructions(&self) -> &[ExpressionOpCode] {
        &self.instructions
    }

    pub fn variables(&self) -> &[Variable] {
        self.variables.as_slice()
    }

    pub fn constants(&self) -> &[ParameterID] {
        self.constants.as_slice()
    }

    pub fn return_type(&self) -> &ExpressionValueType {
        &self.return_type
    }
}

#[derive(Debug, Clone)]
pub enum ExpressionValueType {
    // TODO: we haven't implemented ConceptList, only ValueList right now.
    // TODO: this should hold an actual ValueType, not a Category!
    Single(ValueType),
    List(ValueType),
}

impl ExpressionValueType {
    pub fn value_type(&self) -> &ValueType {
        match self {
            ExpressionValueType::Single(value_type) => value_type,
            ExpressionValueType::List(value_type) => value_type,
        }
    }
}
