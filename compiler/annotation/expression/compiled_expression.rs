/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, fmt};

use encoding::value::value_type::ValueType;
use ir::pattern::{IrID, ParameterID};

use crate::annotation::expression::instructions::op_codes::ExpressionOpCode;

#[derive(Debug, Clone)]
pub struct ExecutableExpression<ID> {
    pub(crate) instructions: Vec<ExpressionOpCode>,
    pub(crate) variables: Vec<ID>,
    pub(crate) constants: Vec<ParameterID>,
    pub(crate) return_type: ExpressionValueType,
}

impl<ID> ExecutableExpression<ID> {
    pub fn instructions(&self) -> &[ExpressionOpCode] {
        &self.instructions
    }

    pub fn variables(&self) -> &[ID] {
        self.variables.as_slice()
    }

    pub fn constants(&self) -> &[ParameterID] {
        self.constants.as_slice()
    }

    pub fn return_type(&self) -> &ExpressionValueType {
        &self.return_type
    }
}

impl<ID: IrID> ExecutableExpression<ID> {
    pub fn map<T: IrID>(self, mapping: &HashMap<ID, T>) -> ExecutableExpression<T> {
        let Self { instructions, variables, constants, return_type } = self;
        ExecutableExpression {
            instructions,
            variables: variables.into_iter().map(|var| mapping[&var]).collect(),
            constants,
            return_type,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
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

impl fmt::Display for ExpressionValueType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExpressionValueType::Single(single) => write!(f, "{}", single),
            ExpressionValueType::List(list) => write!(f, "{}[]", list),
        }
    }
}
