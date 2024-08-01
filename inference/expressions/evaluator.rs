/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;

use answer::variable::Variable;
use encoding::value::value::Value;

use crate::expressions::{
    builtins::{
        binary::MathRemainderLong,
        list_operations::{ListConstructor, ListIndex, ListIndexRange},
        load_cast::{CastLeftLongToDouble, CastRightLongToDouble, CastUnaryLongToDouble, LoadConstant, LoadVariable},
        operators as ops,
        unary::{MathAbsDouble, MathAbsLong, MathCeilDouble, MathFloorDouble, MathRoundDouble},
    },
    expression_compiler::{CompiledExpression, ExpressionInstruction},
    op_codes::ExpressionOpCode,
    ExpressionEvaluationError,
};

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ExpressionValue {
    Single(Value<'static>),
    List(Vec<Value<'static>>),
}

pub struct ExpressionEvaluationState<'this> {
    stack: Vec<ExpressionValue>,
    variables: Box<[ExpressionValue]>,
    next_variable_index: usize,
    constants: &'this [Value<'static>],
    next_constant_index: usize,
}

impl<'this> ExpressionEvaluationState<'this> {
    fn new(variables: Box<[ExpressionValue]>, constants: &'this [Value<'static>]) -> Self {
        Self { stack: Vec::new(), variables, next_variable_index: 0, constants, next_constant_index: 0 }
    }

    pub fn push_value(&mut self, value: Value<'static>) {
        self.stack.push(ExpressionValue::Single(value))
    }

    pub fn push_list(&mut self, value_list: Vec<Value<'static>>) {
        self.stack.push(ExpressionValue::List(value_list))
    }

    pub fn pop_value(&mut self) -> Value<'static> {
        match self.stack.pop().unwrap() {
            ExpressionValue::Single(value) => value,
            _ => unreachable!(),
        }
    }

    pub fn pop_list(&mut self) -> Vec<Value<'static>> {
        match self.stack.pop().unwrap() {
            ExpressionValue::List(value_list) => value_list,
            _ => unreachable!(),
        }
    }

    pub fn next_variable(&mut self) -> ExpressionValue {
        let value = self.variables[self.next_variable_index].clone();
        self.next_variable_index += 1;
        value
    }

    pub fn next_constant(&mut self) -> Value<'static> {
        let constant = self.constants[self.next_constant_index].clone();
        self.next_constant_index += 1;
        constant
    }
}

pub struct ExpressionEvaluator {}

impl ExpressionEvaluator {
    pub fn evaluate(
        compiled: CompiledExpression,
        input: HashMap<Variable, ExpressionValue>,
    ) -> Result<ExpressionValue, ExpressionEvaluationError> {
        let mut variables = Vec::new();
        for v in compiled.variables() {
            variables.push(input.get(v).unwrap().clone());
        }

        let mut state = ExpressionEvaluationState::new(variables.into_boxed_slice(), compiled.constants());
        for instr in compiled.instructions() {
            evaluate_instruction(instr, &mut state)?;
        }
        Ok(state.stack.pop().unwrap())
    }
}

fn evaluate_instruction(
    op_code: &ExpressionOpCode,
    state: &mut ExpressionEvaluationState<'_>,
) -> Result<(), ExpressionEvaluationError> {
    match op_code {
        ExpressionOpCode::LoadConstant => LoadConstant::evaluate(state),
        ExpressionOpCode::LoadVariable => LoadVariable::evaluate(state),
        ExpressionOpCode::ListConstructor => ListConstructor::evaluate(state),
        ExpressionOpCode::ListIndex => ListIndex::evaluate(state),
        ExpressionOpCode::ListIndexRange => ListIndexRange::evaluate(state),

        ExpressionOpCode::CastUnaryLongToDouble => CastUnaryLongToDouble::evaluate(state),
        ExpressionOpCode::CastLeftLongToDouble => CastLeftLongToDouble::evaluate(state),
        ExpressionOpCode::CastRightLongToDouble => CastRightLongToDouble::evaluate(state),

        ExpressionOpCode::OpLongAddLong => ops::OpLongAddLong::evaluate(state),
        ExpressionOpCode::OpLongSubtractLong => ops::OpLongSubtractLong::evaluate(state),
        ExpressionOpCode::OpLongMultiplyLong => ops::OpLongMultiplyLong::evaluate(state),
        ExpressionOpCode::OpLongDivideLong => ops::OpLongDivideLong::evaluate(state),
        ExpressionOpCode::OpLongModuloLong => ops::OpLongModuloLong::evaluate(state),
        ExpressionOpCode::OpLongPowerLong => ops::OpLongPowerLong::evaluate(state),

        ExpressionOpCode::OpDoubleAddDouble => ops::OpDoubleAddDouble::evaluate(state),
        ExpressionOpCode::OpDoubleSubtractDouble => ops::OpDoubleSubtractDouble::evaluate(state),
        ExpressionOpCode::OpDoubleMultiplyDouble => ops::OpDoubleMultiplyDouble::evaluate(state),
        ExpressionOpCode::OpDoubleDivideDouble => ops::OpDoubleDivideDouble::evaluate(state),
        ExpressionOpCode::OpDoubleModuloDouble => ops::OpDoubleModuloDouble::evaluate(state),
        ExpressionOpCode::OpDoublePowerDouble => ops::OpDoublePowerDouble::evaluate(state),

        ExpressionOpCode::MathRemainderLong => MathRemainderLong::evaluate(state),
        ExpressionOpCode::MathRoundDouble => MathRoundDouble::evaluate(state),
        ExpressionOpCode::MathCeilDouble => MathCeilDouble::evaluate(state),
        ExpressionOpCode::MathFloorDouble => MathFloorDouble::evaluate(state),
        ExpressionOpCode::MathAbsLong => MathAbsLong::evaluate(state),
        ExpressionOpCode::MathAbsDouble => MathAbsDouble::evaluate(state),
    }
}
