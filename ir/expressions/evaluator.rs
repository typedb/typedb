/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::HashMap,
    error::Error,
    fmt::{Debug, Display, Formatter},
};

use answer::variable::Variable;
use encoding::value::value::Value;

use crate::{
    expressions::{
        builtins::{
            binary::MathRemainderLong,
            load_cast::{
                CastLeftLongToDouble, CastRightLongToDouble, CastUnaryLongToDouble, LoadConstant, LoadVariable,
            },
            operators as ops,
            unary::{MathCeilDouble, MathFloorDouble, MathRoundDouble},
        },
        expression_compiler::{CompiledExpressionTree, ExpressionInstruction},
        op_codes::ExpressionOpCode,
        ExpressionEvaluationError,
    },
    program::ProgramDefinitionError,
};

pub(crate) struct ExpressionEvaluationState<'constants> {
    constants: &'constants [Value<'static>],
    variable_stack: Vec<Value<'static>>, // TODO: This can be Vec<Position> and we read from the row.

    stack: Vec<Value<'static>>,
    next_constant_index: usize,
}

impl<'constants> ExpressionEvaluationState<'constants> {
    fn new(variable_stack: Vec<Value<'static>>, constants: &'constants [Value<'static>]) -> Self {
        Self { constants, variable_stack, stack: Vec::new(), next_constant_index: 0 }
    }

    pub fn push(&mut self, value: Value<'static>) {
        self.stack.push(value)
    }

    pub fn pop(&mut self) -> Value<'static> {
        self.stack.pop().unwrap()
    }

    pub fn next_variable(&mut self) -> Value<'static> {
        self.variable_stack.pop().unwrap()
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
        compiled: CompiledExpressionTree,
        input: HashMap<Variable, Value<'static>>,
    ) -> Result<Value<'static>, ExpressionEvaluationError> {
        let mut variable_stack = Vec::new();
        for v in compiled.variables() {
            variable_stack.push(input.get(v).unwrap().clone());
        }

        let mut state = ExpressionEvaluationState::new(variable_stack, compiled.constants());
        for instr in compiled.instructions() {
            evaluate_instruction(instr, &mut state)?;
        }
        Ok(state.stack.pop().unwrap())
    }
}

// Really huge switch
fn evaluate_instruction(
    op_code: &ExpressionOpCode,
    state: &mut ExpressionEvaluationState<'_>,
) -> Result<(), ExpressionEvaluationError> {
    match op_code {
        ExpressionOpCode::LoadConstant => LoadConstant::evaluate(state),
        ExpressionOpCode::LoadVariable => LoadVariable::evaluate(state),
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
        ExpressionOpCode::MathAbsLong => todo!(),
        ExpressionOpCode::MathAbsDouble => todo!(),
    }
}
