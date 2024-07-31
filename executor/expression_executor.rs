/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;

use answer::variable::Variable;
use compiler::expression::compiled_expression::CompiledExpression;
use compiler::instruction::expression::binary::{Binary, BinaryExpression, MathRemainderLong};
use compiler::instruction::expression::ExpressionEvaluationError;
use compiler::instruction::expression::list_operations::{ListConstructor, ListIndex, ListIndexRange};
use compiler::instruction::expression::load_cast::{CastBinaryLeft, CastBinaryRight, CastLeftLongToDouble, CastRightLongToDouble, CastUnary, CastUnaryLongToDouble, ImplicitCast, LoadConstant, LoadVariable};
use compiler::instruction::expression::op_codes::ExpressionOpCode;
use compiler::instruction::expression::operators;
use compiler::instruction::expression::unary::{MathAbsDouble, MathAbsLong, MathCeilDouble, MathFloorDouble, MathRoundDouble, Unary, UnaryExpression};
use encoding::value::value::{DBValue, Value};

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ExpressionValue {
    Single(Value<'static>),
    List(Vec<Value<'static>>),
}

struct ExpressionExecutorState<'this> {
    stack: Vec<ExpressionValue>,
    variables: Box<[ExpressionValue]>,
    next_variable_index: usize,
    constants: &'this [Value<'static>],
    next_constant_index: usize,
}

impl<'this> ExpressionExecutorState<'this> {
    fn new(variables: Box<[ExpressionValue]>, constants: &'this [Value<'static>]) -> Self {
        Self { stack: Vec::new(), variables, next_variable_index: 0, constants, next_constant_index: 0 }
    }

    fn push_value(&mut self, value: Value<'static>) {
        self.stack.push(ExpressionValue::Single(value))
    }

    fn push_list(&mut self, value_list: Vec<Value<'static>>) {
        self.stack.push(ExpressionValue::List(value_list))
    }

    fn pop_value(&mut self) -> Value<'static> {
        match self.stack.pop().unwrap() {
            ExpressionValue::Single(value) => value,
            _ => unreachable!(),
        }
    }

    fn pop_list(&mut self) -> Vec<Value<'static>> {
        match self.stack.pop().unwrap() {
            ExpressionValue::List(value_list) => value_list,
            _ => unreachable!(),
        }
    }

    fn next_variable(&mut self) -> ExpressionValue {
        let value = self.variables[self.next_variable_index].clone();
        self.next_variable_index += 1;
        value
    }

    fn next_constant(&mut self) -> Value<'static> {
        let constant = self.constants[self.next_constant_index].clone();
        self.next_constant_index += 1;
        constant
    }
}

pub struct ExpressionExecutor {}

impl ExpressionExecutor {
    pub fn evaluate(
        compiled: CompiledExpression,
        input: HashMap<Variable, ExpressionValue>,
    ) -> Result<ExpressionValue, ExpressionEvaluationError> {
        let mut variables = Vec::new();
        for v in compiled.variables() {
            variables.push(input.get(v).unwrap().clone());
        }

        let mut state = ExpressionExecutorState::new(variables.into_boxed_slice(), compiled.constants());
        for instr in compiled.instructions() {
            evaluate_instruction(instr, &mut state)?;
        }
        Ok(state.stack.pop().unwrap())
    }
}

fn evaluate_instruction(
    op_code: &ExpressionOpCode,
    state: &mut ExpressionExecutorState<'_>,
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

        ExpressionOpCode::OpLongAddLong => operators::OpLongAddLong::evaluate(state),
        ExpressionOpCode::OpLongSubtractLong => operators::OpLongSubtractLong::evaluate(state),
        ExpressionOpCode::OpLongMultiplyLong => operators::OpLongMultiplyLong::evaluate(state),
        ExpressionOpCode::OpLongDivideLong => operators::OpLongDivideLong::evaluate(state),
        ExpressionOpCode::OpLongModuloLong => operators::OpLongModuloLong::evaluate(state),
        ExpressionOpCode::OpLongPowerLong => operators::OpLongPowerLong::evaluate(state),

        ExpressionOpCode::OpDoubleAddDouble => operators::OpDoubleAddDouble::evaluate(state),
        ExpressionOpCode::OpDoubleSubtractDouble => operators::OpDoubleSubtractDouble::evaluate(state),
        ExpressionOpCode::OpDoubleMultiplyDouble => operators::OpDoubleMultiplyDouble::evaluate(state),
        ExpressionOpCode::OpDoubleDivideDouble => operators::OpDoubleDivideDouble::evaluate(state),
        ExpressionOpCode::OpDoubleModuloDouble => operators::OpDoubleModuloDouble::evaluate(state),
        ExpressionOpCode::OpDoublePowerDouble => operators::OpDoublePowerDouble::evaluate(state),

        ExpressionOpCode::MathRemainderLong => MathRemainderLong::evaluate(state),
        ExpressionOpCode::MathRoundDouble => MathRoundDouble::evaluate(state),
        ExpressionOpCode::MathCeilDouble => MathCeilDouble::evaluate(state),
        ExpressionOpCode::MathFloorDouble => MathFloorDouble::evaluate(state),
        ExpressionOpCode::MathAbsLong => MathAbsLong::evaluate(state),
        ExpressionOpCode::MathAbsDouble => MathAbsDouble::evaluate(state),
    }
}


pub trait ExpressionEvaluation {
    fn evaluate<'a>(state: &mut ExpressionExecutorState<'a>) -> Result<(), ExpressionEvaluationError>;
}

impl<T1, T2, R, F> ExpressionEvaluation for Binary<T1, T2, R, F>
    where
        T1: DBValue,
        T2: DBValue,
        R: DBValue,
        F: BinaryExpression<T1, T2, R>,
{
    fn evaluate<'a>(state: &mut ExpressionExecutorState<'a>) -> Result<(), ExpressionEvaluationError> {
        let a2: T2 = T2::from_db_value(state.pop_value()).unwrap();
        let a1: T1 = T1::from_db_value(state.pop_value()).unwrap();
        state.push_value(F::evaluate(a1, a2)?.to_db_value());
        Ok(())
    }
}

impl ExpressionEvaluation for ListConstructor {

    fn evaluate<'a>(state: &mut ExpressionExecutorState<'a>) -> Result<(), ExpressionEvaluationError> {
        let mut n_elements = state.pop_value().unwrap_long() as usize;
        let mut elements: Vec<Value<'static>> = Vec::with_capacity(n_elements);
        for _ in 0..n_elements {
            elements.push(state.pop_value());
        }
        state.push_list(elements);
        Ok(())
    }
}

impl ExpressionEvaluation for ListIndex {

    fn evaluate<'a>(state: &mut ExpressionExecutorState<'a>) -> Result<(), ExpressionEvaluationError> {
        let list = state.pop_list();
        let index = state.pop_value().unwrap_long();
        if let Some(value) = list.get(index as usize) {
            state.push_value(value.clone()); // Should we avoid cloning?
            Ok(())
        } else {
            Err(ExpressionEvaluationError::ListIndexOutOfRange)
        }
    }
}

impl ExpressionEvaluation for ListIndexRange {

    fn evaluate<'a>(state: &mut ExpressionExecutorState<'a>) -> Result<(), ExpressionEvaluationError> {
        let mut list = state.pop_list();
        let to_index = state.pop_value().unwrap_long() as usize;
        let from_index = state.pop_value().unwrap_long() as usize;
        if let Some(sub_slice) = list.get(from_index..to_index) {
            let mut vec = Vec::with_capacity((to_index - from_index + 1) as usize);
            vec.extend_from_slice(sub_slice);
            state.push_list(vec); // TODO: Should we make this more efficient by storing (Vec, range) ?
            Ok(())
        } else {
            Err(ExpressionEvaluationError::ListIndexOutOfRange)
        }
    }
}
impl ExpressionEvaluation for LoadVariable {
    fn evaluate<'a>(state: &mut ExpressionExecutorState<'a>) -> Result<(), ExpressionEvaluationError> {
        match state.next_variable() {
            ExpressionValue::Single(single) => state.push_value(single),
            ExpressionValue::List(list) => state.push_list(list),
        }
        Ok(())
    }
}

impl ExpressionEvaluation for LoadConstant {
    fn evaluate<'a>(state: &mut ExpressionExecutorState<'a>) -> Result<(), ExpressionEvaluationError> {
        let constant = state.next_constant();
        state.push_value(constant);
        Ok(())
    }
}

impl<From: DBValue, To: ImplicitCast<From>> ExpressionEvaluation for CastUnary<From, To> {
    fn evaluate<'a>(state: &mut ExpressionExecutorState<'a>) -> Result<(), ExpressionEvaluationError> {
        let value_before = From::from_db_value(state.pop_value()).unwrap();
        let value_after = To::cast(value_before)?.to_db_value();
        state.push_value(value_after);
        Ok(())
    }
}

impl<From: DBValue, To: ImplicitCast<From>> ExpressionEvaluation for CastBinaryLeft<From, To> {
    fn evaluate<'a>(state: &mut ExpressionExecutorState<'a>) -> Result<(), ExpressionEvaluationError> {
        let right = state.pop_value();
        let left_before = From::from_db_value(state.pop_value()).unwrap();
        let left_after = To::cast(left_before)?.to_db_value();
        state.push_value(left_after);
        state.push_value(right);
        Ok(())
    }
}

impl<From: DBValue, To: ImplicitCast<From>> ExpressionEvaluation for CastBinaryRight<From, To> {
    fn evaluate<'a>(state: &mut ExpressionExecutorState<'a>) -> Result<(), ExpressionEvaluationError> {
        let right_before = From::from_db_value(state.pop_value()).unwrap();
        let right_after = To::cast(right_before)?.to_db_value();
        state.push_value(right_after);
        Ok(())
    }
}

impl<T1, R, F> ExpressionEvaluation for Unary<T1, R, F>
    where
        T1: DBValue,
        R: DBValue,
        F: UnaryExpression<T1, R>,
{
    fn evaluate<'a>(state: &mut ExpressionExecutorState<'a>) -> Result<(), ExpressionEvaluationError> {
        let a1: T1 = T1::from_db_value(state.pop_value()).unwrap();
        state.push_value(F::evaluate(a1)?.to_db_value());
        Ok(())
    }
}
