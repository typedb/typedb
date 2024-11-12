/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, hash::Hash, sync::Arc};

use answer::{variable_value::VariableValue, Thing};
use compiler::annotation::expression::{
    compiled_expression::ExecutableExpression,
    instructions::{
        binary::{Binary, BinaryExpression, MathRemainderLong},
        list_operations::{ListConstructor, ListIndex, ListIndexRange},
        load_cast::{
            CastBinaryLeft, CastBinaryRight, CastLeftDecimalToDouble, CastLeftLongToDouble, CastRightDecimalToDouble,
            CastRightLongToDouble, CastUnary, CastUnaryDecimalToDouble, CastUnaryLongToDouble, ImplicitCast,
            LoadConstant, LoadVariable,
        },
        op_codes::ExpressionOpCode,
        operators,
        unary::{MathAbsDouble, MathAbsLong, MathCeilDouble, MathFloorDouble, MathRoundDouble, Unary, UnaryExpression},
        ExpressionEvaluationError,
    },
};
use encoding::value::value::{NativeValueConvertible, Value};
use ir::{pattern::ParameterID, pipeline::ParameterRegistry};
use storage::snapshot::ReadableSnapshot;

use crate::pipeline::stage::ExecutionContext;

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ExpressionValue {
    Single(Value<'static>),
    List(Arc<[Value<'static>]>),
}

impl ExpressionValue {
    pub(crate) fn try_from_value(
        value: VariableValue<'static>,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
    ) -> Result<Self, ExpressionEvaluationError> {
        match value {
            VariableValue::Value(value) => Ok(ExpressionValue::Single(value)),
            VariableValue::ValueList(values) => Ok(ExpressionValue::List(values)),
            VariableValue::Thing(Thing::Attribute(attr)) => Ok(ExpressionValue::Single(
                attr.get_value(&**context.snapshot(), context.thing_manager())
                    .map_err(|_| ExpressionEvaluationError::CastFailed)?
                    .into_owned(),
            )),
            VariableValue::ThingList(_) => todo!(),
            _ => Err(ExpressionEvaluationError::CastFailed),
        }
    }
}

impl From<ExpressionValue> for VariableValue<'static> {
    fn from(value: ExpressionValue) -> Self {
        match value {
            ExpressionValue::Single(value) => VariableValue::Value(value),
            ExpressionValue::List(values) => VariableValue::ValueList(values),
        }
    }
}

pub struct ExpressionExecutorState<'this> {
    stack: Vec<ExpressionValue>,
    variables: Box<[ExpressionValue]>,
    next_variable_index: usize,
    constants: &'this [ParameterID],
    next_constant_index: usize,
    parameter_registry: &'this ParameterRegistry,
}

impl<'this> ExpressionExecutorState<'this> {
    fn new(
        variables: Box<[ExpressionValue]>,
        constants: &'this [ParameterID],
        parameter_registry: &'this ParameterRegistry,
    ) -> Self {
        Self {
            stack: Vec::new(),
            variables,
            next_variable_index: 0,
            constants,
            next_constant_index: 0,
            parameter_registry,
        }
    }

    fn push_value(&mut self, value: Value<'static>) {
        self.stack.push(ExpressionValue::Single(value))
    }

    fn push_list(&mut self, value_list: Arc<[Value<'static>]>) {
        self.stack.push(ExpressionValue::List(value_list))
    }

    fn pop_value(&mut self) -> Value<'static> {
        match self.stack.pop().unwrap() {
            ExpressionValue::Single(value) => value,
            _ => unreachable!(),
        }
    }

    fn pop_list(&mut self) -> Arc<[Value<'static>]> {
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
        let constant = self.parameter_registry.value_unchecked(self.constants[self.next_constant_index]).clone();
        self.next_constant_index += 1;
        constant
    }
}

pub fn evaluate_expression<ID: Hash + Eq>(
    compiled: &ExecutableExpression<ID>,
    input: HashMap<ID, ExpressionValue>,
    parameters: &ParameterRegistry,
) -> Result<ExpressionValue, ExpressionEvaluationError> {
    let mut variables = Vec::new();
    for v in compiled.variables() {
        variables.push(input.get(v).unwrap().clone());
    }

    let mut state = ExpressionExecutorState::new(variables.into_boxed_slice(), compiled.constants(), parameters);
    for instr in compiled.instructions() {
        evaluate_instruction(instr, &mut state)?;
    }
    Ok(state.stack.pop().unwrap())
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

        ExpressionOpCode::CastUnaryDecimalToDouble => CastUnaryDecimalToDouble::evaluate(state),
        ExpressionOpCode::CastLeftDecimalToDouble => CastLeftDecimalToDouble::evaluate(state),
        ExpressionOpCode::CastRightDecimalToDouble => CastRightDecimalToDouble::evaluate(state),

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
    fn evaluate(state: &mut ExpressionExecutorState<'_>) -> Result<(), ExpressionEvaluationError>;
}

impl<T1, T2, R, F> ExpressionEvaluation for Binary<T1, T2, R, F>
where
    T1: NativeValueConvertible,
    T2: NativeValueConvertible,
    R: NativeValueConvertible,
    F: BinaryExpression<T1, T2, R>,
{
    fn evaluate(state: &mut ExpressionExecutorState<'_>) -> Result<(), ExpressionEvaluationError> {
        let a2: T2 = T2::from_db_value(state.pop_value()).unwrap();
        let a1: T1 = T1::from_db_value(state.pop_value()).unwrap();
        state.push_value(F::evaluate(a1, a2)?.to_db_value());
        Ok(())
    }
}

impl ExpressionEvaluation for ListConstructor {
    fn evaluate(state: &mut ExpressionExecutorState<'_>) -> Result<(), ExpressionEvaluationError> {
        let n_elements = state.pop_value().unwrap_long() as usize;
        let elements: Arc<[Value<'static>]> = (0..n_elements).map(|_| state.pop_value()).collect();
        state.push_list(elements);
        Ok(())
    }
}

impl ExpressionEvaluation for ListIndex {
    fn evaluate(state: &mut ExpressionExecutorState<'_>) -> Result<(), ExpressionEvaluationError> {
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
    fn evaluate(state: &mut ExpressionExecutorState<'_>) -> Result<(), ExpressionEvaluationError> {
        let list = state.pop_list();
        let to_index = state.pop_value().unwrap_long() as usize;
        let from_index = state.pop_value().unwrap_long() as usize;
        if let Some(sub_slice) = list.get(from_index..to_index) {
            state.push_list(sub_slice.into()); // TODO: Should we make this more efficient by storing (Vec, range) ?
            Ok(())
        } else {
            Err(ExpressionEvaluationError::ListIndexOutOfRange)
        }
    }
}
impl ExpressionEvaluation for LoadVariable {
    fn evaluate(state: &mut ExpressionExecutorState<'_>) -> Result<(), ExpressionEvaluationError> {
        match state.next_variable() {
            ExpressionValue::Single(single) => state.push_value(single),
            ExpressionValue::List(list) => state.push_list(list),
        }
        Ok(())
    }
}

impl ExpressionEvaluation for LoadConstant {
    fn evaluate(state: &mut ExpressionExecutorState<'_>) -> Result<(), ExpressionEvaluationError> {
        let constant = state.next_constant();
        state.push_value(constant);
        Ok(())
    }
}

impl<From: NativeValueConvertible, To: ImplicitCast<From>> ExpressionEvaluation for CastUnary<From, To> {
    fn evaluate(state: &mut ExpressionExecutorState<'_>) -> Result<(), ExpressionEvaluationError> {
        let value_before = From::from_db_value(state.pop_value()).unwrap();
        let value_after = To::cast(value_before)?.to_db_value();
        state.push_value(value_after);
        Ok(())
    }
}

impl<From: NativeValueConvertible, To: ImplicitCast<From>> ExpressionEvaluation for CastBinaryLeft<From, To> {
    fn evaluate(state: &mut ExpressionExecutorState<'_>) -> Result<(), ExpressionEvaluationError> {
        let right = state.pop_value();
        let left_before = From::from_db_value(state.pop_value()).unwrap();
        let left_after = To::cast(left_before)?.to_db_value();
        state.push_value(left_after);
        state.push_value(right);
        Ok(())
    }
}

impl<From: NativeValueConvertible, To: ImplicitCast<From>> ExpressionEvaluation for CastBinaryRight<From, To> {
    fn evaluate(state: &mut ExpressionExecutorState<'_>) -> Result<(), ExpressionEvaluationError> {
        let right_before = From::from_db_value(state.pop_value()).unwrap();
        let right_after = To::cast(right_before)?.to_db_value();
        state.push_value(right_after);
        Ok(())
    }
}

impl<T1, R, F> ExpressionEvaluation for Unary<T1, R, F>
where
    T1: NativeValueConvertible,
    R: NativeValueConvertible,
    F: UnaryExpression<T1, R>,
{
    fn evaluate(state: &mut ExpressionExecutorState<'_>) -> Result<(), ExpressionEvaluationError> {
        let a1: T1 = T1::from_db_value(state.pop_value()).unwrap();
        state.push_value(F::evaluate(a1)?.to_db_value());
        Ok(())
    }
}
