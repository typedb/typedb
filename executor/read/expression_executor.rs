/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, hash::Hash, sync::Arc};

use answer::{Thing, variable_value::VariableValue};
use compiler::annotation::expression::{
    compiled_expression::ExecutableExpression,
    instructions::{
        ExpressionEvaluationError,
        binary::{
            Binary, BinaryExpression, MathMaxDecimalDecimal, MathMaxDoubleDouble, MathMaxIntegerInteger,
            MathMinDecimalDecimal, MathMinDoubleDouble, MathMinIntegerInteger, MathRemainderInteger,
        },
        cast::{
            CastBinaryLeft, CastBinaryRight, CastLeftDecimalToDouble, CastLeftIntegerToDecimal,
            CastLeftIntegerToDouble, CastRightDecimalToDouble, CastRightIntegerToDecimal, CastRightIntegerToDouble,
            CastUnary, CastUnaryDecimalToDouble, CastUnaryIntegerToDecimal, CastUnaryIntegerToDouble, ImplicitCast,
        },
        list_operations::{ListConstructor, ListIndex, ListIndexRange},
        load::{LoadConstant, LoadVariable},
        op_codes::ExpressionOpCode,
        operators::{
            OpDateSubtractDate, OpDateTimeAddDuration, OpDateTimeSubtractDate, OpDateTimeSubtractDateTime,
            OpDateTimeSubtractDuration, OpDateTimeTZAddDuration, OpDateTimeTZSubtractDateTimeTZ,
            OpDateTimeTZSubtractDuration, OpDecimalAddDecimal, OpDecimalMultiplyDecimal, OpDecimalSubtractDecimal,
            OpDoubleAddDouble, OpDoubleDivideDouble, OpDoubleModuloDouble, OpDoubleMultiplyDouble, OpDoublePowerDouble,
            OpDoubleSubtractDouble, OpDurationAddDuration, OpDurationSubtractDuration, OpIntegerAddInteger,
            OpIntegerDivideInteger, OpIntegerModuloInteger, OpIntegerMultiplyInteger, OpIntegerPowerInteger,
            OpIntegerSubtractInteger, OpStringAddString,
        },
        unary::{
            LenString, MathAbsDecimal, MathAbsDouble, MathAbsInteger, MathCeilDecimal, MathCeilDouble,
            MathFloorDecimal, MathFloorDouble, MathRoundDecimal, MathRoundDouble, Unary, UnaryExpression,
        },
    },
};
use compiler::for_each_opcode;
use encoding::value::value::{NativeValueConvertible, Value};
use ir::{pattern::ParameterID, pipeline::ParameterRegistry};
use resource::profile::StorageCounters;
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
        storage_counters: StorageCounters,
    ) -> Result<Self, ExpressionEvaluationError> {
        match value {
            VariableValue::Value(value) => Ok(ExpressionValue::Single(value)),
            VariableValue::ValueList(values) => Ok(ExpressionValue::List(values)),
            VariableValue::Thing(Thing::Attribute(attr)) => Ok(ExpressionValue::Single(
                attr.get_value(&**context.snapshot(), context.thing_manager(), storage_counters)
                    .map_err(|source| ExpressionEvaluationError::ConceptRead { typedb_source: source })?
                    .into_owned(),
            )),
            VariableValue::ThingList(things) => {
                let as_value_list = things
                    .iter()
                    .map(|thing| match thing {
                        Thing::Attribute(attr) => Ok(attr
                            .get_value(&**context.snapshot(), context.thing_manager(), storage_counters.clone())
                            .map_err(|source| ExpressionEvaluationError::ConceptRead { typedb_source: source })?
                            .into_owned()),
                        _ => Err(ExpressionEvaluationError::CastFailed {
                            description: "list contains elements without values".to_string(),
                        }),
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(ExpressionValue::List(as_value_list.into()))
            }
            other => Err(ExpressionEvaluationError::CastFailed {
                description: format!("can only get values from {}", other.variant_name()),
            }),
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
        let constant = self.parameter_registry.value_unchecked(&self.constants[self.next_constant_index]).clone();
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
    macro_rules! impl_evaluate_instruction {
        ($($name:ident,)*) => {
            match op_code {
                $(ExpressionOpCode::$name => $name::evaluate(state),)*
            }
        }
    }
    for_each_opcode!(impl_evaluate_instruction)
}


pub trait ExpressionEvaluation {
    fn evaluate(state: &mut ExpressionExecutorState<'_>) -> Result<(), ExpressionEvaluationError>;
}

impl<'a, E: BinaryExpression<'a>> ExpressionEvaluation for Binary<'a, E> {
    fn evaluate(state: &mut ExpressionExecutorState<'_>) -> Result<(), ExpressionEvaluationError> {
        let a2: E::T2 = E::T2::from_db_value(state.pop_value()).unwrap();
        let a1: E::T1 = E::T1::from_db_value(state.pop_value()).unwrap();
        state.push_value(E::evaluate(a1, a2)?.to_db_value());
        Ok(())
    }
}

impl ExpressionEvaluation for ListConstructor {
    fn evaluate(state: &mut ExpressionExecutorState<'_>) -> Result<(), ExpressionEvaluationError> {
        let n_elements = state.pop_value().unwrap_integer() as usize;
        let elements: Arc<[Value<'static>]> = (0..n_elements).map(|_| state.pop_value()).collect();
        state.push_list(elements);
        Ok(())
    }
}

impl ExpressionEvaluation for ListIndex {
    fn evaluate(state: &mut ExpressionExecutorState<'_>) -> Result<(), ExpressionEvaluationError> {
        let list = state.pop_list();
        let index = state.pop_value().unwrap_integer();
        if index >= 0 {
            if let Some(value) = list.get(index as usize) {
                state.push_value(value.clone()); // Should we avoid cloning?
                Ok(())
            } else {
                Err(ExpressionEvaluationError::ListIndexOutOfRange { index, length: list.len() })
            }
        } else {
            Err(ExpressionEvaluationError::ListIndexNegative { index })
        }
    }
}

impl ExpressionEvaluation for ListIndexRange {
    fn evaluate(state: &mut ExpressionExecutorState<'_>) -> Result<(), ExpressionEvaluationError> {
        let list = state.pop_list();
        let to_index = state.pop_value().unwrap_integer();
        let from_index = state.pop_value().unwrap_integer();
        if to_index < 0 {
            return Err(ExpressionEvaluationError::ListIndexNegative { index: to_index });
        } else if from_index < 0 {
            return Err(ExpressionEvaluationError::ListIndexNegative { index: from_index });
        }
        if let Some(sub_slice) = list.get(from_index as usize..to_index as usize) {
            state.push_list(sub_slice.into()); // TODO: Should we make this more efficient by storing (Vec, range) ?
            Ok(())
        } else {
            Err(ExpressionEvaluationError::ListRangeOutOfRange { from_index, to_index, length: list.len() })
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

impl<'a, From: NativeValueConvertible<'a>, To: ImplicitCast<'a, From>> ExpressionEvaluation
    for CastUnary<'a, From, To>
{
    fn evaluate(state: &mut ExpressionExecutorState<'_>) -> Result<(), ExpressionEvaluationError> {
        let value_before = From::from_db_value(state.pop_value()).unwrap();
        let value_after = To::cast(value_before)?.to_db_value();
        state.push_value(value_after);
        Ok(())
    }
}

impl<'a, From: NativeValueConvertible<'a>, To: ImplicitCast<'a, From>> ExpressionEvaluation
    for CastBinaryLeft<'a, From, To>
{
    fn evaluate(state: &mut ExpressionExecutorState<'_>) -> Result<(), ExpressionEvaluationError> {
        let right = state.pop_value();
        let left_before = From::from_db_value(state.pop_value()).unwrap();
        let left_after = To::cast(left_before)?.to_db_value();
        state.push_value(left_after);
        state.push_value(right);
        Ok(())
    }
}

impl<'a, From: NativeValueConvertible<'a>, To: ImplicitCast<'a, From>> ExpressionEvaluation
    for CastBinaryRight<'a, From, To>
{
    fn evaluate(state: &mut ExpressionExecutorState<'_>) -> Result<(), ExpressionEvaluationError> {
        let right_before = From::from_db_value(state.pop_value()).unwrap();
        let right_after = To::cast(right_before)?.to_db_value();
        state.push_value(right_after);
        Ok(())
    }
}

impl<'a, E: UnaryExpression<'a>> ExpressionEvaluation for Unary<'a, E> {
    fn evaluate(state: &mut ExpressionExecutorState<'_>) -> Result<(), ExpressionEvaluationError> {
        let a1: E::T1 = E::T1::from_db_value(state.pop_value()).unwrap();
        state.push_value(E::evaluate(a1)?.to_db_value());
        Ok(())
    }
}
