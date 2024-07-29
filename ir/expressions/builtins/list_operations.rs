/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use encoding::value::{value::Value, value_type::ValueTypeCategory};

use crate::expressions::{
    evaluator::ExpressionEvaluationState,
    expression_compiler::{ExpressionInstruction, ExpressionTreeCompiler, SelfCompiling},
    op_codes::ExpressionOpCode,
    todo__dissolve__builtins::ValueTypeTrait,
    ExpressionCompilationError, ExpressionEvaluationError,
};

pub struct ListConstructor {}
pub struct ListIndex {}
pub struct ListIndexRange {}

impl ExpressionInstruction for ListConstructor {
    const OP_CODE: ExpressionOpCode = ExpressionOpCode::ListConstructor;

    fn evaluate<'a>(state: &mut ExpressionEvaluationState<'a>) -> Result<(), ExpressionEvaluationError> {
        let mut n_elements = state.pop_value().unwrap_long() as usize;
        let mut elements: Vec<Value<'static>> = Vec::with_capacity(n_elements);
        for _ in 0..n_elements {
            elements.push(state.pop_value());
        }
        state.push_list(elements);
        Ok(())
    }
}

impl ExpressionInstruction for ListIndex {
    const OP_CODE: ExpressionOpCode = ExpressionOpCode::ListIndex;

    fn evaluate<'a>(state: &mut ExpressionEvaluationState<'a>) -> Result<(), ExpressionEvaluationError> {
        let mut list = state.pop_list();
        let index = state.pop_value().unwrap_long();
        if let Some(value) = list.get(index as usize) {
            state.push_value(value.clone()); // Should we avoid cloning?
            Ok(())
        } else {
            Err(ExpressionEvaluationError::ListIndexOutOfRange)
        }
    }
}

impl ExpressionInstruction for ListIndexRange {
    const OP_CODE: ExpressionOpCode = ExpressionOpCode::ListIndexRange;

    fn evaluate<'a>(state: &mut ExpressionEvaluationState<'a>) -> Result<(), ExpressionEvaluationError> {
        let mut list = state.pop_list();
        let to_index = state.pop_value().unwrap_long() as usize;
        let from_index = state.pop_value().unwrap_long() as usize;
        if let Some(sub_slice) = list.get(from_index..to_index) {
            let mut vec = Vec::with_capacity((to_index - from_index + 1) as usize);
            vec.extend_from_slice(sub_slice);
            state.push_list(vec); // TODO: Make this more efficient by storing (Vec, range) ?
            Ok(())
        } else {
            Err(ExpressionEvaluationError::ListIndexOutOfRange)
        }
    }
}
