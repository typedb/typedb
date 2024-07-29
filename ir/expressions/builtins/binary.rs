/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{marker::PhantomData, ops::Rem};

use encoding::value::value_type::ValueTypeCategory;

use crate::expressions::{
    evaluator::ExpressionEvaluationState,
    expression_compiler::{ExpressionInstruction, ExpressionTreeCompiler, SelfCompiling},
    op_codes::ExpressionOpCode,
    todo__dissolve__builtins::ValueTypeTrait,
    ExpressionCompilationError, ExpressionEvaluationError,
};

pub trait BinaryExpression<T1: ValueTypeTrait, T2: ValueTypeTrait, R: ValueTypeTrait> {
    const OP_CODE: ExpressionOpCode;
    fn evaluate(a1: T1, a2: T2) -> Result<R, ExpressionEvaluationError>;
}

pub struct Binary<T1, T2, R, F>
where
    T1: ValueTypeTrait,
    T2: ValueTypeTrait,
    R: ValueTypeTrait,
    F: BinaryExpression<T1, T2, R>,
{
    pub phantom: PhantomData<(T1, T2, R, F)>,
}

impl<T1, T2, R, F> ExpressionInstruction for Binary<T1, T2, R, F>
where
    T1: ValueTypeTrait,
    T2: ValueTypeTrait,
    R: ValueTypeTrait,
    F: BinaryExpression<T1, T2, R>,
{
    const OP_CODE: ExpressionOpCode = F::OP_CODE;
    fn evaluate<'a>(state: &mut ExpressionEvaluationState<'a>) -> Result<(), ExpressionEvaluationError> {
        let a2: T2 = T2::from_value(state.pop_value()).unwrap();
        let a1: T1 = T1::from_value(state.pop_value()).unwrap();
        state.push_value(F::evaluate(a1, a2)?.into_value());
        Ok(())
    }
}

impl<T1, T2, R, F> SelfCompiling for Binary<T1, T2, R, F>
where
    T1: ValueTypeTrait,
    T2: ValueTypeTrait,
    R: ValueTypeTrait,
    F: BinaryExpression<T1, T2, R>,
{
    fn return_value_category(&self) -> Option<ValueTypeCategory> {
        Some(R::VALUE_TYPE_CATEGORY)
    }

    fn validate_and_append(builder: &mut ExpressionTreeCompiler<'_>) -> Result<(), ExpressionCompilationError> {
        let a2: T2 =
            T2::from_value(builder.pop_mock()?).map_err(|_| ExpressionCompilationError::InternalUnexpectedValueType)?;
        let a1: T1 =
            T1::from_value(builder.pop_mock()?).map_err(|_| ExpressionCompilationError::InternalUnexpectedValueType)?;
        builder.push_mock(R::mock_value());
        builder.append_instruction(Self::OP_CODE);
        Ok(())
    }
}

macro_rules! binary_instr {
    ( $( $name:ident = $impl_name:ident($a1:ident: $t1:ty, $a2:ident: $t2:ty) -> $r:ty $impl_code:block )* ) => { $(
        pub type $name = Binary<$t1, $t2, $r, $impl_name>;
        pub struct $impl_name {}
        impl BinaryExpression<$t1, $t2, $r> for $impl_name {
            const OP_CODE: ExpressionOpCode = ExpressionOpCode::$name;
            fn evaluate($a1: $t1, $a2: $t2) -> Result<$r, ExpressionEvaluationError> {
                $impl_code
            }
        })*
    };
}
pub(crate) use binary_instr;

binary_instr! {
    MathRemainderLong = MathRemainderLongImpl(a1: i64, a2: i64) -> i64 { Ok(i64::rem(a1, a2)) }
}
