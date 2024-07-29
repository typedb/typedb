/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::marker::PhantomData;

use encoding::value::value_type::ValueTypeCategory;

use crate::expressions::{
    evaluator::ExpressionEvaluationState,
    expression_compiler::{ExpressionInstruction, ExpressionTreeCompiler, SelfCompiling},
    op_codes::ExpressionOpCode,
    todo__dissolve__builtins::ValueTypeTrait,
    ExpressionCompilationError, ExpressionEvaluationError,
};

pub trait UnaryExpression<T1: ValueTypeTrait, R: ValueTypeTrait> {
    const OP_CODE: ExpressionOpCode; // Until we have a better solution, this helps the macro ensure the op code exists.
    fn evaluate(a1: T1) -> Result<R, ExpressionEvaluationError>;
}

pub struct Unary<T1, R, F>
where
    T1: ValueTypeTrait,
    R: ValueTypeTrait,
    F: UnaryExpression<T1, R>,
{
    phantom: PhantomData<(T1, R, F)>,
}

impl<T1, R, F> ExpressionInstruction for Unary<T1, R, F>
where
    T1: ValueTypeTrait,
    R: ValueTypeTrait,
    F: UnaryExpression<T1, R>,
{
    const OP_CODE: ExpressionOpCode = F::OP_CODE;
    fn evaluate<'a>(state: &mut ExpressionEvaluationState<'a>) -> Result<(), ExpressionEvaluationError> {
        let a1: T1 = T1::from_value(state.pop_value()).unwrap();
        state.push_value(F::evaluate(a1)?.into_value());
        Ok(())
    }
}

impl<T1, R, F> SelfCompiling for Unary<T1, R, F>
where
    T1: ValueTypeTrait,
    R: ValueTypeTrait,
    F: UnaryExpression<T1, R>,
{
    fn return_value_category(&self) -> Option<ValueTypeCategory> {
        Some(R::VALUE_TYPE_CATEGORY)
    }

    fn validate_and_append(builder: &mut ExpressionTreeCompiler<'_>) -> Result<(), ExpressionCompilationError> {
        let a1: T1 =
            T1::from_value(builder.pop_mock()?).map_err(|_| ExpressionCompilationError::InternalUnexpectedValueType)?;
        builder.push_mock(R::mock_value());
        builder.append_instruction(Self::OP_CODE);
        Ok(())
    }
}

macro_rules! unary_instr {
    ( $( $name:ident = $impl_name:ident($a1:ident: $t1:ty) -> $r:ty $impl_code:block )* ) => { $(
        pub type $name = Unary<$t1, $r, $impl_name>;
        pub struct $impl_name {}
        impl UnaryExpression<$t1, $r> for $impl_name {
            const OP_CODE: ExpressionOpCode = ExpressionOpCode::$name;
            fn evaluate($a1: $t1) -> Result<$r, ExpressionEvaluationError> {
                $impl_code
            }
        })*
    };
}
pub(crate) use unary_instr;

unary_instr! {
    MathAbsLong = MathAbsLongImpl(a1: i64) -> i64 { Ok(i64::abs(a1)) }
    MathAbsDouble = MathAbsDoubleImpl(a1: f64) -> f64 { Ok(f64::abs(a1)) }
    MathRoundDouble = MathRoundDoubleImpl(a1: f64) -> i64 { Ok(f64::round_ties_even(a1) as i64) } // TODO: Should this be round_ties_even?
    MathCeilDouble = MathCeilDoubleImpl(a1: f64) -> i64 { Ok(f64::ceil(a1) as i64) }
    MathFloorDouble = MathFloorDoubleImpl(a1: f64) -> i64 { Ok(f64::floor(a1) as i64) }
}
