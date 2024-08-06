/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{marker::PhantomData, ops::Rem};

use encoding::value::{value::DBValue, value_type::ValueTypeCategory};

use crate::instruction::expression::{
    op_codes::ExpressionOpCode, CompilableExpression, ExpressionEvaluationError, ExpressionInstruction,
};
use crate::{
    expression::ExpressionCompileError,
    expression::expression_compiler::ExpressionCompilationContext
};

pub trait BinaryExpression<T1: DBValue, T2: DBValue, R: DBValue> {
    const OP_CODE: ExpressionOpCode;
    fn evaluate(a1: T1, a2: T2) -> Result<R, ExpressionEvaluationError>;
}

pub struct Binary<T1, T2, R, F>
where
    T1: DBValue,
    T2: DBValue,
    R: DBValue,
    F: BinaryExpression<T1, T2, R>,
{
    pub phantom: PhantomData<(T1, T2, R, F)>,
}

impl<T1, T2, R, F> ExpressionInstruction for Binary<T1, T2, R, F>
where
    T1: DBValue,
    T2: DBValue,
    R: DBValue,
    F: BinaryExpression<T1, T2, R>,
{
    const OP_CODE: ExpressionOpCode = F::OP_CODE;
}

impl<T1, T2, R, F> CompilableExpression for Binary<T1, T2, R, F>
where
    T1: DBValue,
    T2: DBValue,
    R: DBValue,
    F: BinaryExpression<T1, T2, R>,
{
    fn return_value_category(&self) -> Option<ValueTypeCategory> {
        Some(R::VALUE_TYPE_CATEGORY)
    }

    fn validate_and_append(builder: &mut ExpressionCompilationContext<'_>) -> Result<(), ExpressionCompileError> {
        let a2 = builder.pop_type_single()?;
        let a1 = builder.pop_type_single()?;
        if (a1, a2) != (T1::VALUE_TYPE_CATEGORY, T2::VALUE_TYPE_CATEGORY) {
            Err(ExpressionCompileError::InternalUnexpectedValueType)?;
        }
        builder.push_type_single(R::VALUE_TYPE_CATEGORY);
        builder.append_instruction(Self::OP_CODE);
        Ok(())
    }
}

macro_rules! binary_instruction {
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

pub(crate) use binary_instruction;

binary_instruction! {
    MathRemainderLong = MathRemainderLongImpl(a1: i64, a2: i64) -> i64 { Ok(i64::rem(a1, a2)) }
}
