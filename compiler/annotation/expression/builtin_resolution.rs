/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use encoding::value::value_type::ValueTypeCategory;
use ir::pattern::expression::BuiltinValueFunctionID;
use paste::paste;
use typeql::common::Span;

use crate::annotation::expression::{
    ExpressionCompileError,
    expression_compiler::{BinaryValueFunctionResolver, ExpressionCompilationContext, UnaryValueFunctionResolver},
    instructions::{
        CompilableExpression,
        binary::{
            MathMaxDecimalDecimal, MathMaxDoubleDouble, MathMaxIntegerInteger, MathMinDecimalDecimal,
            MathMinDoubleDouble, MathMinIntegerInteger,
        },
        unary::{
            LenString, MathAbsDecimal, MathAbsDouble, MathAbsInteger, MathCeilDecimal, MathCeilDouble,
            MathFloorDecimal, MathFloorDouble, MathRoundDecimal, MathRoundDouble,
        },
    },
};

macro_rules! unary_builtin {
    ( $( $fid:ident = $impl_prefix:ident $types:tt )* ) => {
        $( unary_builtin_tt! { $fid = $impl_prefix $types  } )*
    }
}

macro_rules! unary_builtin_tt {
    ($fid:ident = $impl_prefix:ident [ $( $t1:ident, )* ]) => {
        paste::paste!(unary_builtin_impl! { $fid = $impl_prefix [ $( $t1 = [<$impl_prefix $t1>], )* ] } );
    }
}

macro_rules! unary_builtin_impl {
    ($fid:ident = $impl_prefix:ident [ $( $t1:ident = $variant_impl:ident, )* ]) => {
pub(super) struct $fid;
impl UnaryValueFunctionResolver for $fid {
    const UNARY_ID: BuiltinValueFunctionID = BuiltinValueFunctionID::$fid;

    fn resolve_validate_append_unary(t1: ValueTypeCategory, builder: &mut ExpressionCompilationContext<'_>, source_span: Option<Span>) -> Result<(), Box<ExpressionCompileError>> {
        match t1 {
            $ ( ValueTypeCategory::$t1 => $variant_impl::validate_and_append(builder), )*
            other => {
                Err(Box::new(ExpressionCompileError::UnsupportedArgumentsForBuiltin {
                    function: Self::UNARY_ID,
                    category: t1, // TODO: Add arg2
                    source_span: source_span,
                }))
            }
        }
    }
}
    };
}

macro_rules! binary_builtin {
    ( $( $fid:ident:$same_args:literal = $impl_prefix:ident $types:tt )* ) => {
        $( binary_builtin_tt! { $fid:$same_args = $impl_prefix $types  } )*
    }
}

macro_rules! binary_builtin_tt {
    ($fid:ident:$same_args:literal = $impl_prefix:ident [ $( ($t1:ident, $t2:ident), )* ]) => {
        paste::paste!(binary_builtin_impl! { $fid:$same_args = $impl_prefix [ $( ($t1, $t2) = [<$impl_prefix $t1 $t2>], )* ] } );
    }
}

macro_rules! binary_builtin_impl {
    ($fid:ident:$same_args:literal = $impl_prefix:ident [ $( ($t1:ident, $t2: ident) = $variant_impl:ident, )* ]) => {
pub(super) struct $fid;
impl BinaryValueFunctionResolver for $fid {
    const BINARY_ID: BuiltinValueFunctionID = BuiltinValueFunctionID::$fid;
    const ARGS_MUST_HAVE_SAME_CATEGORIES: bool = $same_args;
    fn resolve_validate_append_binary(t1: ValueTypeCategory, t2: ValueTypeCategory, builder: &mut ExpressionCompilationContext<'_>, source_span: Option<Span>) -> Result<(), Box<ExpressionCompileError>> {
        match (t1, t2) {
            $ ( (ValueTypeCategory::$t1, ValueTypeCategory::$t2)  => $variant_impl::validate_and_append(builder), )*
            other => {
                Err(Box::new(ExpressionCompileError::UnsupportedArgumentsForBuiltin {
                    function: Self::BINARY_ID,
                    category: t1, // TODO: Add arg2
                    source_span: source_span,
                }))
            }
        }
    }
}
    };
}
unary_builtin! {
    Abs = MathAbs [ Integer, Double, Decimal, ]
    Ceil = MathCeil [ Double, Decimal, ]
    Floor = MathFloor [ Double, Decimal, ]
    Round = MathRound [ Double, Decimal, ]
    Len = Len [ String, ]
}

binary_builtin! {
    Max:true = MathMax [ (Integer, Integer), (Double, Double), (Decimal, Decimal), ]
    Min:true = MathMin [ (Integer, Integer), (Double, Double), (Decimal, Decimal), ]
}
