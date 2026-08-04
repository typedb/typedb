/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use encoding::value::value_type::ValueTypeCategory;
use ir::pattern::expression::Operator;
use typeql::common::Span;

use crate::annotation::expression::{
    ExpressionCompileError,
    expression_compiler::ExpressionCompilationContext,
    instructions::{
        CompilableExpression,
        cast::{
            CastLeftDecimalToDouble, CastLeftIntegerToDecimal, CastLeftIntegerToDouble, CastRightDecimalToDouble,
            CastRightIntegerToDecimal, CastRightIntegerToDouble,
        },
        operators,
    },
};

pub(crate) fn resolve_op(
    builder: &mut ExpressionCompilationContext<'_>,
    operator: Operator,
    left_category: ValueTypeCategory,
    right_category: ValueTypeCategory,
    source_span: Option<Span>,
) -> Result<(), Box<ExpressionCompileError>> {
    match left_category {
        ValueTypeCategory::Boolean => compile_op_boolean(builder, operator, right_category, source_span),
        ValueTypeCategory::Integer => compile_op_integer(builder, operator, right_category, source_span),
        ValueTypeCategory::Double => compile_op_double(builder, operator, right_category, source_span),
        ValueTypeCategory::Decimal => compile_op_decimal(builder, operator, right_category, source_span),
        ValueTypeCategory::Date => compile_op_date(builder, operator, right_category, source_span),
        ValueTypeCategory::DateTime => compile_op_datetime(builder, operator, right_category, source_span),
        ValueTypeCategory::DateTimeTZ => compile_op_datetime_tz(builder, operator, right_category, source_span),
        ValueTypeCategory::Duration => compile_op_duration(builder, operator, right_category, source_span),
        ValueTypeCategory::String => compile_op_string(builder, operator, right_category, source_span),
        ValueTypeCategory::Struct => compile_op_struct(builder, operator, right_category, source_span),
    }
}

fn compile_op_boolean(
    _builder: &mut ExpressionCompilationContext<'_>,
    op: Operator,
    right_category: ValueTypeCategory,
    source_span: Option<Span>,
) -> Result<(), Box<ExpressionCompileError>> {
    Err(Box::new(ExpressionCompileError::UnsupportedOperandsForOperation {
        op,
        left_category: ValueTypeCategory::Boolean,
        right_category,
        source_span,
    }))
}

fn compile_op_integer(
    builder: &mut ExpressionCompilationContext<'_>,
    op: Operator,
    right_category: ValueTypeCategory,
    source_span: Option<Span>,
) -> Result<(), Box<ExpressionCompileError>> {
    match right_category {
        ValueTypeCategory::Integer => {
            compile_op_integer_integer(builder, op)?;
        }
        ValueTypeCategory::Double => {
            CastLeftIntegerToDouble::validate_and_append(builder)?;
            compile_op_double_double(builder, op)?;
        }
        ValueTypeCategory::Decimal => match op {
            Operator::Add => {
                CastLeftIntegerToDecimal::validate_and_append(builder)?;
                operators::OpDecimalAddDecimal::validate_and_append(builder)?;
            }
            Operator::Subtract => {
                CastLeftIntegerToDecimal::validate_and_append(builder)?;
                operators::OpDecimalSubtractDecimal::validate_and_append(builder)?;
            }
            Operator::Multiply => {
                CastLeftIntegerToDecimal::validate_and_append(builder)?;
                operators::OpDecimalMultiplyDecimal::validate_and_append(builder)?;
            }
            other_op => {
                CastLeftIntegerToDouble::validate_and_append(builder)?;
                CastRightDecimalToDouble::validate_and_append(builder)?;
                compile_op_double_double(builder, other_op)?;
            }
        },
        _ => Err(ExpressionCompileError::UnsupportedOperandsForOperation {
            op,
            left_category: ValueTypeCategory::Integer,
            right_category,
            source_span,
        })?,
    }
    Ok(())
}

fn compile_op_double(
    builder: &mut ExpressionCompilationContext<'_>,
    op: Operator,
    right_category: ValueTypeCategory,
    source_span: Option<Span>,
) -> Result<(), Box<ExpressionCompileError>> {
    match right_category {
        ValueTypeCategory::Integer => {
            // The right needs to be cast
            CastRightIntegerToDouble::validate_and_append(builder)?;
            compile_op_double_double(builder, op)?;
        }
        ValueTypeCategory::Decimal => {
            // The right needs to be cast
            CastRightDecimalToDouble::validate_and_append(builder)?;
            compile_op_double_double(builder, op)?;
        }
        ValueTypeCategory::Double => {
            compile_op_double_double(builder, op)?;
        }
        _ => Err(ExpressionCompileError::UnsupportedOperandsForOperation {
            op,
            left_category: ValueTypeCategory::Double,
            right_category,
            source_span,
        })?,
    }
    Ok(())
}

fn compile_op_decimal(
    builder: &mut ExpressionCompilationContext<'_>,
    op: Operator,
    right_category: ValueTypeCategory,
    source_span: Option<Span>,
) -> Result<(), Box<ExpressionCompileError>> {
    match right_category {
        ValueTypeCategory::Integer => match op {
            Operator::Add => {
                CastRightIntegerToDecimal::validate_and_append(builder)?;
                operators::OpDecimalAddDecimal::validate_and_append(builder)?;
            }
            Operator::Subtract => {
                CastRightIntegerToDecimal::validate_and_append(builder)?;
                operators::OpDecimalSubtractDecimal::validate_and_append(builder)?;
            }
            Operator::Multiply => {
                CastRightIntegerToDecimal::validate_and_append(builder)?;
                operators::OpDecimalMultiplyDecimal::validate_and_append(builder)?;
            }
            other_op => {
                CastLeftDecimalToDouble::validate_and_append(builder)?;
                CastRightIntegerToDouble::validate_and_append(builder)?;
                compile_op_double_double(builder, other_op)?;
            }
        },
        ValueTypeCategory::Double => {
            CastLeftDecimalToDouble::validate_and_append(builder)?;
            compile_op_double_double(builder, op)?;
        }
        ValueTypeCategory::Decimal => match op {
            Operator::Add => operators::OpDecimalAddDecimal::validate_and_append(builder)?,
            Operator::Subtract => operators::OpDecimalSubtractDecimal::validate_and_append(builder)?,
            Operator::Multiply => operators::OpDecimalMultiplyDecimal::validate_and_append(builder)?,
            other_op => {
                CastLeftDecimalToDouble::validate_and_append(builder)?;
                CastRightDecimalToDouble::validate_and_append(builder)?;
                compile_op_double_double(builder, other_op)?;
            }
        },
        _ => Err(ExpressionCompileError::UnsupportedOperandsForOperation {
            op,
            left_category: ValueTypeCategory::Decimal,
            right_category,
            source_span,
        })?,
    }
    Ok(())
}

fn compile_op_string(
    builder: &mut ExpressionCompilationContext<'_>,
    op: Operator,
    right_category: ValueTypeCategory,
    source_span: Option<Span>,
) -> Result<(), Box<ExpressionCompileError>> {
    match (op, right_category) {
        (Operator::Add, ValueTypeCategory::String) => operators::OpStringAddString::validate_and_append(builder)?,
        _ => Err(Box::new(ExpressionCompileError::UnsupportedOperandsForOperation {
            op,
            left_category: ValueTypeCategory::String,
            right_category,
            source_span,
        }))?,
    }
    Ok(())
}

fn compile_op_date(
    builder: &mut ExpressionCompilationContext<'_>,
    op: Operator,
    right_category: ValueTypeCategory,
    source_span: Option<Span>,
) -> Result<(), Box<ExpressionCompileError>> {
    match (op, right_category) {
        (Operator::Subtract, ValueTypeCategory::Date) => operators::OpDateSubtractDate::validate_and_append(builder)?,
        _ => Err(Box::new(ExpressionCompileError::UnsupportedOperandsForOperation {
            op,
            left_category: ValueTypeCategory::Date,
            right_category,
            source_span,
        }))?,
    }
    Ok(())
}

fn compile_op_datetime(
    builder: &mut ExpressionCompilationContext<'_>,
    op: Operator,
    right_category: ValueTypeCategory,
    source_span: Option<Span>,
) -> Result<(), Box<ExpressionCompileError>> {
    match (op, right_category) {
        (Operator::Subtract, ValueTypeCategory::Date) => {
            operators::OpDateTimeSubtractDate::validate_and_append(builder)?
        }
        (Operator::Subtract, ValueTypeCategory::DateTime) => {
            operators::OpDateTimeSubtractDateTime::validate_and_append(builder)?
        }
        (Operator::Add, ValueTypeCategory::Duration) => operators::OpDateTimeAddDuration::validate_and_append(builder)?,
        (Operator::Subtract, ValueTypeCategory::Duration) => {
            operators::OpDateTimeSubtractDuration::validate_and_append(builder)?
        }
        _ => Err(Box::new(ExpressionCompileError::UnsupportedOperandsForOperation {
            op,
            left_category: ValueTypeCategory::DateTime,
            right_category,
            source_span,
        }))?,
    }
    Ok(())
}

fn compile_op_datetime_tz(
    builder: &mut ExpressionCompilationContext<'_>,
    op: Operator,
    right_category: ValueTypeCategory,
    source_span: Option<Span>,
) -> Result<(), Box<ExpressionCompileError>> {
    match (op, right_category) {
        (Operator::Subtract, ValueTypeCategory::DateTimeTZ) => {
            operators::OpDateTimeTZSubtractDateTimeTZ::validate_and_append(builder)?
        }
        (Operator::Add, ValueTypeCategory::Duration) => {
            operators::OpDateTimeTZAddDuration::validate_and_append(builder)?
        }
        (Operator::Subtract, ValueTypeCategory::Duration) => {
            operators::OpDateTimeTZSubtractDuration::validate_and_append(builder)?
        }
        _ => Err(Box::new(ExpressionCompileError::UnsupportedOperandsForOperation {
            op,
            left_category: ValueTypeCategory::DateTimeTZ,
            right_category,
            source_span,
        }))?,
    }
    Ok(())
}

fn compile_op_duration(
    builder: &mut ExpressionCompilationContext<'_>,
    op: Operator,
    right_category: ValueTypeCategory,
    source_span: Option<Span>,
) -> Result<(), Box<ExpressionCompileError>> {
    match (op, right_category) {
        (Operator::Add, ValueTypeCategory::Duration) => operators::OpDurationAddDuration::validate_and_append(builder)?,
        (Operator::Subtract, ValueTypeCategory::Duration) => {
            operators::OpDurationSubtractDuration::validate_and_append(builder)?
        }
        _ => Err(Box::new(ExpressionCompileError::UnsupportedOperandsForOperation {
            op,
            left_category: ValueTypeCategory::Duration,
            right_category,
            source_span,
        }))?,
    }
    Ok(())
}

fn compile_op_struct(
    _builder: &mut ExpressionCompilationContext<'_>,
    op: Operator,
    right_category: ValueTypeCategory,
    source_span: Option<Span>,
) -> Result<(), Box<ExpressionCompileError>> {
    Err(Box::new(ExpressionCompileError::UnsupportedOperandsForOperation {
        op,
        left_category: ValueTypeCategory::Struct,
        right_category,
        source_span,
    }))
}

// Ops with Left, Right resolved
fn compile_op_integer_integer(
    builder: &mut ExpressionCompilationContext<'_>,
    op: Operator,
) -> Result<(), Box<ExpressionCompileError>> {
    match op {
        Operator::Add => operators::OpIntegerAddInteger::validate_and_append(builder)?,
        Operator::Subtract => operators::OpIntegerSubtractInteger::validate_and_append(builder)?,
        Operator::Multiply => operators::OpIntegerMultiplyInteger::validate_and_append(builder)?,
        Operator::Divide => operators::OpIntegerDivideInteger::validate_and_append(builder)?,
        Operator::Modulo => operators::OpIntegerModuloInteger::validate_and_append(builder)?,
        Operator::Power => operators::OpIntegerPowerInteger::validate_and_append(builder)?,
    }
    Ok(())
}

fn compile_op_double_double(
    builder: &mut ExpressionCompilationContext<'_>,
    op: Operator,
) -> Result<(), Box<ExpressionCompileError>> {
    match op {
        Operator::Add => operators::OpDoubleAddDouble::validate_and_append(builder)?,
        Operator::Subtract => operators::OpDoubleSubtractDouble::validate_and_append(builder)?,
        Operator::Multiply => operators::OpDoubleMultiplyDouble::validate_and_append(builder)?,
        Operator::Divide => operators::OpDoubleDivideDouble::validate_and_append(builder)?,
        Operator::Modulo => operators::OpDoubleModuloDouble::validate_and_append(builder)?,
        Operator::Power => operators::OpDoublePowerDouble::validate_and_append(builder)?,
    }
    Ok(())
}
