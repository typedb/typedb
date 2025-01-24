/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use encoding::value::decimal_value::Decimal;

use crate::annotation::expression::instructions::{
    binary::{binary_instruction, Binary, BinaryExpression},
    check_operation,
    op_codes::ExpressionOpCode,
    ExpressionEvaluationError,
};

binary_instruction! {
    OpIntegerAddInteger = OpIntegerAddIntegerImpl(a1: i64, a2: i64) -> i64 { check_operation(i64::checked_add(a1, a2)) }
    OpIntegerSubtractInteger = OpIntegerSubtractIntegerImpl(a1: i64, a2: i64) -> i64 { check_operation(i64::checked_sub(a1, a2)) }
    OpIntegerMultiplyInteger = OpIntegerMultiplyIntegerImpl(a1: i64, a2: i64) -> i64 { check_operation(i64::checked_mul(a1, a2)) }
    OpIntegerDivideInteger = OpIntegerDivideIntegerImpl(a1: i64, a2: i64) -> f64 { checked_div(a1 as f64, a2 as f64) }
    OpIntegerModuloInteger = OpIntegerModuloIntegerImpl(a1: i64, a2: i64) -> i64 { Ok(i64::rem_euclid(a1, a2)) }
    OpIntegerPowerInteger = OpIntegerPowerIntegerImpl(a1: i64, a2: i64) -> f64 { Ok(f64::powf(a1 as f64, a2 as f64)) }

    OpDoubleAddDouble = OpDoubleAddDoubleImpl(a1: f64, a2: f64) -> f64 { Ok(a1 + a2) }
    OpDoubleSubtractDouble = OpDoubleSubtractDoubleImpl(a1: f64, a2: f64) -> f64 { Ok(a1 - a2) }
    OpDoubleMultiplyDouble = OpDoubleMultiplyDoubleImpl(a1: f64, a2: f64) -> f64 { Ok(a1 * a2) }
    OpDoubleDivideDouble = OpDoubleDivideDoubleImpl(a1: f64, a2: f64) -> f64 { checked_div(a1, a2) }
    OpDoubleModuloDouble = OpDoubleModuloDoubleImpl(a1: f64, a2: f64) -> f64 { Ok(f64::rem_euclid(a1, a2)) }
    OpDoublePowerDouble = OpDoublePowerDoubleImpl(a1: f64, a2: f64) -> f64 { Ok(f64::powf(a1, a2)) }

    OpDecimalAddDecimal = OpDecimalAddDecimalImpl(a1: Decimal, a2: Decimal) -> Decimal { Ok( a1 + a2) }
    OpDecimalSubtractDecimal = OpDecimalSubtractDecimalImpl(a1: Decimal, a2: Decimal) -> Decimal { Ok(a1 - a2) }
    OpDecimalMultiplyDecimal = OpDecimalMultiplyDecimalImpl(a1: Decimal, a2: Decimal) -> Decimal { Ok(a1 * a2) }
}

fn checked_div(a1: f64, a2: f64) -> Result<f64, ExpressionEvaluationError> {
    let res = a1 / a2;
    if res.is_normal() {
        Ok(res)
    } else {
        Err(ExpressionEvaluationError::DivisionFailed { dividend: a1 as f64, divisor: a2 })
    }
}
