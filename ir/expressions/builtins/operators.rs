/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use crate::expressions::{
    builtins::{
        binary::{binary_instr, Binary, BinaryExpression},
        check_operation,
    },
    op_codes::ExpressionOpCode,
    ExpressionEvaluationError,
};
binary_instr! {
    OpLongAddLong = OpLongAddLongImpl(a1: i64, a2: i64) -> i64 { check_operation(i64::checked_add(a1, a2)) }
    OpLongSubtractLong = OpLongSubtractLongImpl(a1: i64, a2: i64) -> i64 { check_operation(i64::checked_sub(a1, a2)) }
    OpLongMultiplyLong = OpLongMultiplyLongImpl(a1: i64, a2: i64) -> i64 { check_operation(i64::checked_mul(a1, a2)) }
    OpLongDivideLong = OpLongDivideLongImpl(a1: i64, a2: i64) -> f64 { Ok(a1 as f64 / a2 as f64) }
    OpLongModuloLong = OpLongModuloLongImpl(a1: i64, a2: i64) -> i64 { Ok(i64::rem_euclid(a1, a2)) }
    OpLongPowerLong = OpLongPowerLongImpl(a1: i64, a2: i64) -> f64 { Ok(f64::powf(a1 as f64, a2 as f64)) }

    OpDoubleAddDouble = OpDoubleAddDoubleImpl(a1: f64, a2: f64) -> f64 { Ok(a1 + a2) }
    OpDoubleSubtractDouble = OpDoubleSubtractDoubleImpl(a1: f64, a2: f64) -> f64 { Ok(a1 - a2) }
    OpDoubleMultiplyDouble = OpDoubleMultiplyDoubleImpl(a1: f64, a2: f64) -> f64 { Ok(a1 * a2) }
    OpDoubleDivideDouble = OpDoubleDivideDoubleImpl(a1: f64, a2: f64) -> f64 { Ok(a1 / a2) }
    OpDoubleModuloDouble = OpDoubleModuloDoubleImpl(a1: f64, a2: f64) -> f64 { Ok(f64::rem_euclid(a1, a2)) }
    OpDoublePowerDouble = OpDoublePowerDoubleImpl(a1: f64, a2: f64) -> f64 { Ok(f64::powf(a1, a2)) }
}
