/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::borrow::Cow;

use chrono::{DateTime, NaiveDate, NaiveDateTime};
use encoding::value::{decimal_value::Decimal, duration_value::Duration, timezone::TimeZone};

use crate::annotation::expression::instructions::{
    binary::{binary_instruction, Binary, BinaryExpression},
    check_operation,
    op_codes::ExpressionOpCode,
    ExpressionEvaluationError,
};

binary_instruction! { 'a
    OpIntegerAddInteger = OpIntegerAddIntegerImpl(a1: i64, a2: i64) -> i64 { check_operation(i64::checked_add(a1, a2), "add") }
    OpIntegerSubtractInteger = OpIntegerSubtractIntegerImpl(a1: i64, a2: i64) -> i64 { check_operation(i64::checked_sub(a1, a2), "sub") }
    OpIntegerMultiplyInteger = OpIntegerMultiplyIntegerImpl(a1: i64, a2: i64) -> i64 { check_operation(i64::checked_mul(a1, a2), "mul") }
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

    OpDateSubtractDate = OpDateSubtractDateImpl(a1: NaiveDate, a2: NaiveDate) -> Duration {
        if a2 <= a1 {
            Ok(Duration::between_dates(a2, a1))
        } else {
            Err(ExpressionEvaluationError::NegativeDurationSub { lhs: a1.to_string(), rhs: a2.to_string()})
        }
    }

    OpDateTimeAddDuration = OpDateTimeAddDurationImpl(a1: NaiveDateTime, a2: Duration) -> NaiveDateTime { Ok(a1 + a2) } // TODO range check
    OpDateTimeSubtractDuration = OpDateTimeSubtractDurationImpl(a1: NaiveDateTime, a2: Duration) -> NaiveDateTime { Ok(a1 - a2) } // TODO range check
    OpDateTimeSubtractDateTime = OpDateTimeSubtractDateTimeImpl(a1: NaiveDateTime, a2: NaiveDateTime) -> Duration {
        if a2 <= a1 {
            Ok(Duration::between_datetimes(a2, a1))
        } else {
            Err(ExpressionEvaluationError::NegativeDurationSub { lhs: a1.to_string(), rhs: a2.to_string()})
        }
    }
    OpDateTimeSubtractDate = OpDateTimeSubtractDateImpl(a1: NaiveDateTime, a2: NaiveDate) -> Duration {
        let a2 = NaiveDateTime::from(a2);
        if a2 <= a1 {
            Ok(Duration::between_datetimes(a2, a1))
        } else {
            Err(ExpressionEvaluationError::NegativeDurationSub { lhs: a1.to_string(), rhs: a2.to_string()})
        }
    }

    OpDateTimeTZAddDuration = OpDateTimeTZAddDurationImpl(a1: DateTime<TimeZone>, a2: Duration) -> DateTime<TimeZone> { Ok(a1 + a2) } // TODO range check
    OpDateTimeTZSubtractDuration = OpDateTimeTZSubtractDurationImpl(a1: DateTime<TimeZone>, a2: Duration) -> DateTime<TimeZone> { Ok(a1 - a2) } // TODO range check
    OpDateTimeTZSubtractDateTimeTZ = OpDateTimeTZSubtractDateTimeTZImpl(a1: DateTime<TimeZone>, a2: DateTime<TimeZone>) -> Duration {
        if a2 <= a1 {
            Ok(Duration::between_datetimes_tz(a2, a1))
        } else {
            Err(ExpressionEvaluationError::NegativeDurationSub { lhs: a1.to_string(), rhs: a2.to_string()})
        }
    }

    OpDurationAddDuration = OpDurationAddDurationImpl(a1: Duration, a2: Duration) -> Duration { Ok(a1 + a2) }
    OpDurationSubtractDuration = OpDurationSubtractDurationImpl(a1: Duration, a2: Duration) -> Duration { Ok(a1 - a2) } // TODO range check

    OpStringAddString = OpStringAddStringImpl(a1: Cow<'a, str>, a2: Cow<'a, str>) -> Cow<'a, str> { Ok(Cow::Owned(format!("{a1}{a2}"))) }
}

fn checked_div(a1: f64, a2: f64) -> Result<f64, ExpressionEvaluationError> {
    let res = a1 / a2;
    if res.is_finite() {
        Ok(res)
    } else {
        Err(ExpressionEvaluationError::DivisionFailed { dividend: a1, divisor: a2 })
    }
}
