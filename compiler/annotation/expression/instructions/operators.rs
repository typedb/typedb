/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::borrow::Cow;

use chrono::{DateTime, NaiveDate, NaiveDateTime};
use encoding::value::{
    decimal_value::Decimal,
    duration_value::{DateTimeExt, Duration},
    timezone::TimeZone,
};

use crate::annotation::expression::instructions::{
    ExpressionEvaluationError,
    binary::{Binary, BinaryExpression, binary_instruction},
    check_operation,
    op_codes::ExpressionOpCode,
};

binary_instruction! { 'a
    OpIntegerAddInteger(a1: i64, a2: i64) -> i64 { check_operation(i64::checked_add(a1, a2), "add") }
    OpIntegerSubtractInteger(a1: i64, a2: i64) -> i64 { check_operation(i64::checked_sub(a1, a2), "sub") }
    OpIntegerMultiplyInteger(a1: i64, a2: i64) -> i64 { check_operation(i64::checked_mul(a1, a2), "mul") }
    OpIntegerDivideInteger(a1: i64, a2: i64) -> f64 { checked_div(a1 as f64, a2 as f64) }
    OpIntegerModuloInteger(a1: i64, a2: i64) -> i64 { Ok(i64::rem_euclid(a1, a2)) }
    OpIntegerPowerInteger(a1: i64, a2: i64) -> f64 { Ok(f64::powf(a1 as f64, a2 as f64)) }

    OpDoubleAddDouble(a1: f64, a2: f64) -> f64 { Ok(a1 + a2) }
    OpDoubleSubtractDouble(a1: f64, a2: f64) -> f64 { Ok(a1 - a2) }
    OpDoubleMultiplyDouble(a1: f64, a2: f64) -> f64 { Ok(a1 * a2) }
    OpDoubleDivideDouble(a1: f64, a2: f64) -> f64 { checked_div(a1, a2) }
    OpDoubleModuloDouble(a1: f64, a2: f64) -> f64 { Ok(f64::rem_euclid(a1, a2)) }
    OpDoublePowerDouble(a1: f64, a2: f64) -> f64 { Ok(f64::powf(a1, a2)) }

    OpDecimalAddDecimal(a1: Decimal, a2: Decimal) -> Decimal { Ok( a1 + a2) }
    OpDecimalSubtractDecimal(a1: Decimal, a2: Decimal) -> Decimal { Ok(a1 - a2) }
    OpDecimalMultiplyDecimal(a1: Decimal, a2: Decimal) -> Decimal { Ok(a1 * a2) }

    OpDateSubtractDate(a1: NaiveDate, a2: NaiveDate) -> Duration {
        if a2 <= a1 {
            Ok(Duration::between_dates(a2, a1))
        } else {
            Err(ExpressionEvaluationError::NegativeDatetimeSub { lhs: a1.to_string(), rhs: a2.to_string()})
        }
    }

    OpDateTimeAddDuration(a1: NaiveDateTime, a2: Duration) -> NaiveDateTime {
        check_operation(DateTimeExt::checked_add(a1, a2), "add")
    }
    OpDateTimeSubtractDuration(a1: NaiveDateTime, a2: Duration) -> NaiveDateTime {
        check_operation(DateTimeExt::checked_sub(a1, a2), "sub")
    }
    OpDateTimeSubtractDateTime(a1: NaiveDateTime, a2: NaiveDateTime) -> Duration {
        if a2 <= a1 {
            Ok(Duration::between_datetimes(a2, a1))
        } else {
            Err(ExpressionEvaluationError::NegativeDatetimeSub { lhs: a1.to_string(), rhs: a2.to_string()})
        }
    }
    OpDateTimeSubtractDate(a1: NaiveDateTime, a2: NaiveDate) -> Duration {
        let a2 = NaiveDateTime::from(a2);
        if a2 <= a1 {
            Ok(Duration::between_datetimes(a2, a1))
        } else {
            Err(ExpressionEvaluationError::NegativeDatetimeSub { lhs: a1.to_string(), rhs: a2.to_string()})
        }
    }

    OpDateTimeTZAddDuration(a1: DateTime<TimeZone>, a2: Duration) -> DateTime<TimeZone> {
        check_operation(DateTimeExt::checked_add(a1, a2), "add")
    }
    OpDateTimeTZSubtractDuration(a1: DateTime<TimeZone>, a2: Duration) -> DateTime<TimeZone> {
        check_operation(DateTimeExt::checked_sub(a1, a2), "sub")
    }
    OpDateTimeTZSubtractDateTimeTZ(a1: DateTime<TimeZone>, a2: DateTime<TimeZone>) -> Duration {
        if a2 <= a1 {
            Ok(Duration::between_datetimes_tz(a2, a1))
        } else {
            Err(ExpressionEvaluationError::NegativeDatetimeSub { lhs: a1.to_string(), rhs: a2.to_string()})
        }
    }

    OpDurationAddDuration(a1: Duration, a2: Duration) -> Duration {
        check_operation(Duration::checked_add(a1, a2), "add")
    }

    OpDurationSubtractDuration(a1: Duration, a2: Duration) -> Duration {
        check_operation(Duration::checked_sub(a1, a2), "sub")
    }

    OpStringAddString(a1: Cow<'a, str>, a2: Cow<'a, str>) -> Cow<'a, str> { Ok(Cow::Owned(format!("{a1}{a2}"))) }
}

fn checked_div(a1: f64, a2: f64) -> Result<f64, ExpressionEvaluationError> {
    let res = a1 / a2;
    if res.is_finite() { Ok(res) } else { Err(ExpressionEvaluationError::DivisionFailed { dividend: a1, divisor: a2 }) }
}
