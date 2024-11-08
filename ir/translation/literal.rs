/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{borrow::Cow, str::FromStr};

use chrono::{FixedOffset, NaiveDate, NaiveDateTime, NaiveTime};
use chrono_tz::Tz;
use concept::type_::annotation::AnnotationRegex;
use encoding::value::{
    decimal_value::Decimal,
    duration_value::{Duration, MONTHS_PER_YEAR, NANOS_PER_HOUR, NANOS_PER_MINUTE, NANOS_PER_SEC},
    timezone::TimeZone,
    value::Value,
};
use typeql::{
    annotation::Regex,
    value::{
        BooleanLiteral, DateFragment, DateTimeLiteral, DateTimeTZLiteral, DurationLiteral, IntegerLiteral, Literal,
        Sign, SignedDecimalLiteral, SignedIntegerLiteral, StringLiteral, TimeFragment, ValueLiteral,
    },
};

use crate::LiteralParseError;

pub(crate) fn translate_literal(literal: &Literal) -> Result<Value<'static>, LiteralParseError> {
    Value::from_typeql_literal(literal)
}

pub trait FromTypeQLLiteral: Sized {
    type TypeQLLiteral;
    fn from_typeql_literal(literal: &Self::TypeQLLiteral) -> Result<Self, LiteralParseError>;
}

fn parse_primitive<T: FromStr>(fragment: &str) -> Result<T, LiteralParseError> {
    fragment.parse::<T>().map_err(|_| LiteralParseError::FragmentParseError { fragment: fragment.to_owned() })
}

impl FromTypeQLLiteral for Value<'static> {
    type TypeQLLiteral = Literal;

    fn from_typeql_literal(literal: &Self::TypeQLLiteral) -> Result<Self, LiteralParseError> {
        // We don't know the final type yet. Zip with value-type annotations when constructing the executor.
        match &literal.inner {
            ValueLiteral::Boolean(boolean) => Ok(Value::Boolean(bool::from_typeql_literal(boolean)?)),
            ValueLiteral::Integer(integer) => Ok(Value::Long(i64::from_typeql_literal(integer)?)),
            ValueLiteral::Decimal(decimal) => match Decimal::from_typeql_literal(decimal) {
                Ok(decimal) => Ok(Value::Decimal(decimal)),
                Err(_) => Ok(Value::Double(f64::from_typeql_literal(decimal)?)),
            },
            ValueLiteral::Date(date) => Ok(Value::Date(NaiveDate::from_typeql_literal(&date.date)?)),
            ValueLiteral::DateTime(datetime) => Ok(Value::DateTime(NaiveDateTime::from_typeql_literal(datetime)?)),
            ValueLiteral::DateTimeTz(datetime_tz) => {
                Ok(Value::DateTimeTZ(chrono::DateTime::from_typeql_literal(datetime_tz)?))
            }
            ValueLiteral::Duration(duration) => Ok(Value::Duration(Duration::from_typeql_literal(duration)?)),
            ValueLiteral::String(string) => Ok(Value::String(Cow::Owned(String::from_typeql_literal(string)?))),
            ValueLiteral::Struct(_) => todo!(),
        }
    }
}

impl FromTypeQLLiteral for bool {
    type TypeQLLiteral = BooleanLiteral;

    fn from_typeql_literal(literal: &Self::TypeQLLiteral) -> Result<Self, LiteralParseError> {
        parse_primitive(literal.value.as_str())
    }
}

impl FromTypeQLLiteral for i64 {
    type TypeQLLiteral = SignedIntegerLiteral;

    fn from_typeql_literal(literal: &Self::TypeQLLiteral) -> Result<Self, LiteralParseError> {
        let unsigned: i64 = parse_primitive(literal.integral.as_str())?;
        Ok(match literal.sign.unwrap_or(Sign::Plus) {
            Sign::Plus => unsigned,
            Sign::Minus => -unsigned,
        })
    }
}

impl FromTypeQLLiteral for u32 {
    type TypeQLLiteral = IntegerLiteral;

    fn from_typeql_literal(literal: &Self::TypeQLLiteral) -> Result<Self, LiteralParseError> {
        parse_primitive(literal.value.as_str())
    }
}

impl FromTypeQLLiteral for u64 {
    type TypeQLLiteral = IntegerLiteral;

    fn from_typeql_literal(literal: &Self::TypeQLLiteral) -> Result<Self, LiteralParseError> {
        parse_primitive(literal.value.as_str())
    }
}

impl FromTypeQLLiteral for f64 {
    type TypeQLLiteral = SignedDecimalLiteral;
    fn from_typeql_literal(literal: &Self::TypeQLLiteral) -> Result<Self, LiteralParseError> {
        let unsigned = parse_primitive::<f64>(literal.decimal.as_str())?;
        match &literal.sign.unwrap_or(Sign::Plus) {
            Sign::Plus => Ok(unsigned),
            Sign::Minus => Ok(-unsigned),
        }
    }
}

impl FromTypeQLLiteral for Decimal {
    type TypeQLLiteral = SignedDecimalLiteral;

    fn from_typeql_literal(literal: &Self::TypeQLLiteral) -> Result<Self, LiteralParseError> {
        if literal.decimal.contains("e") {
            Err(LiteralParseError::ScientificNotationNotAllowedForDecimal { literal: literal.to_string() })?;
        }

        let decimal = parse_primitive::<Decimal>(&literal.decimal)?;

        Ok(match literal.sign {
            None | Some(Sign::Plus) => decimal,
            Some(Sign::Minus) => -decimal,
        })
    }
}

impl FromTypeQLLiteral for NaiveDate {
    type TypeQLLiteral = DateFragment;

    fn from_typeql_literal(literal: &Self::TypeQLLiteral) -> Result<Self, LiteralParseError> {
        let (year, month, day) = (
            parse_primitive(literal.year.as_str())?,
            parse_primitive(literal.month.as_str())?,
            parse_primitive(literal.day.as_str())?,
        );
        NaiveDate::from_ymd_opt(year, month, day).ok_or(LiteralParseError::InvalidDate { year, month, day })
    }
}

impl FromTypeQLLiteral for NaiveTime {
    type TypeQLLiteral = TimeFragment;

    fn from_typeql_literal(literal: &Self::TypeQLLiteral) -> Result<Self, LiteralParseError> {
        let hour = parse_primitive(literal.hour.as_str())?;
        let minute = parse_primitive(literal.minute.as_str())?;
        let second = if let Some(second) = &literal.second { parse_primitive(second.as_str())? } else { 0 };
        let nano = if let Some(fraction) = &literal.second_fraction {
            let number_len = fraction.len();
            parse_primitive::<u32>(fraction.as_str())? * 10u32.pow(9 - number_len as u32)
        } else {
            0
        };
        if let Some(naive_time) = NaiveTime::from_hms_nano_opt(hour, minute, second, nano) {
            Ok(naive_time)
        } else {
            Err(LiteralParseError::InvalidTime { hour, minute, second, nano })
        }
    }
}

impl FromTypeQLLiteral for TimeZone {
    type TypeQLLiteral = typeql::value::TimeZone;

    fn from_typeql_literal(literal: &Self::TypeQLLiteral) -> Result<Self, LiteralParseError> {
        match literal {
            typeql::value::TimeZone::IANA(name) => Ok(TimeZone::IANA(
                Tz::from_str_insensitive(name).map_err(|_| LiteralParseError::TimeZoneLookup { name: name.clone() })?,
            )),
            typeql::value::TimeZone::ISO(value) => Ok(TimeZone::Fixed(
                FixedOffset::from_str(value).map_err(|_| LiteralParseError::FixedOffset { value: value.clone() })?,
            )),
        }
    }
}

impl FromTypeQLLiteral for NaiveDateTime {
    type TypeQLLiteral = DateTimeLiteral;

    fn from_typeql_literal(literal: &Self::TypeQLLiteral) -> Result<Self, LiteralParseError> {
        let date = NaiveDate::from_typeql_literal(&literal.date)?;
        let time = NaiveTime::from_typeql_literal(&literal.time)?;
        Ok(NaiveDateTime::new(date, time))
    }
}

impl FromTypeQLLiteral for chrono::DateTime<TimeZone> {
    type TypeQLLiteral = DateTimeTZLiteral;

    fn from_typeql_literal(literal: &Self::TypeQLLiteral) -> Result<Self, LiteralParseError> {
        let date = NaiveDate::from_typeql_literal(&literal.date)?;
        let time = NaiveTime::from_typeql_literal(&literal.time)?;
        let tz = TimeZone::from_typeql_literal(&literal.timezone)?;
        Ok(NaiveDateTime::new(date, time).and_local_timezone(tz).unwrap())
    }
}

impl FromTypeQLLiteral for Duration {
    type TypeQLLiteral = DurationLiteral;

    fn from_typeql_literal(literal: &Self::TypeQLLiteral) -> Result<Self, LiteralParseError> {
        let mut months = 0;
        let mut days = 0;
        let mut nanos = 0;

        match literal {
            DurationLiteral::Weeks(weeks) => days += 7 * u32::from_typeql_literal(weeks)?,
            DurationLiteral::DateAndTime(date_part, time_part) => {
                months +=
                    MONTHS_PER_YEAR * date_part.years.as_ref().map(u32::from_typeql_literal).transpose()?.unwrap_or(0);
                months += date_part.months.as_ref().map(u32::from_typeql_literal).transpose()?.unwrap_or(0);
                days += date_part.days.as_ref().map(u32::from_typeql_literal).transpose()?.unwrap_or(0);

                if let Some(time_part) = time_part {
                    nanos += NANOS_PER_HOUR
                        * time_part.hours.as_ref().map(u64::from_typeql_literal).transpose()?.unwrap_or(0);
                    nanos += NANOS_PER_MINUTE
                        * time_part.minutes.as_ref().map(u64::from_typeql_literal).transpose()?.unwrap_or(0);

                    let seconds_decimal = (time_part.seconds.as_ref())
                        .map(|seconds| parse_primitive::<Decimal>(&seconds.value))
                        .transpose()?
                        .unwrap_or_default();

                    nanos += seconds_decimal.integer_part() as u64 * NANOS_PER_SEC;

                    let nanos_decimal = (seconds_decimal - seconds_decimal.integer_part()) * NANOS_PER_SEC;
                    debug_assert_eq!(nanos_decimal.fractional_part(), 0);
                    nanos += nanos_decimal.integer_part() as u64;
                }
            }
        }

        Ok(Duration::new(months, days, nanos))
    }
}

impl FromTypeQLLiteral for String {
    type TypeQLLiteral = StringLiteral;

    fn from_typeql_literal(literal: &Self::TypeQLLiteral) -> Result<Self, LiteralParseError> {
        literal
            .unescape()
            .map_err(|err| LiteralParseError::CannotUnescapeString { literal: literal.clone(), source: err })
    }
}

impl FromTypeQLLiteral for AnnotationRegex {
    type TypeQLLiteral = Regex;

    fn from_typeql_literal(literal: &Self::TypeQLLiteral) -> Result<AnnotationRegex, LiteralParseError> {
        Ok(AnnotationRegex::new(literal.regex.unescape_regex().map_err(|err| {
            LiteralParseError::CannotUnescapeRegexString { literal: literal.regex.clone(), source: err }
        })?))
    }
}

#[cfg(test)]
pub mod tests {
    use encoding::value::{
        decimal_value::{Decimal, FRACTIONAL_PART_DENOMINATOR_LOG10},
        value::Value,
    };
    use typeql::query::stage::Stage;

    use crate::{
        pattern::expression::Expression,
        pipeline::function_signature::HashMapFunctionSignatureIndex,
        translation::{match_::translate_match, TranslationContext},
        RepresentationError,
    };

    fn parse_value_via_typeql_expression(s: &str) -> Result<Value<'static>, RepresentationError> {
        let query = format!("match $x = {}; select $x;", s);
        if let Stage::Match(match_) =
            typeql::parse_query(query.as_str()).unwrap().into_pipeline().stages.first().unwrap()
        {
            let mut context = TranslationContext::new();
            let block =
                translate_match(&mut context, &HashMapFunctionSignatureIndex::empty(), match_)?.finish().unwrap();
            let x = block.conjunction().constraints()[0].as_expression_binding().unwrap().expression().get_root();
            match *x {
                Expression::Constant(id) => Ok(context.parameters.value_unchecked(id).to_owned()),
                _ => unreachable!(),
            }
        } else {
            unreachable!();
        }
    }

    #[test]
    fn parse() {
        assert_eq!(parse_value_via_typeql_expression("true").unwrap(), Value::Boolean(true));
        assert_ne!(parse_value_via_typeql_expression("false").unwrap(), Value::Boolean(true));

        assert_eq!(
            parse_value_via_typeql_expression("123.00456").unwrap(),
            Value::Decimal(Decimal::new(123, 456 * 10u64.pow(FRACTIONAL_PART_DENOMINATOR_LOG10 - 5)))
        );
        assert!(f64::abs(parse_value_via_typeql_expression("123.456e0").unwrap().unwrap_double() - 123.456) < 1.0e-6);

        // TODO: Extend
    }
}
