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
    duration_value::{Duration, DAYS_PER_WEEK, MONTHS_PER_YEAR, NANOS_PER_HOUR, NANOS_PER_MINUTE, NANOS_PER_SEC},
    timezone::TimeZone,
    value::Value,
};
use typeql::{
    annotation::Regex,
    common::{Span, Spanned},
    value::{
        BooleanLiteral, DateFragment, DateTimeLiteral, DateTimeTZLiteral, DurationLiteral, IntegerLiteral, Literal,
        Sign, SignedDecimalLiteral, SignedDoubleLiteral, SignedIntegerLiteral, StringLiteral, TimeFragment,
        ValueLiteral,
    },
};

use crate::LiteralParseError;

pub(crate) fn translate_literal(literal: &Literal) -> Result<Value<'static>, LiteralParseError> {
    Value::from_typeql_literal(literal, literal.span())
}

pub trait FromTypeQLLiteral: Sized {
    type TypeQLLiteral;
    fn from_typeql_literal(literal: &Self::TypeQLLiteral, source_span: Option<Span>)
        -> Result<Self, LiteralParseError>;
}

fn parse_primitive<T: FromStr>(fragment: &str, source_span: Option<Span>) -> Result<T, LiteralParseError> {
    fragment
        .parse::<T>()
        .map_err(|_| LiteralParseError::FragmentParseError { fragment: fragment.to_owned(), source_span })
}

impl FromTypeQLLiteral for Value<'static> {
    type TypeQLLiteral = Literal;

    fn from_typeql_literal(
        literal: &Self::TypeQLLiteral,
        source_span: Option<Span>,
    ) -> Result<Self, LiteralParseError> {
        // We don't know the final type yet. Zip with value-type annotations when constructing the executor.
        match &literal.inner {
            ValueLiteral::Boolean(boolean) => Ok(Value::Boolean(bool::from_typeql_literal(boolean, source_span)?)),
            ValueLiteral::Integer(integer) => Ok(Value::Integer(i64::from_typeql_literal(integer, source_span)?)),
            ValueLiteral::Decimal(decimal) => Ok(Value::Decimal(Decimal::from_typeql_literal(decimal, source_span)?)),
            ValueLiteral::Double(double) => Ok(Value::Double(f64::from_typeql_literal(double, source_span)?)),
            ValueLiteral::Date(date) => Ok(Value::Date(NaiveDate::from_typeql_literal(&date.date, source_span)?)),
            ValueLiteral::DateTime(datetime) => {
                Ok(Value::DateTime(NaiveDateTime::from_typeql_literal(datetime, source_span)?))
            }
            ValueLiteral::DateTimeTz(datetime_tz) => {
                Ok(Value::DateTimeTZ(chrono::DateTime::from_typeql_literal(datetime_tz, source_span)?))
            }
            ValueLiteral::Duration(duration) => {
                Ok(Value::Duration(Duration::from_typeql_literal(duration, source_span)?))
            }
            ValueLiteral::String(string) => {
                Ok(Value::String(Cow::Owned(String::from_typeql_literal(string, source_span)?)))
            }
            ValueLiteral::Struct(_) => {
                Err(LiteralParseError::UnimplementedLanguageFeature { feature: error::UnimplementedFeature::Structs })
            }
        }
    }
}

impl FromTypeQLLiteral for bool {
    type TypeQLLiteral = BooleanLiteral;

    fn from_typeql_literal(
        literal: &Self::TypeQLLiteral,
        source_span: Option<Span>,
    ) -> Result<Self, LiteralParseError> {
        parse_primitive(literal.value.as_str(), source_span)
    }
}

impl FromTypeQLLiteral for i64 {
    type TypeQLLiteral = SignedIntegerLiteral;

    fn from_typeql_literal(
        literal: &Self::TypeQLLiteral,
        source_span: Option<Span>,
    ) -> Result<Self, LiteralParseError> {
        let unsigned: i64 = parse_primitive(literal.integral.as_str(), source_span)?;
        Ok(match literal.sign.unwrap_or(Sign::Plus) {
            Sign::Plus => unsigned,
            Sign::Minus => -unsigned,
        })
    }
}

impl FromTypeQLLiteral for u32 {
    type TypeQLLiteral = IntegerLiteral;

    fn from_typeql_literal(
        literal: &Self::TypeQLLiteral,
        source_span: Option<Span>,
    ) -> Result<Self, LiteralParseError> {
        parse_primitive(literal.value.as_str(), source_span)
    }
}

impl FromTypeQLLiteral for u64 {
    type TypeQLLiteral = IntegerLiteral;

    fn from_typeql_literal(
        literal: &Self::TypeQLLiteral,
        source_span: Option<Span>,
    ) -> Result<Self, LiteralParseError> {
        parse_primitive(literal.value.as_str(), source_span)
    }
}

impl FromTypeQLLiteral for f64 {
    type TypeQLLiteral = SignedDoubleLiteral;
    fn from_typeql_literal(
        literal: &Self::TypeQLLiteral,
        source_span: Option<Span>,
    ) -> Result<Self, LiteralParseError> {
        let unsigned = parse_primitive::<f64>(literal.double.as_str(), source_span)?;
        match &literal.sign.unwrap_or(Sign::Plus) {
            Sign::Plus => Ok(unsigned),
            Sign::Minus => Ok(-unsigned),
        }
    }
}

impl FromTypeQLLiteral for Decimal {
    type TypeQLLiteral = SignedDecimalLiteral;

    fn from_typeql_literal(
        literal: &Self::TypeQLLiteral,
        source_span: Option<Span>,
    ) -> Result<Self, LiteralParseError> {
        let decimal = parse_primitive::<Decimal>(&literal.decimal, source_span)?;

        Ok(match literal.sign {
            None | Some(Sign::Plus) => decimal,
            Some(Sign::Minus) => -decimal,
        })
    }
}

impl FromTypeQLLiteral for NaiveDate {
    type TypeQLLiteral = DateFragment;

    fn from_typeql_literal(
        literal: &Self::TypeQLLiteral,
        source_span: Option<Span>,
    ) -> Result<Self, LiteralParseError> {
        let (year, month, day) = (
            parse_primitive(literal.year.as_str(), source_span)?,
            parse_primitive(literal.month.as_str(), source_span)?,
            parse_primitive(literal.day.as_str(), source_span)?,
        );
        NaiveDate::from_ymd_opt(year, month, day).ok_or(LiteralParseError::InvalidDate {
            year,
            month,
            day,
            source_span,
        })
    }
}

impl FromTypeQLLiteral for NaiveTime {
    type TypeQLLiteral = TimeFragment;

    fn from_typeql_literal(
        literal: &Self::TypeQLLiteral,
        source_span: Option<Span>,
    ) -> Result<Self, LiteralParseError> {
        let hour = parse_primitive(literal.hour.as_str(), source_span)?;
        let minute = parse_primitive(literal.minute.as_str(), source_span)?;
        let second =
            if let Some(second) = &literal.second { parse_primitive(second.as_str(), source_span)? } else { 0 };
        let nano = if let Some(fraction) = &literal.second_fraction {
            let number_len = fraction.len();
            parse_primitive::<u32>(fraction.as_str(), source_span)? * 10u32.pow(9 - number_len as u32)
        } else {
            0
        };
        if let Some(naive_time) = NaiveTime::from_hms_nano_opt(hour, minute, second, nano) {
            Ok(naive_time)
        } else {
            Err(LiteralParseError::InvalidTime { hour, minute, second, nano, source_span })
        }
    }
}

impl FromTypeQLLiteral for TimeZone {
    type TypeQLLiteral = typeql::value::TimeZone;

    fn from_typeql_literal(
        literal: &Self::TypeQLLiteral,
        source_span: Option<Span>,
    ) -> Result<Self, LiteralParseError> {
        match literal {
            typeql::value::TimeZone::IANA(name) => Ok(TimeZone::IANA(
                Tz::from_str_insensitive(name)
                    .map_err(|_| LiteralParseError::InvalidTimezoneNamed { name: name.clone(), source_span })?,
            )),
            typeql::value::TimeZone::ISO(value) => {
                Ok(TimeZone::Fixed(FixedOffset::from_str(value).map_err(|_| {
                    LiteralParseError::InvalidTimezoneFixedOffset { offset: value.clone(), source_span }
                })?))
            }
        }
    }
}

impl FromTypeQLLiteral for NaiveDateTime {
    type TypeQLLiteral = DateTimeLiteral;

    fn from_typeql_literal(
        literal: &Self::TypeQLLiteral,
        source_span: Option<Span>,
    ) -> Result<Self, LiteralParseError> {
        let date = NaiveDate::from_typeql_literal(&literal.date, source_span)?;
        let time = NaiveTime::from_typeql_literal(&literal.time, source_span)?;
        Ok(NaiveDateTime::new(date, time))
    }
}

impl FromTypeQLLiteral for chrono::DateTime<TimeZone> {
    type TypeQLLiteral = DateTimeTZLiteral;

    fn from_typeql_literal(
        literal: &Self::TypeQLLiteral,
        source_span: Option<Span>,
    ) -> Result<Self, LiteralParseError> {
        let date = NaiveDate::from_typeql_literal(&literal.date, source_span)?;
        let time = NaiveTime::from_typeql_literal(&literal.time, source_span)?;
        let tz = TimeZone::from_typeql_literal(&literal.timezone, source_span)?;
        Ok(NaiveDateTime::new(date, time).and_local_timezone(tz).unwrap())
    }
}

impl FromTypeQLLiteral for Duration {
    type TypeQLLiteral = DurationLiteral;

    fn from_typeql_literal(
        literal: &Self::TypeQLLiteral,
        source_span: Option<Span>,
    ) -> Result<Self, LiteralParseError> {
        let mut months = 0;
        let mut days = 0;
        let mut nanos = 0;

        match literal {
            DurationLiteral::Weeks(weeks) => days = DAYS_PER_WEEK * u32::from_typeql_literal(weeks, source_span)?,
            DurationLiteral::DateAndTime(date_part, time_part) => {
                months = MONTHS_PER_YEAR * parse_optional_int(&date_part.years, source_span)?.unwrap_or(0);
                months += parse_optional_int(&date_part.months, source_span)?.unwrap_or(0);
                days = parse_optional_int(&date_part.days, source_span)?.unwrap_or(0);
                if let Some(time_part) = time_part {
                    nanos = duration_time_part_to_nanos(time_part, source_span)?;
                }
            }
            DurationLiteral::Time(time_part) => nanos = duration_time_part_to_nanos(time_part, source_span)?,
        }

        Ok(Duration::new(months, days, nanos))
    }
}

fn duration_time_part_to_nanos(
    time_part: &typeql::value::DurationTime,
    source_span: Option<Span>,
) -> Result<u64, LiteralParseError> {
    let mut nanos = 0;
    nanos += NANOS_PER_HOUR * parse_optional_int(&time_part.hours, source_span)?.unwrap_or(0);
    nanos += NANOS_PER_MINUTE * parse_optional_int(&time_part.minutes, source_span)?.unwrap_or(0);
    let seconds_decimal = (time_part.seconds.as_ref())
        .map(|seconds| parse_primitive::<Decimal>(&seconds.value, source_span))
        .transpose()?
        .unwrap_or_default();
    nanos += seconds_decimal.integer_part() as u64 * NANOS_PER_SEC;
    let nanos_decimal = (seconds_decimal - seconds_decimal.integer_part()) * NANOS_PER_SEC;
    debug_assert_eq!(nanos_decimal.fractional_part(), 0);
    nanos += nanos_decimal.integer_part() as u64;
    Ok(nanos)
}

fn parse_optional_int<I: FromTypeQLLiteral<TypeQLLiteral = IntegerLiteral>>(
    literal: &Option<IntegerLiteral>,
    source_span: Option<Span>,
) -> Result<Option<I>, LiteralParseError> {
    match literal {
        Some(literal) => I::from_typeql_literal(literal, source_span).map(Some),
        None => Ok(None),
    }
}

impl FromTypeQLLiteral for String {
    type TypeQLLiteral = StringLiteral;

    fn from_typeql_literal(
        literal: &Self::TypeQLLiteral,
        source_span: Option<Span>,
    ) -> Result<Self, LiteralParseError> {
        literal.unescape().map_err(|err| LiteralParseError::CannotUnescapeString {
            literal: literal.clone(),
            typedb_source: err,
            source_span,
        })
    }
}

impl FromTypeQLLiteral for AnnotationRegex {
    type TypeQLLiteral = Regex;

    fn from_typeql_literal(
        literal: &Self::TypeQLLiteral,
        source_span: Option<Span>,
    ) -> Result<AnnotationRegex, LiteralParseError> {
        Ok(AnnotationRegex::new(literal.regex.unescape_regex().map_err(|err| {
            LiteralParseError::CannotUnescapeRegexString {
                literal: literal.regex.clone(),
                source_span,
                typedb_source: err,
            }
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
        pipeline::{function_signature::HashMapFunctionSignatureIndex, ParameterRegistry},
        translation::{match_::translate_match, TranslationContext},
        RepresentationError,
    };

    fn parse_value_via_typeql_expression(s: &str) -> Result<Value<'static>, Box<RepresentationError>> {
        let query = format!("match let $x = {}; select $x;", s);
        if let Stage::Match(match_) =
            typeql::parse_query(query.as_str()).unwrap().into_pipeline().stages.first().unwrap()
        {
            let mut context = TranslationContext::new();
            let mut value_parameters = ParameterRegistry::new();
            let block =
                translate_match(&mut context, &mut value_parameters, &HashMapFunctionSignatureIndex::empty(), match_)?
                    .finish()
                    .unwrap();
            let x = block.conjunction().constraints()[0].as_expression_binding().unwrap().expression().get_root();
            match *x {
                Expression::Constant(id) => Ok(value_parameters.value_unchecked(id).to_owned()),
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
            parse_value_via_typeql_expression("123.00456dec").unwrap(),
            Value::Decimal(Decimal::new(123, 456 * 10u64.pow(FRACTIONAL_PART_DENOMINATOR_LOG10 - 5)))
        );
        assert!(f64::abs(parse_value_via_typeql_expression("123.456").unwrap().unwrap_double() - 123.456) < 1.0e-6);
        assert!(f64::abs(parse_value_via_typeql_expression("123.456e0").unwrap().unwrap_double() - 123.456) < 1.0e-6);

        // TODO: Extend
    }
}
