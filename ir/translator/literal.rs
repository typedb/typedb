/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{borrow::Cow, str::FromStr};

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime};
use chrono_tz::Tz;
use encoding::value::{
    decimal_value::{Decimal, FRACTIONAL_PART_DENOMINATOR_LOG10},
    value::Value,
};
use typeql::value::{DateTimeTZLiteral, Literal, Sign, SignedDecimalLiteral, TimeZone, ValueLiteral};

use crate::{LiteralParseError, PatternDefinitionError};

pub(crate) fn parse_literal(literal: &Literal) -> Result<Value<'static>, PatternDefinitionError> {
    parse_literal_impl(literal)
        .map_err(|source| PatternDefinitionError::LiteralParseError { literal: literal.to_string(), source })
}

fn parse_literal_impl(literal: &Literal) -> Result<Value<'static>, LiteralParseError> {
    // We don't know the final type yet. Zip with value-type annotations when constructing the executor.
    Ok(match &literal.inner {
        ValueLiteral::Boolean(boolean) => Value::Boolean(bool::from_typeql_literal(boolean)?),
        ValueLiteral::Integer(integer) => Value::Long(i64::from_typeql_literal(integer)?),
        ValueLiteral::Decimal(decimal) => match Decimal::from_typeql_literal(decimal) {
            Ok(decimal) => Value::Decimal(decimal),
            Err(_) => Value::Double(f64::from_typeql_literal(decimal)?),
        },
        ValueLiteral::Date(date) => Value::Date(NaiveDate::from_typeql_literal(&date.date)?),
        ValueLiteral::DateTime(datetime) => Value::DateTime(NaiveDateTime::from_typeql_literal(datetime)?),
        ValueLiteral::DateTimeTz(datetime_tz) => {
            Value::DateTimeTZ(chrono::DateTime::<chrono_tz::Tz>::from_typeql_literal(datetime_tz)?)
        }
        ValueLiteral::Duration(_) => todo!(),
        ValueLiteral::String(string) => Value::String(Cow::Owned(string.value.clone())),
    })
}

pub trait FromTypeQLLiteral: Sized {
    type TypeQLLiteral;
    fn from_typeql_literal(literal: &Self::TypeQLLiteral) -> Result<Self, LiteralParseError>;

    fn parse_primitive<T: FromStr>(fragment: &str) -> Result<T, LiteralParseError> {
        fragment.parse::<T>().map_err(|_| LiteralParseError::FragmentParseError { fragment: fragment.to_owned() })
    }
}

impl FromTypeQLLiteral for bool {
    type TypeQLLiteral = typeql::value::BooleanLiteral;

    fn from_typeql_literal(literal: &Self::TypeQLLiteral) -> Result<Self, LiteralParseError> {
        Self::parse_primitive(literal.value.as_str())
    }
}

impl FromTypeQLLiteral for i64 {
    type TypeQLLiteral = typeql::value::SignedIntegerLiteral;

    fn from_typeql_literal(literal: &Self::TypeQLLiteral) -> Result<Self, LiteralParseError> {
        let magnitude: i64 = Self::parse_primitive(literal.integral.as_str())?;
        Ok(match literal.sign {
            Sign::Plus => 1,
            Sign::Minus => -1,
        } * magnitude)
    }
}

impl FromTypeQLLiteral for f64 {
    type TypeQLLiteral = SignedDecimalLiteral;
    fn from_typeql_literal(literal: &Self::TypeQLLiteral) -> Result<Self, LiteralParseError> {
        let sign = match &literal.sign {
            Sign::Plus => "+",
            Sign::Minus => "-",
        };
        Self::parse_primitive::<f64>(format!("{}{}", sign, literal.decimal).as_str())
    }
}

impl FromTypeQLLiteral for Decimal {
    type TypeQLLiteral = SignedDecimalLiteral;

    fn from_typeql_literal(literal: &Self::TypeQLLiteral) -> Result<Self, LiteralParseError> {
        if literal.decimal.contains("e") {
            Err(LiteralParseError::ScientificNotationNotAllowedForDecimal { literal: literal.to_string() })?;
        }
        let (integral_str, fractional_str) = literal.decimal.split_once(".").unwrap();
        let integral = Self::parse_primitive::<i64>(integral_str)?;
        let number_len = fractional_str.len();
        let fractional = Self::parse_primitive::<u64>(fractional_str)?
            * 10u64.pow(FRACTIONAL_PART_DENOMINATOR_LOG10 - number_len as u32);

        Ok(match literal.sign {
            Sign::Plus => Decimal::new(integral, fractional),
            Sign::Minus => Decimal::new(0, 0) - Decimal::new(integral, fractional),
        })
    }
}

impl FromTypeQLLiteral for NaiveDate {
    type TypeQLLiteral = typeql::value::DateFragment;

    fn from_typeql_literal(literal: &Self::TypeQLLiteral) -> Result<Self, LiteralParseError> {
        let (year, month, day) = (
            Self::parse_primitive(literal.year.as_str())?,
            Self::parse_primitive(literal.month.as_str())?,
            Self::parse_primitive(literal.day.as_str())?,
        );
        NaiveDate::from_ymd_opt(year, month, day)
            .map_or_else(|| Err(LiteralParseError::InvalidDate { year, month, day }), |date| Ok(date))
    }
}

impl FromTypeQLLiteral for NaiveTime {
    type TypeQLLiteral = typeql::value::TimeFragment;

    fn from_typeql_literal(literal: &Self::TypeQLLiteral) -> Result<Self, LiteralParseError> {
        let hour = Self::parse_primitive(literal.hour.as_str())?;
        let minute = Self::parse_primitive(literal.minute.as_str())?;
        let second = if let Some(s) = &literal.second { Self::parse_primitive(s.as_str())? } else { 0 };
        let nano = if let Some(s) = &literal.second {
            let number_len = s.len();
            Self::parse_primitive::<u32>(s.as_str())? * 10u32.pow(9 - number_len as u32)
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
            TimeZone::IANA(first, second) => todo!(),
            TimeZone::ISO(iso) => todo!(),
        }
    }
}

impl FromTypeQLLiteral for NaiveDateTime {
    type TypeQLLiteral = typeql::value::DateTimeLiteral;

    fn from_typeql_literal(literal: &Self::TypeQLLiteral) -> Result<Self, LiteralParseError> {
        let date = NaiveDate::from_typeql_literal(&literal.date)?;
        let time = NaiveTime::from_typeql_literal(&literal.time)?;
        Ok(NaiveDateTime::new(date, time))
    }
}

impl FromTypeQLLiteral for chrono::DateTime<chrono_tz::Tz> {
    type TypeQLLiteral = DateTimeTZLiteral;

    fn from_typeql_literal(literal: &Self::TypeQLLiteral) -> Result<Self, LiteralParseError> {
        let date = NaiveDate::from_typeql_literal(&literal.date)?;
        let time = NaiveTime::from_typeql_literal(&literal.time)?;
        let tz: chrono_tz::Tz = todo!();
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
        pattern::expression::Expression, program::function_signature::HashMapFunctionIndex,
        translator::block_builder::TypeQLBuilder, LiteralParseError, PatternDefinitionError,
    };

    fn parse_value_via_typeql_expression(s: &str) -> Result<Value<'static>, PatternDefinitionError> {
        let query = format!("match $x = {}; filter $x;", s);
        if let Stage::Match(match_) =
            typeql::parse_query(query.as_str()).unwrap().into_pipeline().stages.get(0).unwrap()
        {
            let block = TypeQLBuilder::build_match(&HashMapFunctionIndex::empty(), &match_)?;
            let x = &block.conjunction().constraints()[0].as_expression_binding().unwrap().expression.tree()[0];
            match x {
                Expression::Constant(constant) => Ok(constant.clone()),
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
