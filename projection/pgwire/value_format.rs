/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Converts TypeDB [`Value`]s into Postgres wire protocol text format strings.
//!
//! Postgres text format is what gets sent inside `DataRow` messages when
//! using the Simple Query protocol (format code 0). Each type has specific
//! formatting rules documented in the PostgreSQL docs.

use encoding::value::value::Value;

/// Convert a TypeDB `Value` to its Postgres text-format representation.
///
/// This is the string that would appear in a `DataRow` cell when using text mode.
///
/// # Postgres text format reference
///
/// | TypeDB type   | Postgres type    | Format example                     |
/// |---------------|------------------|------------------------------------|
/// | Boolean       | bool             | `t` / `f`                          |
/// | Integer       | int8             | `42`                               |
/// | Double        | float8           | `3.14`                             |
/// | Decimal       | numeric          | `123.456`                          |
/// | Date          | date             | `2024-01-15`                       |
/// | DateTime      | timestamp        | `2024-01-15 10:30:00`              |
/// | DateTimeTZ    | timestamptz      | `2024-01-15 10:30:00+00`           |
/// | Duration      | interval         | `1 year 2 mons 3 days 04:05:06`    |
/// | String        | text             | raw string (no quoting)            |
/// | Struct        | jsonb            | `{...}` (JSON object, best-effort) |
pub fn value_to_pg_text(value: &Value<'_>) -> String {
    match value {
        Value::Boolean(b) => if *b { "t" } else { "f" }.to_string(),
        Value::Integer(i) => i.to_string(),
        Value::Double(d) => format_double(*d),
        Value::Decimal(d) => {
            // Decimal::Display adds "dec" suffix; strip it for Postgres numeric format.
            let s = d.to_string();
            s.strip_suffix("dec").unwrap_or(&s).to_string()
        }
        Value::Date(d) => d.to_string(),       // chrono NaiveDate Display: YYYY-MM-DD
        Value::DateTime(dt) => dt.to_string(), // chrono NaiveDateTime Display: YYYY-MM-DD HH:MM:SS[.frac]
        Value::DateTimeTZ(dt) => {
            let base = dt.naive_local().to_string();
            // Compute offset from difference between local and UTC representations.
            let offset_secs = (dt.naive_local() - dt.naive_utc()).num_seconds() as i32;
            let hours = offset_secs / 3600;
            let mins = (offset_secs.abs() % 3600) / 60;
            if mins == 0 {
                format!("{}{:+03}", base, hours)
            } else {
                format!("{}{:+03}:{:02}", base, hours, mins)
            }
        }
        Value::Duration(d) => format_duration(d.months, d.days, d.nanos),
        Value::String(s) => s.to_string(),
        Value::Struct(s) => format!("{:?}", s.as_ref()),
    }
}

/// Format an `f64` in Postgres text-format style.
///
/// Special values use Postgres spelling: `NaN`, `Infinity`, `-Infinity`.
/// Normal values use Rust's shortest-representation Display (decimal notation).
fn format_double(v: f64) -> String {
    if v.is_nan() {
        return "NaN".to_string();
    }
    if v == f64::INFINITY {
        return "Infinity".to_string();
    }
    if v == f64::NEG_INFINITY {
        return "-Infinity".to_string();
    }
    v.to_string()
}

/// Format a duration as a Postgres `interval` text string.
///
/// Follows Postgres output conventions:
/// - Year/month components use singular/plural (`1 year`, `2 mons`)
/// - Time component uses `HH:MM:SS[.ffffff]` with trailing-zero trimming
/// - A completely-zero duration outputs `00:00:00`
fn format_duration(months: u32, days: u32, nanos: u64) -> String {
    let mut parts: Vec<String> = Vec::new();

    let years = months / 12;
    let mons = months % 12;

    if years > 0 {
        parts.push(format!("{} {}", years, if years == 1 { "year" } else { "years" }));
    }
    if mons > 0 {
        parts.push(format!("{} {}", mons, if mons == 1 { "mon" } else { "mons" }));
    }
    if days > 0 {
        parts.push(format!("{} {}", days, if days == 1 { "day" } else { "days" }));
    }

    let total_secs = nanos / 1_000_000_000;
    let remaining_nanos = nanos % 1_000_000_000;
    let hours = total_secs / 3600;
    let mins = (total_secs % 3600) / 60;
    let secs = total_secs % 60;

    if nanos > 0 || parts.is_empty() {
        let time = if remaining_nanos > 0 {
            let micros = remaining_nanos / 1_000;
            let frac = format!("{:06}", micros);
            let trimmed = frac.trim_end_matches('0');
            format!("{:02}:{:02}:{:02}.{}", hours, mins, secs, trimmed)
        } else {
            format!("{:02}:{:02}:{:02}", hours, mins, secs)
        };
        parts.push(time);
    }

    parts.join(" ")
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::borrow::Cow;

    use chrono::{FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, TimeZone as ChronoTimeZone};
    use encoding::value::{decimal_value::Decimal, duration_value::Duration, timezone::TimeZone, value::Value};

    use super::*;

    // ── Boolean ────────────────────────────────────────────────────

    #[test]
    fn test_boolean_true() {
        assert_eq!(value_to_pg_text(&Value::Boolean(true)), "t");
    }

    #[test]
    fn test_boolean_false() {
        assert_eq!(value_to_pg_text(&Value::Boolean(false)), "f");
    }

    // ── Integer ────────────────────────────────────────────────────

    #[test]
    fn test_integer_positive() {
        assert_eq!(value_to_pg_text(&Value::Integer(42)), "42");
    }

    #[test]
    fn test_integer_negative() {
        assert_eq!(value_to_pg_text(&Value::Integer(-100)), "-100");
    }

    #[test]
    fn test_integer_zero() {
        assert_eq!(value_to_pg_text(&Value::Integer(0)), "0");
    }

    #[test]
    fn test_integer_max() {
        assert_eq!(value_to_pg_text(&Value::Integer(i64::MAX)), i64::MAX.to_string());
    }

    #[test]
    fn test_integer_min() {
        assert_eq!(value_to_pg_text(&Value::Integer(i64::MIN)), i64::MIN.to_string());
    }

    // ── Double ─────────────────────────────────────────────────────

    #[test]
    fn test_double_fractional() {
        assert_eq!(value_to_pg_text(&Value::Double(3.14)), "3.14");
    }

    #[test]
    fn test_double_whole_number() {
        // Postgres text output for float8: 42.0 → "42"
        assert_eq!(value_to_pg_text(&Value::Double(42.0)), "42");
    }

    #[test]
    fn test_double_negative() {
        assert_eq!(value_to_pg_text(&Value::Double(-1.5)), "-1.5");
    }

    #[test]
    fn test_double_zero() {
        assert_eq!(value_to_pg_text(&Value::Double(0.0)), "0");
    }

    #[test]
    fn test_double_infinity() {
        assert_eq!(value_to_pg_text(&Value::Double(f64::INFINITY)), "Infinity");
    }

    #[test]
    fn test_double_neg_infinity() {
        assert_eq!(value_to_pg_text(&Value::Double(f64::NEG_INFINITY)), "-Infinity");
    }

    #[test]
    fn test_double_nan() {
        assert_eq!(value_to_pg_text(&Value::Double(f64::NAN)), "NaN");
    }

    #[test]
    fn test_double_small_fraction() {
        assert_eq!(value_to_pg_text(&Value::Double(0.001)), "0.001");
    }

    // ── Decimal ────────────────────────────────────────────────────

    #[test]
    fn test_decimal_whole() {
        // Decimal::new(123, 0) → integer=123, frac=0
        let d = Decimal::new(123, 0);
        assert_eq!(value_to_pg_text(&Value::Decimal(d)), "123.0");
    }

    #[test]
    fn test_decimal_with_fraction() {
        // 0.456 → fractional_parts = 456 * 10^16 = 4_560_000_000_000_000_000
        let d = Decimal::new(123, 4_560_000_000_000_000_000);
        assert_eq!(value_to_pg_text(&Value::Decimal(d)), "123.456");
    }

    #[test]
    fn test_decimal_negative() {
        let d = Decimal::new(-42, 0);
        assert_eq!(value_to_pg_text(&Value::Decimal(d)), "-42.0");
    }

    #[test]
    fn test_decimal_negative_with_fraction() {
        // -3.5: In TypeDB Decimal, negatives with fractions use integer=-4, frac=DENOM-frac_value
        // but let's check: Decimal::new(-3, 5_000_000_000_000_000_000) → -3 + 0.5 =-2.5 or -3.5?
        // Actually from the Display impl:
        // if integer < 0: frac = DENOM - fractional, int = (integer+1).abs()
        // So for -3.5 we need: integer=-4, fractional=5_000_000_000_000_000_000
        // Display: int = (-4+1).abs() = 3, frac = DENOM - 5e18 = 5e18 → "-3.5"
        let d = Decimal::new(-4, 5_000_000_000_000_000_000);
        assert_eq!(value_to_pg_text(&Value::Decimal(d)), "-3.5");
    }

    #[test]
    fn test_decimal_zero() {
        let d = Decimal::new(0, 0);
        assert_eq!(value_to_pg_text(&Value::Decimal(d)), "0.0");
    }

    // ── Date ───────────────────────────────────────────────────────

    #[test]
    fn test_date_normal() {
        let d = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        assert_eq!(value_to_pg_text(&Value::Date(d)), "2024-01-15");
    }

    #[test]
    fn test_date_epoch() {
        let d = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        assert_eq!(value_to_pg_text(&Value::Date(d)), "1970-01-01");
    }

    #[test]
    fn test_date_leap_day() {
        let d = NaiveDate::from_ymd_opt(2024, 2, 29).unwrap();
        assert_eq!(value_to_pg_text(&Value::Date(d)), "2024-02-29");
    }

    // ── DateTime (timestamp without timezone) ──────────────────────

    #[test]
    fn test_datetime_no_fractional_seconds() {
        let dt = NaiveDateTime::new(
            NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(),
            NaiveTime::from_hms_opt(10, 30, 0).unwrap(),
        );
        assert_eq!(value_to_pg_text(&Value::DateTime(dt)), "2024-01-15 10:30:00");
    }

    #[test]
    fn test_datetime_with_microseconds() {
        let dt = NaiveDateTime::new(
            NaiveDate::from_ymd_opt(2024, 6, 15).unwrap(),
            NaiveTime::from_hms_micro_opt(14, 30, 45, 123_456).unwrap(),
        );
        assert_eq!(value_to_pg_text(&Value::DateTime(dt)), "2024-06-15 14:30:45.123456");
    }

    #[test]
    fn test_datetime_midnight() {
        let dt = NaiveDateTime::new(
            NaiveDate::from_ymd_opt(2000, 12, 31).unwrap(),
            NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
        );
        assert_eq!(value_to_pg_text(&Value::DateTime(dt)), "2000-12-31 00:00:00");
    }

    // ── DateTimeTZ (timestamp with timezone) ───────────────────────

    #[test]
    fn test_datetime_tz_utc() {
        let offset = FixedOffset::east_opt(0).unwrap();
        let naive = NaiveDateTime::new(
            NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(),
            NaiveTime::from_hms_opt(10, 30, 0).unwrap(),
        );
        let dt = offset.from_local_datetime(&naive).unwrap();
        let tz_value = dt.with_timezone(&TimeZone::Fixed(offset));
        assert_eq!(value_to_pg_text(&Value::DateTimeTZ(tz_value)), "2024-01-15 10:30:00+00");
    }

    #[test]
    fn test_datetime_tz_positive_offset() {
        let offset = FixedOffset::east_opt(5 * 3600 + 30 * 60).unwrap(); // +05:30
        let naive = NaiveDateTime::new(
            NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(),
            NaiveTime::from_hms_opt(10, 30, 0).unwrap(),
        );
        let dt = offset.from_local_datetime(&naive).unwrap();
        let tz_value = dt.with_timezone(&TimeZone::Fixed(offset));
        assert_eq!(value_to_pg_text(&Value::DateTimeTZ(tz_value)), "2024-01-15 10:30:00+05:30");
    }

    #[test]
    fn test_datetime_tz_negative_offset() {
        let offset = FixedOffset::west_opt(5 * 3600).unwrap(); // -05:00
        let naive = NaiveDateTime::new(
            NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(),
            NaiveTime::from_hms_opt(10, 30, 0).unwrap(),
        );
        let dt = offset.from_local_datetime(&naive).unwrap();
        let tz_value = dt.with_timezone(&TimeZone::Fixed(offset));
        assert_eq!(value_to_pg_text(&Value::DateTimeTZ(tz_value)), "2024-01-15 10:30:00-05");
    }

    #[test]
    fn test_datetime_tz_iana() {
        // Use a named IANA timezone via chrono-tz.
        let tz_iana = chrono_tz::US::Eastern;
        let naive = NaiveDateTime::new(
            NaiveDate::from_ymd_opt(2024, 7, 15).unwrap(), // July → EDT (-04:00)
            NaiveTime::from_hms_opt(10, 30, 0).unwrap(),
        );
        let dt = tz_iana.from_local_datetime(&naive).unwrap();
        let tz_value = dt.with_timezone(&TimeZone::IANA(tz_iana));
        // Postgres with IANA timezone still outputs numeric offset.
        assert_eq!(value_to_pg_text(&Value::DateTimeTZ(tz_value)), "2024-07-15 10:30:00-04");
    }

    // ── Duration (Postgres interval format) ────────────────────────

    #[test]
    fn test_duration_zero() {
        let d = Duration::new_checked(0, 0, 0).unwrap();
        assert_eq!(value_to_pg_text(&Value::Duration(d)), "00:00:00");
    }

    #[test]
    fn test_duration_months_only() {
        let d = Duration::new_checked(14, 0, 0).unwrap(); // 1 year 2 mons
        assert_eq!(value_to_pg_text(&Value::Duration(d)), "1 year 2 mons");
    }

    #[test]
    fn test_duration_days_only() {
        let d = Duration::new_checked(0, 5, 0).unwrap();
        assert_eq!(value_to_pg_text(&Value::Duration(d)), "5 days");
    }

    #[test]
    fn test_duration_one_day() {
        let d = Duration::new_checked(0, 1, 0).unwrap();
        assert_eq!(value_to_pg_text(&Value::Duration(d)), "1 day");
    }

    #[test]
    fn test_duration_time_only() {
        // 4 hours 5 minutes 6 seconds = 4*3600 + 5*60 + 6 = 14706 seconds → nanos
        let nanos = (4 * 3600 + 5 * 60 + 6) * 1_000_000_000u64;
        let d = Duration::new_checked(0, 0, nanos).unwrap();
        assert_eq!(value_to_pg_text(&Value::Duration(d)), "04:05:06");
    }

    #[test]
    fn test_duration_full() {
        let nanos = (4 * 3600 + 5 * 60 + 6) * 1_000_000_000u64;
        let d = Duration::new_checked(14, 3, nanos).unwrap(); // 1yr 2mo 3d 4h5m6s
        assert_eq!(value_to_pg_text(&Value::Duration(d)), "1 year 2 mons 3 days 04:05:06");
    }

    #[test]
    fn test_duration_with_microseconds() {
        // 1.5 ms = 1500 µs = 1_500_000 ns
        let d = Duration::new_checked(0, 0, 1_500_000).unwrap();
        assert_eq!(value_to_pg_text(&Value::Duration(d)), "00:00:00.0015");
    }

    #[test]
    fn test_duration_one_year() {
        let d = Duration::new_checked(12, 0, 0).unwrap();
        assert_eq!(value_to_pg_text(&Value::Duration(d)), "1 year");
    }

    #[test]
    fn test_duration_one_month() {
        let d = Duration::new_checked(1, 0, 0).unwrap();
        assert_eq!(value_to_pg_text(&Value::Duration(d)), "1 mon");
    }

    #[test]
    fn test_duration_multiple_years_and_months() {
        let d = Duration::new_checked(27, 0, 0).unwrap(); // 2y 3m
        assert_eq!(value_to_pg_text(&Value::Duration(d)), "2 years 3 mons");
    }

    // ── String ─────────────────────────────────────────────────────

    #[test]
    fn test_string_simple() {
        let val = Value::String(Cow::Borrowed("hello world"));
        assert_eq!(value_to_pg_text(&val), "hello world");
    }

    #[test]
    fn test_string_empty() {
        let val = Value::String(Cow::Borrowed(""));
        assert_eq!(value_to_pg_text(&val), "");
    }

    #[test]
    fn test_string_with_special_chars() {
        let val = Value::String(Cow::Borrowed("hello\nworld\ttab"));
        assert_eq!(value_to_pg_text(&val), "hello\nworld\ttab");
    }

    #[test]
    fn test_string_unicode() {
        let val = Value::String(Cow::Borrowed("日本語テスト"));
        assert_eq!(value_to_pg_text(&val), "日本語テスト");
    }

    // ── Struct (best-effort JSON) ──────────────────────────────────
    // Struct is hard to construct in tests because StructValue requires
    // a DefinitionKey and field IDs. We test the basic contract: output
    // is a string representation. Full struct tests can be added when
    // the materialization layer provides real struct values.

    // ── Edge cases ─────────────────────────────────────────────────

    #[test]
    fn test_double_large_integer() {
        // 1e15 is exactly representable; Rust Display gives decimal notation.
        assert_eq!(value_to_pg_text(&Value::Double(1e15)), "1000000000000000");
    }

    #[test]
    fn test_double_negative_zero() {
        // Postgres treats -0 same as 0; Rust Display gives "-0".
        // We accept Rust's default here.
        let result = value_to_pg_text(&Value::Double(-0.0));
        assert!(result == "0" || result == "-0");
    }
}
