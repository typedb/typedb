/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    fmt,
    ops::{Add, Sub},
    str::FromStr,
};

use chrono::{DateTime, Days, Months, NaiveDateTime, TimeDelta, TimeZone};

const NANOS_PER_SEC: u64 = 1_000_000_000;
const NANOS_PER_MINUTE: u64 = 60 * NANOS_PER_SEC;
const NANOS_PER_HOUR: u64 = 60 * 60 * NANOS_PER_SEC;

const MAX_YEAR: i32 = (i32::MAX >> 13) - 1; // NaiveDate.year() is from a trait and not `const`
const MIN_YEAR: i32 = (i32::MIN >> 13) + 1; // NaiveDate.year() is from a trait and not `const`

const MONTHS_PER_YEAR: u32 = 12;
const MAX_MONTHS: u32 = (MAX_YEAR - MIN_YEAR + 1) as u32 * MONTHS_PER_YEAR;

const DAYS_PER_WEEK: u32 = 7;

#[derive(Clone, Copy, Debug)]
pub struct Duration {
    pub(super) months: u32,
    pub(super) days: u32,
    pub(super) nanos: u64,
}

impl Duration {
    fn new(months: u32, days: u32, nanos: u64) -> Self {
        assert!(months <= MAX_MONTHS);
        Self { months, days, nanos }
    }

    fn is_empty(&self) -> bool {
        self.months == 0 && self.days == 0 && self.nanos == 0
    }

    pub fn years(years: u32) -> Self {
        Self { months: years * MONTHS_PER_YEAR, days: 0, nanos: 0 }
    }

    pub fn months(months: u32) -> Self {
        Self { months, days: 0, nanos: 0 }
    }

    pub fn weeks(weeks: u32) -> Self {
        Self { months: 0, days: weeks * DAYS_PER_WEEK, nanos: 0 }
    }

    pub fn days(days: u32) -> Self {
        Self { months: 0, days, nanos: 0 }
    }

    pub fn hours(hours: u64) -> Self {
        Self { months: 0, days: 0, nanos: hours * NANOS_PER_HOUR }
    }

    pub fn minutes(minutes: u64) -> Self {
        Self { months: 0, days: 0, nanos: minutes * NANOS_PER_MINUTE }
    }

    pub fn seconds(seconds: u64) -> Self {
        Self { months: 0, days: 0, nanos: seconds * NANOS_PER_SEC }
    }
}

// Equivalent to derive(PartialEq), but spelled out to be clear this is the intended behaviour
impl PartialEq for Duration {
    fn eq(&self, other: &Self) -> bool {
        (self.months, self.days, self.nanos) == (other.months, other.days, other.nanos)
    }
}

impl Add for Duration {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        let lhs = self;
        Self::new(lhs.months + rhs.months, lhs.days + rhs.days, lhs.nanos + rhs.nanos)
    }
}

impl Add<Duration> for NaiveDateTime {
    type Output = Self;

    fn add(self, rhs: Duration) -> Self::Output {
        self + Months::new(rhs.months)
            + Days::new(rhs.days as u64)
            + TimeDelta::new((rhs.nanos / NANOS_PER_SEC) as i64, (rhs.nanos % NANOS_PER_SEC) as u32).unwrap()
    }
}

impl<Tz: TimeZone> Add<Duration> for DateTime<Tz> {
    type Output = Self;

    fn add(self, rhs: Duration) -> Self::Output {
        self + Months::new(rhs.months)
            + Days::new(rhs.days as u64)
            + TimeDelta::new((rhs.nanos / NANOS_PER_SEC) as i64, (rhs.nanos % NANOS_PER_SEC) as u32).unwrap()
    }
}

impl Sub for Duration {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self::Output {
        let lhs = self;
        Self::new(lhs.months - rhs.months, lhs.days - rhs.days, lhs.nanos - rhs.nanos)
    }
}

impl Sub<Duration> for NaiveDateTime {
    type Output = Self;

    fn sub(self, rhs: Duration) -> Self::Output {
        self - Months::new(rhs.months)
            - Days::new(rhs.days as u64)
            - TimeDelta::new((rhs.nanos / NANOS_PER_SEC) as i64, (rhs.nanos % NANOS_PER_SEC) as u32).unwrap()
    }
}

impl<Tz: TimeZone> Sub<Duration> for DateTime<Tz> {
    type Output = Self;

    fn sub(self, rhs: Duration) -> Self::Output {
        self - Months::new(rhs.months)
            - Days::new(rhs.days as u64)
            - TimeDelta::new((rhs.nanos / NANOS_PER_SEC) as i64, (rhs.nanos % NANOS_PER_SEC) as u32).unwrap()
    }
}

#[derive(Debug)]
pub struct DurationParseError;

struct Segment {
    number: u64,
    symbol: u8,
    number_len: usize,
}

fn read_u32(str: &str) -> Result<(Segment, &str), DurationParseError> {
    let mut i = 0;
    while i + 1 < str.len() && str.as_bytes()[i].is_ascii_digit() {
        i += 1;
    }
    if i == 0 {
        return Err(DurationParseError);
    }
    let value = str[..i].parse().map_err(|_| DurationParseError)?;
    Ok((Segment { number: value, symbol: str.as_bytes()[i], number_len: i }, &str[i + 1..]))
}

impl FromStr for Duration {
    type Err = DurationParseError;

    fn from_str(mut str: &str) -> Result<Self, Self::Err> {
        let mut months = 0;
        let mut days = 0;
        let mut nanos = 0;

        if str.as_bytes()[0] != b'P' {
            return Err(DurationParseError);
        }
        str = &str[1..];

        let mut parsing_time = false;
        let mut previous_symbol = None;
        while !str.is_empty() {
            if str.as_bytes()[0] == b'T' {
                parsing_time = true;
                str = &str[1..];
            }

            let (Segment { number, symbol, number_len }, tail) = read_u32(str)?;
            str = tail;

            if previous_symbol == Some(b'.') && symbol != b'S' {
                return Err(DurationParseError);
            }

            match symbol {
                b'Y' => months += number as u32 * MONTHS_PER_YEAR,
                b'M' if !parsing_time => months += number as u32,

                b'W' => days += number as u32 * DAYS_PER_WEEK,
                b'D' => days += number as u32,

                b'H' => nanos += number * NANOS_PER_HOUR,
                b'M' if parsing_time => nanos += number * NANOS_PER_MINUTE,
                b'.' => nanos += number * NANOS_PER_SEC,
                b'S' if previous_symbol != Some(b'.') => nanos += number * NANOS_PER_SEC,
                b'S' if previous_symbol == Some(b'.') => nanos += number * 10u64.pow(9 - number_len as u32),
                _ => return Err(DurationParseError),
            }
            previous_symbol = Some(symbol);
        }

        Ok(Self { months, days, nanos })
    }
}

/// ISO-8601 compliant representation of a duration
impl fmt::Display for Duration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_empty() {
            f.write_str("PT0S")?;
            return Ok(());
        }

        write!(f, "P")?;

        if self.months > 0 || self.days > 0 {
            let years = self.months / MONTHS_PER_YEAR;
            let months = self.months % MONTHS_PER_YEAR;
            let days = self.days;
            if years > 0 {
                write!(f, "{years}Y")?;
            }
            if months > 0 {
                write!(f, "{months}M")?;
            }
            if days > 0 {
                write!(f, "{days}D")?;
            }
        }

        if self.nanos > 0 {
            write!(f, "T")?;

            let hours = self.nanos / NANOS_PER_HOUR;
            let minutes = (self.nanos % NANOS_PER_HOUR) / NANOS_PER_MINUTE;
            let seconds = (self.nanos % NANOS_PER_MINUTE) / NANOS_PER_SEC;
            let nanos = self.nanos % NANOS_PER_SEC;

            if hours > 0 {
                write!(f, "{hours}H")?;
            }
            if minutes > 0 {
                write!(f, "{minutes}M")?;
            }
            if seconds > 0 && nanos == 0 {
                write!(f, "{seconds}S")?;
            } else if nanos > 0 {
                write!(f, "{seconds}.{nanos:09}S")?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #![allow(non_snake_case)]
    #![allow(clippy::just_underscores_and_digits)]

    use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
    use chrono_tz::Europe::London;

    use super::Duration;

    #[test]
    fn adding_one_day_ignores_dst() {
        // London DST change occurred on 2024-03-31 01:00:00 GMT
        let _2024_03_30__12_00_00 = NaiveDateTime::new(
            NaiveDate::from_ymd_opt(2024, 3, 30).unwrap(),
            NaiveTime::from_hms_opt(12, 0, 0).unwrap(),
        )
        .and_local_timezone(London)
        .unwrap();

        let _2024_03_31__12_00_00 = NaiveDateTime::new(
            NaiveDate::from_ymd_opt(2024, 3, 31).unwrap(),
            NaiveTime::from_hms_opt(12, 0, 0).unwrap(),
        )
        .and_local_timezone(London)
        .unwrap();

        let p1d = Duration::days(1);

        assert_eq!(_2024_03_30__12_00_00 + p1d, _2024_03_31__12_00_00)
    }

    #[test]
    fn adding_24_hours_respects_dst() {
        // London DST change occurred on 2024-03-31 01:00:00 GMT
        let _2024_03_30__12_00_00 = NaiveDateTime::new(
            NaiveDate::from_ymd_opt(2024, 3, 30).unwrap(),
            NaiveTime::from_hms_opt(12, 0, 0).unwrap(),
        )
        .and_local_timezone(London)
        .unwrap();

        let _2024_03_31__13_00_00 = NaiveDateTime::new(
            NaiveDate::from_ymd_opt(2024, 3, 31).unwrap(),
            NaiveTime::from_hms_opt(13, 0, 0).unwrap(),
        )
        .and_local_timezone(London)
        .unwrap();

        let pt24h = Duration::hours(24);

        assert_eq!(_2024_03_30__12_00_00 + pt24h, _2024_03_31__13_00_00)
    }
}
