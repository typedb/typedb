/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    fmt,
    ops::{Add, Sub},
};

use chrono::{Days, Months, NaiveDateTime, TimeDelta};

const NANOS_PER_SEC: u32 = 1_000_000_000;
const SECS_PER_HOUR: u32 = 60 * 60;
const SECS_PER_MINUTE: u32 = 60;

const MAX_YEAR: i32 = (i32::MAX >> 13) - 1; // NaiveDate.year() is from a trait and not `const`
const MIN_YEAR: i32 = (i32::MIN >> 13) + 1; // NaiveDate.year() is from a trait and not `const`

const MONTHS_PER_YEAR: u32 = 12;
const MAX_MONTHS: u32 = (MAX_YEAR - MIN_YEAR + 1) as u32 * MONTHS_PER_YEAR;

const MAX_HOURS: u32 = 1_000_000;
const MAX_SECONDS: u32 = MAX_HOURS * SECS_PER_HOUR;

#[derive(Clone, Copy, Debug)]
pub struct Duration {
    pub(super) months: u32,
    pub(super) days: u32,
    pub(super) seconds: u32,
    pub(super) nanos: u32,
}

impl Duration {
    fn new(months: u32, days: u32, seconds: u32, nanos: u32) -> Self {
        assert!(months <= MAX_MONTHS);
        // assert!(days <= TODO);
        assert!(seconds <= MAX_SECONDS);
        assert!(nanos <= NANOS_PER_SEC);
        Self { months, days, seconds, nanos }
    }

    fn is_empty(&self) -> bool {
        self.months == 0 && self.days == 0 && self.seconds == 0 && self.nanos == 0
    }
}

// Equivalent to derive(PartialEq), but spelled out to be clear this is the intended behaviour
impl PartialEq for Duration {
    fn eq(&self, other: &Self) -> bool {
        (self.months, self.days, self.seconds, self.nanos) == (other.months, other.days, other.seconds, other.nanos)
    }
}

impl Add for Duration {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        let lhs = self;
        Self::new(lhs.months + rhs.months, lhs.days + rhs.days, lhs.seconds + rhs.seconds, lhs.nanos + rhs.nanos)
    }
}

impl Add<Duration> for NaiveDateTime {
    type Output = NaiveDateTime;

    fn add(self, rhs: Duration) -> Self::Output {
        self + Months::new(rhs.months)
            + Days::new(rhs.days as u64)
            + TimeDelta::new(rhs.seconds as i64, rhs.nanos).unwrap()
    }
}

impl Sub for Duration {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self::Output {
        let lhs = self;
        Self::new(lhs.months - rhs.months, lhs.days - rhs.days, lhs.seconds - rhs.seconds, lhs.nanos - rhs.nanos)
    }
}

impl Sub<Duration> for NaiveDateTime {
    type Output = NaiveDateTime;

    fn sub(self, rhs: Duration) -> Self::Output {
        self - Months::new(rhs.months)
            - Days::new(rhs.days as u64)
            - TimeDelta::new(rhs.seconds as i64, rhs.nanos).unwrap()
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

        if self.seconds > 0 || self.nanos > 0 {
            let hours = self.seconds / SECS_PER_HOUR;
            let minutes = (self.seconds % SECS_PER_HOUR) / SECS_PER_MINUTE;
            let seconds = self.seconds % SECS_PER_MINUTE;
            let nanos = self.nanos;
            write!(f, "T")?;
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
