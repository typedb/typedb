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

use chrono::{
    DateTime, Datelike, Days, MappedLocalTime, Months, NaiveDate, NaiveDateTime, Offset, TimeDelta, TimeZone,
};

pub const NANOS_PER_SEC: u64 = 1_000_000_000;
pub const NANOS_PER_MINUTE: u64 = 60 * NANOS_PER_SEC;
pub const NANOS_PER_HOUR: u64 = 60 * 60 * NANOS_PER_SEC;
pub const NANOS_PER_NAIVE_DAY: u64 = 24 * NANOS_PER_HOUR;

const MAX_YEAR: i32 = (i32::MAX >> 13) - 1; // NaiveDate.year() is from a trait and not `const`
const MIN_YEAR: i32 = (i32::MIN >> 13) + 1; // NaiveDate.year() is from a trait and not `const`

pub const MONTHS_PER_YEAR: u32 = 12;
const MAX_MONTHS: u32 = (MAX_YEAR - MIN_YEAR + 1) as u32 * MONTHS_PER_YEAR;

pub const DAYS_PER_WEEK: u32 = 7;

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub struct Duration {
    pub months: u32,
    pub days: u32,
    pub nanos: u64,
}

impl Duration {
    pub fn new(months: u32, days: u32, nanos: u64) -> Self {
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

    pub fn nanos(nanos: u64) -> Self {
        Self { months: 0, days: 0, nanos }
    }

    pub fn between_dates(earlier: NaiveDate, later: NaiveDate) -> Self {
        debug_assert!(earlier <= later, "attempting to subtract with underflow");
        let mut months = (later.year() - earlier.year()) as u32 * MONTHS_PER_YEAR + later.month() - earlier.month();

        let adjusted_earlier = earlier + Months::new(months);
        let days = if later.day() < adjusted_earlier.day() {
            months -= 1;
            let adjusted_earlier = earlier + Months::new(months);
            later.day() + adjusted_earlier.num_days_in_month() as u32 - adjusted_earlier.day()
        } else {
            later.day() - adjusted_earlier.day()
        };

        Self { months, days, nanos: 0 }
    }

    pub fn between_datetimes(earlier: NaiveDateTime, later: NaiveDateTime) -> Self {
        debug_assert!(earlier <= later, "attempting to subtract with underflow");
        let date_duration = if later.time() >= earlier.time() {
            Self::between_dates(earlier.date(), later.date())
        } else {
            Self::between_dates(
                earlier.date(),
                later.date().pred_opt().expect(
                    "later datetime has earlier time of day, it must have date later the earlier datetime's date",
                ),
            )
        };
        let adjusted_earlier = earlier + date_duration;
        let nanos = (later - adjusted_earlier).num_nanoseconds().expect("time difference < 1 day cannot overflow");
        date_duration + Self::nanos(nanos as u64)
    }

    pub fn between_datetimes_tz<Tz: TimeZone>(earlier: DateTime<Tz>, later: DateTime<Tz>) -> Self {
        debug_assert!(earlier <= later, "attempting to subtract with underflow");
        let later = later.with_timezone(&earlier.timezone());
        let date_duration = if later.time() >= earlier.time() {
            Self::between_dates(earlier.date_naive(), later.date_naive())
        } else {
            let day_before_later = {
                let mut days = 1;
                loop {
                    if let Some(date) = later.clone().checked_sub_days(Days::new(days)) {
                        break date;
                    }
                    days += 1;
                }
            };
            Self::between_dates(earlier.date_naive(), day_before_later.date_naive())
        };
        let adjusted_earlier = earlier + date_duration;
        let nanos = (later - adjusted_earlier).num_nanoseconds().expect("time difference < 1 day cannot overflow");
        date_duration + Self::nanos(nanos as u64)
    }

    pub fn checked_add(self, rhs: Self) -> Option<Self> {
        let Self { months, days, nanos } = self;
        let Self { months: rhs_months, days: rhs_days, nanos: rhs_nanos } = rhs;
        Some(Self::new(months.checked_add(rhs_months)?, days.checked_add(rhs_days)?, nanos.checked_add(rhs_nanos)?))
    }

    pub fn checked_sub(self, rhs: Self) -> Option<Self> {
        let Self { months, days, nanos } = self;
        let Self { months: rhs_months, days: rhs_days, nanos: rhs_nanos } = rhs;
        if months >= rhs_months && days >= rhs_days && nanos >= rhs_nanos {
            Some(Self::new(months - rhs_months, days - rhs_days, nanos - rhs_nanos))
        } else {
            None
        }
    }

    fn time_as_timedelta(&self) -> TimeDelta {
        TimeDelta::new((self.nanos / NANOS_PER_SEC) as i64, (self.nanos % NANOS_PER_SEC) as u32).unwrap()
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
        self + Months::new(rhs.months) + Days::new(rhs.days as u64) + rhs.time_as_timedelta()
    }
}

impl<Tz: TimeZone> Add<Duration> for DateTime<Tz> {
    type Output = Self;

    fn add(self, rhs: Duration) -> Self::Output {
        resolve_date_time(self.naive_local() + Months::new(rhs.months) + Days::new(rhs.days as u64), self.timezone())
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
        self - Months::new(rhs.months) - Days::new(rhs.days as u64) - rhs.time_as_timedelta()
    }
}

impl<Tz: TimeZone> Sub<Duration> for DateTime<Tz> {
    type Output = Self;

    fn sub(self, rhs: Duration) -> Self::Output {
        resolve_date_time(self.naive_local() - Months::new(rhs.months) - Days::new(rhs.days as u64), self.timezone())
            - rhs.time_as_timedelta()
    }
}

pub trait DateTimeExt: Sized {
    fn checked_add(self, rhs: Duration) -> Option<Self>;
    fn checked_sub(self, rhs: Duration) -> Option<Self>;
}

impl DateTimeExt for NaiveDateTime {
    fn checked_add(self, rhs: Duration) -> Option<Self> {
        self.checked_add_months(Months::new(rhs.months))?
            .checked_add_days(Days::new(rhs.days as u64))?
            .checked_add_signed(rhs.time_as_timedelta())
    }

    fn checked_sub(self, rhs: Duration) -> Option<Self> {
        self.checked_sub_months(Months::new(rhs.months))?
            .checked_sub_days(Days::new(rhs.days as u64))?
            .checked_sub_signed(rhs.time_as_timedelta())
    }
}

impl<Tz: TimeZone> DateTimeExt for DateTime<Tz> {
    fn checked_add(self, rhs: Duration) -> Option<Self> {
        resolve_date_time(
            self.naive_local()
                .checked_add_months(Months::new(rhs.months))?
                .checked_add_days(Days::new(rhs.days as u64))?,
            self.timezone(),
        )
        .checked_add_signed(rhs.time_as_timedelta())
    }

    fn checked_sub(self, rhs: Duration) -> Option<Self> {
        resolve_date_time(
            self.naive_local()
                .checked_sub_months(Months::new(rhs.months))?
                .checked_sub_days(Days::new(rhs.days as u64))?,
            self.timezone(),
        )
        .checked_sub_signed(rhs.time_as_timedelta())
    }
}

// helper: find a datetime in a given timezone closest to provided local time
// in case of ambiguity (fold), take the earlier datetime
// in case where datetime does not exists in a time zone (gap), advance by the length of the gap
fn resolve_date_time<Tz: TimeZone>(local: NaiveDateTime, tz: Tz) -> DateTime<Tz> {
    match tz.from_local_datetime(&local) {
        MappedLocalTime::Single(dt) | MappedLocalTime::Ambiguous(dt, _) => dt,
        MappedLocalTime::None => {
            // fake it until `GapInfo` is released in `chrono-tz` in 0.11.0 or 0.10.2

            // assuming no time zone changes within a span of 72 hours
            // assuming no gaps longer than 72 hours
            const SAFE_SHIFT: TimeDelta = TimeDelta::hours(72);

            let earlier = local - SAFE_SHIFT;
            let later = local + SAFE_SHIFT;
            let before_offset = tz.offset_from_utc_datetime(&earlier).fix();
            let after_offset = tz.offset_from_utc_datetime(&later).fix();
            debug_assert!(
                after_offset.local_minus_utc() > before_offset.local_minus_utc(),
                "Unexpected offset direction around a gap: transition from {before_offset} to {after_offset} around {local}"
            );

            tz.from_local_datetime(&(local - before_offset + after_offset)).unwrap()
        }
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

    use std::mem;

    use chrono::{DateTime, Days, FixedOffset, Months, NaiveDate, NaiveDateTime, NaiveTime};
    use chrono_tz::Europe::London;
    use rand::{rngs::SmallRng, thread_rng, Rng, SeedableRng};
    use resource::constants::common::SECONDS_IN_HOUR;

    use super::{DateTimeExt, Duration, MAX_YEAR, MIN_YEAR, NANOS_PER_NAIVE_DAY};
    use crate::value::{
        primitive_encoding::encode_u32,
        timezone::{TimeZone, NUM_TZS},
    };

    fn random_naive_date(rng: &mut impl Rng) -> NaiveDate {
        let year = rng.gen_range(MIN_YEAR..=MAX_YEAR);
        let is_leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
        let ordinal_day = rng.gen_range(1..365 + is_leap as u32);
        NaiveDate::from_yo_opt(year, ordinal_day).unwrap()
    }

    fn random_naive_utc_date_time(rng: &mut impl Rng) -> NaiveDateTime {
        let date = random_naive_date(rng);

        const SECS_PER_DAY: u32 = 24 * 60 * 60;
        let secs_from_midnight = rng.gen_range(0..SECS_PER_DAY);
        const NANOS_PER_SEC: u32 = 1_000_000_000;
        let nsecs = rng.gen_range(0..NANOS_PER_SEC);
        let time = NaiveTime::from_num_seconds_from_midnight_opt(secs_from_midnight, nsecs).unwrap();

        date.and_time(time)
    }

    fn random_timezone(rng: &mut impl Rng) -> TimeZone {
        if rng.gen_bool(0.5) {
            TimeZone::Fixed(FixedOffset::east_opt(rng.gen_range(-86399..86400)).unwrap())
        } else {
            TimeZone::decode(encode_u32(rng.gen_range(0..NUM_TZS)))
        }
    }

    fn random_datetime(rng: &mut impl Rng) -> DateTime<TimeZone> {
        random_naive_utc_date_time(rng).and_utc().with_timezone(&random_timezone(rng))
    }

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

        assert_eq!(_2024_03_30__12_00_00 + p1d, _2024_03_31__12_00_00);
        assert_eq!(_2024_03_30__12_00_00.checked_add(p1d), Some(_2024_03_31__12_00_00));
    }

    #[test]
    fn subtracting_across_dst_uses_days() {
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
        assert_eq!(Duration::between_datetimes_tz(_2024_03_30__12_00_00, _2024_03_31__12_00_00), p1d);
    }

    #[test]
    fn subtracting_across_dst_shows_correct_time_delta() {
        // London DST change occurred on 2024-03-31 01:00:00 GMT
        let _2024_03_30__12_00_00 = NaiveDateTime::new(
            NaiveDate::from_ymd_opt(2024, 3, 30).unwrap(),
            NaiveTime::from_hms_opt(12, 0, 0).unwrap(),
        )
        .and_local_timezone(London)
        .unwrap();

        let _2024_03_31__11_00_00 = NaiveDateTime::new(
            NaiveDate::from_ymd_opt(2024, 3, 31).unwrap(),
            NaiveTime::from_hms_opt(11, 0, 0).unwrap(),
        )
        .and_local_timezone(London)
        .unwrap();

        let pt22h = Duration::hours(22);
        assert_eq!(_2024_03_30__12_00_00 + pt22h, _2024_03_31__11_00_00);
        assert_eq!(Duration::between_datetimes_tz(_2024_03_30__12_00_00, _2024_03_31__11_00_00), pt22h);
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

        assert_eq!(_2024_03_30__12_00_00 + pt24h, _2024_03_31__13_00_00);
        assert_eq!(_2024_03_30__12_00_00.checked_add(pt24h), Some(_2024_03_31__13_00_00));
    }

    #[test]
    fn when_adding_duration_is_ambiguous_earlier_is_used() {
        // London DST change occurred on 2024-10-27 02:00:00 BST
        let _2024_10_26__01_30_00__London = NaiveDateTime::new(
            NaiveDate::from_ymd_opt(2024, 10, 26).unwrap(),
            NaiveTime::from_hms_opt(1, 30, 0).unwrap(),
        )
        .and_local_timezone(TimeZone::IANA(London))
        .unwrap();

        let _2024_10_27__01_30_00__BST = NaiveDateTime::new(
            NaiveDate::from_ymd_opt(2024, 10, 27).unwrap(),
            NaiveTime::from_hms_opt(1, 30, 0).unwrap(),
        )
        .and_local_timezone(TimeZone::Fixed(FixedOffset::east_opt(SECONDS_IN_HOUR as i32).unwrap()))
        .unwrap();

        let _2024_10_27__01_30_00__GMT = NaiveDateTime::new(
            NaiveDate::from_ymd_opt(2024, 10, 27).unwrap(),
            NaiveTime::from_hms_opt(1, 30, 0).unwrap(),
        )
        .and_local_timezone(TimeZone::Fixed(FixedOffset::east_opt(0).unwrap()))
        .unwrap();

        let p1d = Duration::days(1);
        let pt24h = Duration::hours(24);
        let pt25h = Duration::hours(25);

        assert_eq!(_2024_10_26__01_30_00__London + p1d, _2024_10_27__01_30_00__BST);
        assert_eq!(_2024_10_26__01_30_00__London + pt24h, _2024_10_27__01_30_00__BST);
        assert_eq!(_2024_10_26__01_30_00__London + pt25h, _2024_10_27__01_30_00__GMT);

        assert_eq!(_2024_10_26__01_30_00__London.checked_add(p1d), Some(_2024_10_27__01_30_00__BST));
        assert_eq!(_2024_10_26__01_30_00__London.checked_add(pt24h), Some(_2024_10_27__01_30_00__BST));
        assert_eq!(_2024_10_26__01_30_00__London.checked_add(pt25h), Some(_2024_10_27__01_30_00__GMT));
    }

    #[test]
    fn when_adding_duration_lands_in_gap_it_is_advanced_by_gap() {
        // London DST change occurred on 2024-03-31 01:00:00 GMT
        let _2024_03_30__01_30_00 = NaiveDateTime::new(
            NaiveDate::from_ymd_opt(2024, 3, 30).unwrap(),
            NaiveTime::from_hms_opt(1, 30, 0).unwrap(),
        )
        .and_local_timezone(London)
        .unwrap();

        let _2024_03_31__02_30_00 = NaiveDateTime::new(
            NaiveDate::from_ymd_opt(2024, 3, 31).unwrap(),
            NaiveTime::from_hms_opt(2, 30, 0).unwrap(),
        )
        .and_local_timezone(London)
        .unwrap();

        let p1d = Duration::days(1);

        assert_eq!(_2024_03_30__01_30_00 + p1d, _2024_03_31__02_30_00);
        assert_eq!(_2024_03_30__01_30_00.checked_add(p1d), Some(_2024_03_31__02_30_00));
    }

    #[test]
    fn adding_24_hours_or_1_day_to_naive_datetime_is_always_the_same() {
        let p1d = Duration::days(1);
        let pt24h = Duration::hours(24);

        let seed = thread_rng().gen();
        let mut rng = SmallRng::seed_from_u64(seed);
        eprintln!("Running with seed: {seed}");

        for _ in 0..1_000_000 {
            let date_time = random_naive_utc_date_time(&mut rng);
            assert_eq!(date_time + p1d, date_time + pt24h);
        }
    }

    #[test]
    fn date_subtraction_is_consistent() {
        let seed = thread_rng().gen();
        let mut rng = SmallRng::seed_from_u64(seed);
        eprintln!("Running with seed: {seed}");

        for _ in 0..1_000_000 {
            let mut earlier = random_naive_date(&mut rng);
            let mut later = random_naive_date(&mut rng);
            if earlier > later {
                mem::swap(&mut earlier, &mut later);
            }
            let Duration { months, days, nanos } = Duration::between_dates(earlier, later);
            assert_eq!(nanos, 0);
            assert_eq!(earlier + Months::new(months) + Days::new(days.into()), later);
        }
    }

    #[test]
    fn datetime_subtraction_is_consistent() {
        let seed = thread_rng().gen();
        let mut rng = SmallRng::seed_from_u64(seed);
        eprintln!("Running with seed: {seed}");

        for _ in 0..1_000_000 {
            let mut date_time_1 = random_datetime(&mut rng);
            let mut date_time_2 = random_datetime(&mut rng);
            if date_time_1 > date_time_2 {
                mem::swap(&mut date_time_1, &mut date_time_2);
            }
            assert_eq!(
                date_time_1 + Duration::between_datetimes_tz(date_time_1, date_time_2),
                date_time_2.with_timezone(&date_time_1.timezone())
            );
        }
    }

    #[test]
    fn datetime_subtraction_always_produces_time_delta_less_than_a_day() {
        let seed = thread_rng().gen();
        let mut rng = SmallRng::seed_from_u64(seed);
        eprintln!("Running with seed: {seed}");

        for _ in 0..1_000_000 {
            let mut date_time_1 = random_datetime(&mut rng);
            let mut date_time_2 = random_datetime(&mut rng);
            if date_time_1 > date_time_2 {
                mem::swap(&mut date_time_1, &mut date_time_2);
            }
            let diff = Duration::between_datetimes_tz(date_time_1, date_time_2);
            assert!(diff.nanos < NANOS_PER_NAIVE_DAY);
        }
    }

    #[test]
    fn extreme_timezone_changes_are_respected() {
        // Samoa switched from -10 to +14 later 29th of December, 2011,
        // skipping 30th of December.
        let _2011_12_29__12_00_00__Apia = NaiveDate::from_ymd_opt(2011, 12, 29)
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap()
            .and_local_timezone(chrono_tz::Tz::Pacific__Apia)
            .unwrap();
        let _2011_12_31__12_00_00__Apia = NaiveDate::from_ymd_opt(2011, 12, 31)
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap()
            .and_local_timezone(chrono_tz::Tz::Pacific__Apia)
            .unwrap();

        assert_eq!(_2011_12_29__12_00_00__Apia + Duration::days(1), _2011_12_31__12_00_00__Apia);
        assert_eq!(_2011_12_29__12_00_00__Apia + Duration::days(2), _2011_12_31__12_00_00__Apia);
        assert_eq!(_2011_12_29__12_00_00__Apia + Duration::hours(24), _2011_12_31__12_00_00__Apia);

        assert_eq!(_2011_12_29__12_00_00__Apia.checked_add(Duration::days(1)), Some(_2011_12_31__12_00_00__Apia));
        assert_eq!(_2011_12_29__12_00_00__Apia.checked_add(Duration::days(2)), Some(_2011_12_31__12_00_00__Apia));
        assert_eq!(_2011_12_29__12_00_00__Apia.checked_add(Duration::hours(24)), Some(_2011_12_31__12_00_00__Apia));
    }

    #[test]
    fn extreme_timezone_changes_produce_sensible_duration_diffs() {
        // Samoa switched from -10 to +14 later 29th of December, 2011,
        // skipping 30th of December.
        let _2011_12_29__12_00_00__Apia = NaiveDate::from_ymd_opt(2011, 12, 29)
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap()
            .and_local_timezone(chrono_tz::Tz::Pacific__Apia)
            .unwrap();
        let _2011_12_31__12_00_00__Apia = NaiveDate::from_ymd_opt(2011, 12, 31)
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap()
            .and_local_timezone(chrono_tz::Tz::Pacific__Apia)
            .unwrap();

        assert_eq!(
            Duration::between_datetimes_tz(_2011_12_29__12_00_00__Apia, _2011_12_31__12_00_00__Apia),
            Duration::days(2),
        );

        let _2011_12_29__13_00_00__Apia = NaiveDate::from_ymd_opt(2011, 12, 29)
            .unwrap()
            .and_hms_opt(13, 0, 0)
            .unwrap()
            .and_local_timezone(chrono_tz::Tz::Pacific__Apia)
            .unwrap();
        assert_eq!(
            Duration::between_datetimes_tz(_2011_12_29__13_00_00__Apia, _2011_12_31__12_00_00__Apia),
            Duration::hours(23),
        );
    }

    #[test]
    fn empty_duration_is_printed_correctly() {
        let expected = "PT0S";
        assert_eq!(expected, Duration::years(0).to_string());
        assert_eq!(expected, Duration::months(0).to_string());
        assert_eq!(expected, Duration::weeks(0).to_string());
        assert_eq!(expected, Duration::days(0).to_string());
        assert_eq!(expected, Duration::hours(0).to_string());
        assert_eq!(expected, Duration::minutes(0).to_string());
        assert_eq!(expected, Duration::seconds(0).to_string());
        assert_eq!(expected, Duration::nanos(0).to_string());
    }

    #[test]
    fn duration_is_parsed_correctly() {
        let years = 7;
        let months = 99;
        let days = 1234;
        let hours = 77;
        let minutes = 123;
        let seconds = 999;
        let nanos = 987654321;

        let input = format!("P{}Y{}M{}DT{}H{}M{}.{:09}S", years, months, days, hours, minutes, seconds, nanos);

        let parsed = input.parse().unwrap();
        let expected = Duration::years(years)
            + Duration::months(months)
            + Duration::days(days)
            + Duration::hours(hours)
            + Duration::minutes(minutes)
            + Duration::seconds(seconds)
            + Duration::nanos(nanos);
        assert_eq!(expected, parsed);
    }
}
