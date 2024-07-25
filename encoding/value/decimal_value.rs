/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    fmt,
    ops::{Add, Mul, Sub},
};

pub const FRACTIONAL_PART_DENOMINATOR_LOG10: u32 = 19;
const FRACTIONAL_PART_DENOMINATOR: u64 = 10u64.pow(FRACTIONAL_PART_DENOMINATOR_LOG10);

#[allow(clippy::assertions_on_constants)]
const _ASSERT: () = {
    assert!(FRACTIONAL_PART_DENOMINATOR > u64::MAX / 10);
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Decimal {
    integer: i64,
    fractional: u64,
}

impl Decimal {
    pub const MIN: Self = Self::new(i64::MIN, 0);
    pub const MAX: Self = Self::new(i64::MAX, FRACTIONAL_PART_DENOMINATOR - 1);

    pub const fn new(integer: i64, fractional: u64) -> Self {
        assert!(fractional < FRACTIONAL_PART_DENOMINATOR);
        Self { integer, fractional }
    }

    pub fn integer_part(&self) -> i64 {
        self.integer
    }

    pub fn fractional_part(&self) -> u64 {
        self.fractional
    }
}

impl Add for Decimal {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        let lhs = self;
        let (fractional, carry) = match lhs.fractional.overflowing_add(rhs.fractional) {
            (frac, false) if frac < FRACTIONAL_PART_DENOMINATOR => (frac, 0),
            (frac, true) if frac < FRACTIONAL_PART_DENOMINATOR => {
                (frac + 0u64.wrapping_sub(FRACTIONAL_PART_DENOMINATOR), 1)
            }
            (frac, false) => (frac - FRACTIONAL_PART_DENOMINATOR, 1),
            (_, true) => unreachable!(),
        };
        let integer = lhs.integer + rhs.integer + carry;

        Self::new(integer, fractional)
    }
}

impl Sub for Decimal {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self::Output {
        let lhs = self;
        let (fractional, carry) = match lhs.fractional.overflowing_sub(rhs.fractional) {
            (frac, false) => (frac, 0),
            (frac, true) => (frac.wrapping_add(FRACTIONAL_PART_DENOMINATOR), 1),
        };
        let integer = lhs.integer - rhs.integer - carry;

        Self::new(integer, fractional)
    }
}

impl Mul for Decimal {
    type Output = Decimal;

    fn mul(self, rhs: Self) -> Self::Output {
        let lhs = self;

        let extended_denominator = FRACTIONAL_PART_DENOMINATOR as i128;
        let fractional = (lhs.fractional as i128 * rhs.fractional as i128
            + /* rounding! */ extended_denominator / 2)
            / extended_denominator
            + lhs.fractional as i128 * rhs.integer as i128 % extended_denominator
            + lhs.integer as i128 * rhs.fractional as i128 % extended_denominator;
        let mut carry = fractional / extended_denominator;
        let mut fractional = fractional % extended_denominator;

        while fractional < 0 {
            carry -= 1;
            fractional += extended_denominator;
        }

        let integer = (lhs.integer * rhs.integer) as i128 // intentionally letting overflow occur before extending
            + lhs.fractional as i128 * rhs.integer as i128 / extended_denominator
            + lhs.integer as i128 * rhs.fractional as i128 / extended_denominator
            + carry;

        Self::new(integer as i64, fractional as u64)
    }
}

macro_rules! impl_integer_ops {
    (
        $($optrait:ident::$opname:ident),+ $(,)?
        for
        $types:tt
    ) => {$(impl_integer_ops! { @op $optrait::$opname for $types})+};
    (
        @op $optrait:ident::$opname:ident
        for
        { $($int:ty),+ $(,)? }
    ) => {$(
        impl $optrait<$int> for Decimal {
            type Output = Decimal;
            fn $opname(self, rhs: $int) -> Self::Output {
                $optrait::$opname(self, Decimal::new(rhs as i64, 0))
            }
        }
        impl $optrait<Decimal> for $int {
            type Output = Decimal;
            fn $opname(self, rhs: Decimal) -> Self::Output {
                $optrait::$opname(Decimal::new(self as i64, 0), rhs)
            }
        }
        impl $optrait<&Decimal> for $int {
            type Output = Decimal;
            fn $opname(self, rhs: &Decimal) -> Self::Output {
                $optrait::$opname(Decimal::new(self as i64, 0), rhs)
            }
        }
        impl $optrait<Decimal> for &$int {
            type Output = Decimal;
            fn $opname(self, rhs: Decimal) -> Self::Output {
                $optrait::$opname(Decimal::new(*self as i64, 0), rhs)
            }
        }
        impl $optrait<&Decimal> for &$int {
            type Output = Decimal;
            fn $opname(self, rhs: &Decimal) -> Self::Output {
                $optrait::$opname(Decimal::new(*self as i64, 0), rhs)
            }
        }
    )+};
}

impl_integer_ops! {
    Add::add, Sub::sub, Mul::mul
    for
    { u8, u16, u32, u64, i8, i16, i32, i64 }
}

macro_rules! impl_ref_ops {
    ($($optrait:ident::$opname:ident),+ $(,)?) => {$(
        impl<T> $optrait<&T> for Decimal
        where
            T: Copy,
            Decimal: $optrait<T>,
        {
            type Output = <Decimal as $optrait<T>>::Output;
            fn $opname(self, rhs: &T) -> Self::Output {
                <Decimal as $optrait<T>>::$opname(self, *rhs)
            }
        }

        impl<T> $optrait<T> for &Decimal
        where
            T: Copy,
            Decimal: $optrait<T>,
        {
            type Output = <Decimal as $optrait<T>>::Output;
            fn $opname(self, rhs: T) -> Self::Output {
                <Decimal as $optrait<T>>::$opname(*self, rhs)
            }
        }
    )+};
}

impl_ref_ops! { Add::add, Sub::sub, Mul::mul }

impl fmt::Display for Decimal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // TODO
        fmt::Display::fmt(&(self.integer as f64 + self.fractional as f64 / FRACTIONAL_PART_DENOMINATOR as f64), f)
    }
}

#[cfg(test)]
mod tests {
    use rand::{rngs::SmallRng, thread_rng, Rng, SeedableRng};

    use super::{Decimal, FRACTIONAL_PART_DENOMINATOR};

    fn random_decimal(rng: &mut impl Rng) -> Decimal {
        Decimal { integer: rng.gen(), fractional: rng.gen_range(0..FRACTIONAL_PART_DENOMINATOR) }
    }

    fn random_small_decimal(rng: &mut impl Rng) -> Decimal {
        const INTEGER_MAX_ABS: i64 = (u64::MAX / FRACTIONAL_PART_DENOMINATOR) as i64;
        Decimal {
            integer: rng.gen_range(-INTEGER_MAX_ABS..=INTEGER_MAX_ABS),
            fractional: rng.gen_range(0..FRACTIONAL_PART_DENOMINATOR),
        }
    }

    #[test]
    #[allow(clippy::assertions_on_constants)]
    fn fractional_part_overflow_is_handled_correctly() {
        let sub_one = 1 - Decimal::new(0, 1);
        assert_eq!(sub_one, Decimal::new(0, FRACTIONAL_PART_DENOMINATOR - 1));
        assert_eq!(sub_one + sub_one, 2 - Decimal::new(0, 2));

        assert!(FRACTIONAL_PART_DENOMINATOR > u64::MAX / 2);

        let u64_max_div_denom =
            Decimal::new((u64::MAX / FRACTIONAL_PART_DENOMINATOR) as i64, u64::MAX % FRACTIONAL_PART_DENOMINATOR);
        assert_eq!(
            Decimal::new(0, FRACTIONAL_PART_DENOMINATOR - 1)
                + Decimal::new(0, 0u64.wrapping_sub(FRACTIONAL_PART_DENOMINATOR)),
            u64_max_div_denom
        );

        assert_eq!(sub_one * sub_one, 1 - Decimal::new(0, 2)); // rounded to nearest
    }

    #[test]
    fn randomized_tests() {
        const fn as_i128(lhs: Decimal) -> i128 {
            lhs.integer as i128 * FRACTIONAL_PART_DENOMINATOR as i128 + lhs.fractional as i128
        }

        let seed = thread_rng().gen();
        let mut rng = SmallRng::seed_from_u64(seed);
        eprintln!("Running with seed: {seed}");

        let range = as_i128(Decimal::MIN)..=as_i128(Decimal::MAX);
        for _ in 0..1_000_000 {
            let lhs = random_decimal(&mut rng);
            let rhs = random_decimal(&mut rng);

            if as_i128(lhs).checked_add(as_i128(rhs)).is_some_and(|res| range.contains(&res)) {
                assert_eq!(as_i128(lhs + rhs), as_i128(lhs) + as_i128(rhs), "{:?} + {:?} != {:?}", lhs, rhs, lhs + rhs);
            }
            if as_i128(lhs).checked_sub(as_i128(rhs)).is_some_and(|res| range.contains(&res)) {
                assert_eq!(as_i128(lhs - rhs), as_i128(lhs) - as_i128(rhs), "{:?} - {:?} != {:?}", lhs, rhs, lhs - rhs);
            }

            // two random decimal numbers will almost always overflow on multiplication
            let rhs = random_small_decimal(&mut rng);

            if as_i128(lhs).checked_mul(rhs.integer as i128).is_some_and(|res| range.contains(&res))
                && as_i128(lhs).checked_mul(rhs.integer as i128 + 1).is_some_and(|res| range.contains(&res))
            {
                let lhs_i128 = as_i128(lhs);
                let rhs_i128 = as_i128(rhs);

                let sign = lhs_i128.signum() * rhs_i128.signum();

                let abs_lhs_u128 = lhs_i128.unsigned_abs();
                let abs_rhs_u128 = rhs_i128.unsigned_abs();

                let mul = unsigned_bigint_mul(as_unsigned_bigint(abs_lhs_u128), as_unsigned_bigint(abs_rhs_u128));
                let bigint_mul_result = sign
                    * ((mul[0] >= FRACTIONAL_PART_DENOMINATOR / 2) as i128
                        + mul[1] as i128
                        + mul[2] as i128 * FRACTIONAL_PART_DENOMINATOR as i128);

                assert_eq!(as_i128(lhs * rhs), bigint_mul_result, "{:?} * {:?} != {:?}", lhs, rhs, lhs * rhs);
            }
        }
    }

    const DENOMINATOR_U128: u128 = FRACTIONAL_PART_DENOMINATOR as u128;

    fn as_unsigned_bigint(int: u128) -> Vec<u64> {
        vec![(int % DENOMINATOR_U128) as u64, (int / DENOMINATOR_U128) as u64]
    }

    fn unsigned_bigint_mul(lhs: Vec<u64>, rhs: Vec<u64>) -> Vec<u64> {
        fn unsigned_bigint_add(lhs: Vec<u64>, rhs: Vec<u64>) -> Vec<u64> {
            let mut carry = 0;
            (0..=usize::max(lhs.len(), rhs.len()))
                .map(|index| {
                    let left = *lhs.get(index).unwrap_or(&0);
                    let right = *rhs.get(index).unwrap_or(&0);
                    let wide_res = left as u128 + right as u128 + carry as u128;
                    carry = (wide_res / DENOMINATOR_U128) as u64;
                    (wide_res % DENOMINATOR_U128) as u64
                })
                .collect()
        }

        fn bigint_mul_u64(lhs: Vec<u64>, rhs: u64) -> Vec<u64> {
            let mut buf = Vec::new();
            for (i, left) in lhs.into_iter().enumerate() {
                let mut bigint = vec![0; i];
                let wide_res = left as u128 * rhs as u128;
                bigint.extend([(wide_res % DENOMINATOR_U128) as u64, (wide_res / DENOMINATOR_U128) as u64]);
                buf = unsigned_bigint_add(buf, bigint);
            }
            buf
        }

        let mut buf = Vec::new();
        for (i, left) in lhs.into_iter().enumerate() {
            let mut bigint = vec![0; i];
            bigint.extend(bigint_mul_u64(rhs.clone(), left));
            buf = unsigned_bigint_add(buf, bigint);
        }
        buf
    }
}
