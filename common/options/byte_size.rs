/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Byte-size parsing inspired by the `bytesize` crate
//! (https://github.com/hyunsik/bytesize, Apache-2.0).
//!
//! Convention: `KB`/`MB`/`GB`/`TB` are treated as **binary** (1024-based)
//! to match TypeDB's existing `KB`/`MB`/`GB` constants in
//! `resource/constants.rs`. The explicit `KiB`/`MiB`/`GiB`/`TiB` forms are
//! also accepted as aliases. Decimal SI units (`kB`, `K`, etc.) are NOT
//! supported — the parser only accepts the forms documented here.

use std::{
    fmt::{self, Display, Formatter},
    str::FromStr,
};

use serde::{Deserialize, Deserializer};

const KIB: u64 = 1024;
const MIB: u64 = 1024 * KIB;
const GIB: u64 = 1024 * MIB;
const TIB: u64 = 1024 * GIB;

/// A byte-size value, expressible from a unit-suffixed string such as
/// `"1gb"`, `"500 mb"`, `"512MiB"`, or a plain integer (raw bytes).
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ByteSize(u64);

impl ByteSize {
    pub const fn bytes(value: u64) -> Self {
        Self(value)
    }

    pub const fn kibibytes(value: u64) -> Self {
        Self(value * KIB)
    }

    pub const fn mebibytes(value: u64) -> Self {
        Self(value * MIB)
    }

    pub const fn gibibytes(value: u64) -> Self {
        Self(value * GIB)
    }

    pub const fn as_u64(self) -> u64 {
        self.0
    }

    pub const fn as_usize(self) -> usize {
        self.0 as usize
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParseByteSizeError {
    input: String,
    reason: &'static str,
}

impl Display for ParseByteSizeError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "Could not parse byte size from '{}': {}", self.input, self.reason)
    }
}

impl std::error::Error for ParseByteSizeError {}

impl FromStr for ByteSize {
    type Err = ParseByteSizeError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let trimmed = input.trim();
        if trimmed.is_empty() {
            return Err(ParseByteSizeError { input: input.to_owned(), reason: "value is empty" });
        }

        // Split into the leading numeric part and trailing unit suffix.
        // The numeric part is the longest prefix of digits (no decimals or signs).
        let split_at = trimmed.find(|c: char| !c.is_ascii_digit()).unwrap_or(trimmed.len());
        let (number_part, unit_part) = trimmed.split_at(split_at);
        if number_part.is_empty() {
            return Err(ParseByteSizeError {
                input: input.to_owned(),
                reason: "missing numeric portion",
            });
        }
        let number: u64 = number_part.parse().map_err(|_| ParseByteSizeError {
            input: input.to_owned(),
            reason: "numeric portion is not a non-negative integer that fits in u64",
        })?;

        let unit = unit_part.trim().to_ascii_lowercase();
        let multiplier: u64 = match unit.as_str() {
            "" | "b" => 1,
            "k" | "kb" | "kib" => KIB,
            "m" | "mb" | "mib" => MIB,
            "g" | "gb" | "gib" => GIB,
            "t" | "tb" | "tib" => TIB,
            _ => {
                return Err(ParseByteSizeError {
                    input: input.to_owned(),
                    reason: "unknown unit suffix (expected one of: B, KB/KiB, MB/MiB, GB/GiB, TB/TiB)",
                });
            }
        };

        let bytes = number.checked_mul(multiplier).ok_or_else(|| ParseByteSizeError {
            input: input.to_owned(),
            reason: "value overflows u64",
        })?;
        Ok(Self(bytes))
    }
}

impl Display for ByteSize {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        // Choose the largest unit that yields a clean (non-fractional) representation
        // when possible, otherwise fall back to one decimal place.
        let bytes = self.0;
        let (value, unit) = if bytes >= TIB {
            (bytes as f64 / TIB as f64, "TiB")
        } else if bytes >= GIB {
            (bytes as f64 / GIB as f64, "GiB")
        } else if bytes >= MIB {
            (bytes as f64 / MIB as f64, "MiB")
        } else if bytes >= KIB {
            (bytes as f64 / KIB as f64, "KiB")
        } else {
            return write!(f, "{} B", bytes);
        };
        if (value.fract()).abs() < f64::EPSILON {
            write!(f, "{} {}", value as u64, unit)
        } else {
            write!(f, "{:.1} {}", value, unit)
        }
    }
}

impl<'de> Deserialize<'de> for ByteSize {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ByteSizeVisitor;

        impl<'de> serde::de::Visitor<'de> for ByteSizeVisitor {
            type Value = ByteSize;

            fn expecting(&self, f: &mut Formatter<'_>) -> fmt::Result {
                f.write_str(
                    "a byte-size value, e.g. `1gb`, `500 mb`, `512MiB`, or a non-negative integer of bytes",
                )
            }

            fn visit_str<E>(self, value: &str) -> Result<ByteSize, E>
            where
                E: serde::de::Error,
            {
                ByteSize::from_str(value).map_err(serde::de::Error::custom)
            }

            fn visit_u64<E>(self, value: u64) -> Result<ByteSize, E>
            where
                E: serde::de::Error,
            {
                Ok(ByteSize::bytes(value))
            }

            fn visit_i64<E>(self, value: i64) -> Result<ByteSize, E>
            where
                E: serde::de::Error,
            {
                if value < 0 {
                    Err(serde::de::Error::custom("byte size cannot be negative"))
                } else {
                    Ok(ByteSize::bytes(value as u64))
                }
            }
        }

        deserializer.deserialize_any(ByteSizeVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_plain_bytes() {
        assert_eq!(ByteSize::from_str("1024").unwrap(), ByteSize::bytes(1024));
        assert_eq!(ByteSize::from_str("0").unwrap(), ByteSize::bytes(0));
    }

    #[test]
    fn parses_units_lowercase() {
        assert_eq!(ByteSize::from_str("1kb").unwrap(), ByteSize::bytes(KIB));
        assert_eq!(ByteSize::from_str("1mb").unwrap(), ByteSize::bytes(MIB));
        assert_eq!(ByteSize::from_str("1gb").unwrap(), ByteSize::bytes(GIB));
        assert_eq!(ByteSize::from_str("2tb").unwrap(), ByteSize::bytes(2 * TIB));
    }

    #[test]
    fn parses_units_uppercase() {
        assert_eq!(ByteSize::from_str("1KB").unwrap(), ByteSize::bytes(KIB));
        assert_eq!(ByteSize::from_str("1MB").unwrap(), ByteSize::bytes(MIB));
        assert_eq!(ByteSize::from_str("1GB").unwrap(), ByteSize::bytes(GIB));
    }

    #[test]
    fn parses_units_mixed_case_and_iec_form() {
        assert_eq!(ByteSize::from_str("512MiB").unwrap(), ByteSize::bytes(512 * MIB));
        assert_eq!(ByteSize::from_str("4kib").unwrap(), ByteSize::bytes(4 * KIB));
        assert_eq!(ByteSize::from_str("1GiB").unwrap(), ByteSize::bytes(GIB));
    }

    #[test]
    fn parses_units_iec_all_caps() {
        assert_eq!(ByteSize::from_str("4KIB").unwrap(), ByteSize::bytes(4 * KIB));
        assert_eq!(ByteSize::from_str("512MIB").unwrap(), ByteSize::bytes(512 * MIB));
        assert_eq!(ByteSize::from_str("1GIB").unwrap(), ByteSize::bytes(GIB));
        assert_eq!(ByteSize::from_str("2TIB").unwrap(), ByteSize::bytes(2 * TIB));
    }

    #[test]
    fn parses_with_internal_space() {
        assert_eq!(ByteSize::from_str("500 mb").unwrap(), ByteSize::bytes(500 * MIB));
        assert_eq!(ByteSize::from_str("1   GB").unwrap(), ByteSize::bytes(GIB));
    }

    #[test]
    fn parses_with_outer_whitespace() {
        assert_eq!(ByteSize::from_str("  1 gb  ").unwrap(), ByteSize::bytes(GIB));
    }

    #[test]
    fn parses_unit_alone_letter() {
        assert_eq!(ByteSize::from_str("4k").unwrap(), ByteSize::bytes(4 * KIB));
        assert_eq!(ByteSize::from_str("3M").unwrap(), ByteSize::bytes(3 * MIB));
        assert_eq!(ByteSize::from_str("2g").unwrap(), ByteSize::bytes(2 * GIB));
    }

    #[test]
    fn parses_explicit_byte_unit() {
        assert_eq!(ByteSize::from_str("42b").unwrap(), ByteSize::bytes(42));
        assert_eq!(ByteSize::from_str("42 B").unwrap(), ByteSize::bytes(42));
    }

    #[test]
    fn rejects_empty() {
        assert!(ByteSize::from_str("").is_err());
        assert!(ByteSize::from_str("   ").is_err());
    }

    #[test]
    fn rejects_negative() {
        assert!(ByteSize::from_str("-1mb").is_err());
    }

    #[test]
    fn rejects_unknown_unit() {
        assert!(ByteSize::from_str("1pb").is_err());
        assert!(ByteSize::from_str("1giga").is_err());
        assert!(ByteSize::from_str("1xb").is_err());
    }

    #[test]
    fn rejects_no_number() {
        assert!(ByteSize::from_str("mb").is_err());
        assert!(ByteSize::from_str("gb").is_err());
    }

    #[test]
    fn rejects_decimals() {
        // We deliberately do not support fractional input; the bytesize crate
        // does, but TypeDB has no use for it and rejecting reduces ambiguity.
        assert!(ByteSize::from_str("1.5gb").is_err());
    }

    #[test]
    fn rejects_overflow() {
        // u64 max is ~16 EiB; 18,446,744 TiB overflows.
        assert!(ByteSize::from_str("99999999999999tb").is_err());
    }

    #[test]
    fn display_round_trip() {
        let cases = [
            (ByteSize::bytes(0), "0 B"),
            (ByteSize::bytes(512), "512 B"),
            (ByteSize::kibibytes(1), "1 KiB"),
            (ByteSize::mebibytes(512), "512 MiB"),
            (ByteSize::gibibytes(1), "1 GiB"),
            (ByteSize::bytes(1536), "1.5 KiB"),
        ];
        for (size, expected) in cases {
            assert_eq!(size.to_string(), expected, "Display for {:?}", size);
        }
        // Round-trip the integral cases through FromStr.
        for size in [
            ByteSize::bytes(0),
            ByteSize::bytes(512),
            ByteSize::kibibytes(1),
            ByteSize::mebibytes(512),
            ByteSize::gibibytes(1),
        ] {
            let rendered = size.to_string();
            // Display emits e.g. "512 MiB" — FromStr needs to accept this.
            let parsed = ByteSize::from_str(&rendered).expect("display output must round-trip");
            assert_eq!(parsed, size, "round-trip for {:?} via '{}'", size, rendered);
        }
    }

    #[test]
    fn deserialize_from_string() {
        let parsed: ByteSize = serde_yaml2::from_str("\"1gb\"").unwrap();
        assert_eq!(parsed, ByteSize::gibibytes(1));
    }

    #[test]
    fn deserialize_from_integer() {
        let parsed: ByteSize = serde_yaml2::from_str("1024").unwrap();
        assert_eq!(parsed, ByteSize::bytes(1024));
    }

    #[test]
    fn deserialize_unquoted_unit_string_in_yaml() {
        // YAML treats `1gb` as a string when not enclosed in quotes —
        // verify our Deserialize impl handles it.
        let parsed: ByteSize = serde_yaml2::from_str("1gb").unwrap();
        assert_eq!(parsed, ByteSize::gibibytes(1));
    }

    #[test]
    fn deserialize_negative_rejected() {
        let result: Result<ByteSize, _> = serde_yaml2::from_str("-1");
        assert!(result.is_err());
    }
}
