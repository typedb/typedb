/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    borrow::Cow,
    fmt::{self, Write},
};

pub const KB: u64 = 1024;
pub const MB: u64 = KB * KB;
pub const GB: u64 = MB * KB;

// TODO: this needs to be optimised using bigger strides than a single byte!
///
/// Performs a big-endian +1 operation that errors on overflow
///
pub fn increment(bytes: &mut [u8]) -> Result<(), BytesError> {
    for byte in bytes.iter_mut().rev() {
        let (val, overflow) = byte.overflowing_add(1);
        *byte = val;
        if !overflow {
            return Ok(());
        }
    }
    Err(BytesError { kind: BytesErrorKind::IncrementOverflow {} })
}

///
/// Performs a 'const' big-endian +1 operation that panics on overflow
///
pub const fn increment_fixed<const SIZE: usize>(mut bytes: [u8; SIZE]) -> [u8; SIZE] {
    let mut index = SIZE;
    while index > 0 {
        let (val, overflow) = bytes[index - 1].overflowing_add(1);
        bytes[index - 1] = val;
        if overflow {
            panic!("Overflow while incrementing array")
        }
        index -= 1;
    }
    bytes
}

#[derive(Debug)]
pub struct BytesError {
    pub kind: BytesErrorKind,
}

#[derive(Debug)]
pub enum BytesErrorKind {
    IncrementOverflow {},
}

impl fmt::Display for BytesError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.kind {
            BytesErrorKind::IncrementOverflow {} => {
                write!(f, "BytesError::IncrementOverflow")
            }
        }
    }
}

#[derive(Clone)]
pub struct HexBytesFormatter<'a>(Cow<'a, [u8]>);

impl<'a> HexBytesFormatter<'a> {
    pub fn owned(bytes: Vec<u8>) -> Self {
        Self(Cow::Owned(bytes))
    }

    pub fn borrowed(bytes: &'a [u8]) -> Self {
        Self(Cow::Borrowed(bytes))
    }

    pub fn format_iid(&self) -> String {
        const PREFIX: &'static str = "0x";
        let mut result = String::with_capacity(PREFIX.len() + self.0.len() * 2);
        result.push_str(PREFIX);
        self.0.iter().for_each(|byte| write!(result, "{byte:02x}").expect("Expected IID formatting"));
        result
    }
}

impl fmt::Display for HexBytesFormatter<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

impl fmt::Debug for HexBytesFormatter<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        const GROUP: usize = 2;
        const BREAK: usize = 16;
        f.write_str("[")?;
        if f.alternate() {
            f.write_str("\n    ")?;
        }
        for (i, byte) in self.0.iter().enumerate() {
            write!(f, "{:02X}", byte)?;
            if i + 1 < self.0.len() {
                if f.alternate() && (i + 1) % BREAK == 0 {
                    f.write_str("\n    ")?;
                } else if (i + 1) % GROUP == 0 {
                    f.write_char(' ')?;
                }
            }
        }
        if f.alternate() {
            f.write_char('\n')?;
        }
        f.write_char(']')?;
        Ok(())
    }
}
