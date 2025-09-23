/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    borrow::Cow,
    collections::HashMap,
    fmt::{self, Write},
};

#[derive(Clone, Debug)]
pub enum JSON {
    Object(HashMap<Cow<'static, str>, JSON>),
    Array(Vec<JSON>),
    String(Cow<'static, str>),
    Number(f64),
    Boolean(bool),
    Null,
}

impl fmt::Display for JSON {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JSON::Object(object) => {
                f.write_char('{')?;
                for (i, (k, v)) in object.iter().enumerate() {
                    if i > 0 {
                        f.write_str(", ")?;
                    }
                    write!(f, r#""{}": {}"#, k, v)?;
                }
                f.write_char('}')?;
            }
            JSON::Array(list) => {
                f.write_char('[')?;
                for (i, v) in list.iter().enumerate() {
                    if i > 0 {
                        f.write_str(", ")?;
                    }
                    write!(f, "{}", v)?;
                }
                f.write_char(']')?;
            }
            JSON::String(string) => write_escaped_string(string, f)?,
            JSON::Number(number) => write!(f, "{number}")?,
            JSON::Boolean(boolean) => write!(f, "{boolean}")?,
            JSON::Null => write!(f, "null")?,
        }
        Ok(())
    }
}

fn write_escaped_string(string: &str, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    const HEX: u8 = 0;
    const BSP: u8 = b'b';
    const TAB: u8 = b't';
    const LF_: u8 = b'n';
    const FF_: u8 = b'f';
    const CR_: u8 = b'r';

    const ASCII_CONTROL: usize = 0x20;

    const ESCAPE: [u8; ASCII_CONTROL] = [
        HEX, HEX, HEX, HEX, HEX, HEX, HEX, HEX, //
        BSP, TAB, LF_, HEX, FF_, CR_, HEX, HEX, //
        HEX, HEX, HEX, HEX, HEX, HEX, HEX, HEX, //
        HEX, HEX, HEX, HEX, HEX, HEX, HEX, HEX, //
    ];

    const HEX_DIGITS: &[u8; 0x10] = b"0123456789abcdef";

    let mut buf = Vec::with_capacity(string.len());

    for byte in string.bytes() {
        if (byte as usize) < ASCII_CONTROL {
            match ESCAPE[byte as usize] {
                HEX => {
                    buf.extend_from_slice(&[
                        b'\\',
                        b'u',
                        b'0',
                        b'0',
                        HEX_DIGITS[(byte as usize & 0xF0) >> 4],
                        HEX_DIGITS[byte as usize & 0x0F],
                    ]);
                }
                special => buf.extend_from_slice(&[b'\\', special]),
            }
        } else {
            match byte {
                b'"' | b'\\' => buf.extend_from_slice(&[b'\\', byte]),
                _ => buf.push(byte),
            }
        }
    }

    write!(f, r#""{}""#, unsafe { String::from_utf8_unchecked(buf) })
}
