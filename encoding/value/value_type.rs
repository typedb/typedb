/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use crate::layout::infix::InfixID;

// A tiny struct will always be more efficient owning its own data and being Copy
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct ValueTypeID {
    bytes: [u8; ValueTypeID::LENGTH],
}

impl ValueTypeID {
    const LENGTH: usize = 1;

    pub const fn new(bytes: [u8; ValueTypeID::LENGTH]) -> Self {
        ValueTypeID { bytes }
    }

    pub fn bytes(&self) -> [u8; InfixID::LENGTH] {
        self.bytes
    }
}

// TODO: how do we handle user-created compound structs?
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ValueType {
    Boolean,
    Long,
    Double,
    String,

    // TODO: consider splitting with/without timezone
    DateTime,

    /*
    Duration, // Naming: 'interval'?
     */
}

macro_rules! value_type_functions {
    ($(
        $name:ident => $bytes:tt
    ),+ $(,)?) => {
        pub const fn value_type_id(&self) -> ValueTypeID {
            let bytes = match self {
                $(
                    Self::$name => &$bytes,
                )+
            };
            ValueTypeID::new(*bytes)
        }

        pub fn from_value_type_id(value_type_id: ValueTypeID) -> Self {
            match value_type_id.bytes() {
                $(
                    $bytes => Self::$name,
                )+
                _ => unreachable!(),
            }
       }
   };
}

impl ValueType {
    value_type_functions!(
        Boolean => [0],
        Long => [1],
        Double => [2],
        String => [3],
        DateTime => [4],
    );
}
