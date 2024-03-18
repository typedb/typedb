/*
 *  Copyright (C) 2023 Vaticle
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as
 *  published by the Free Software Foundation, either version 3 of the
 *  License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

use crate::layout::infix::InfixID;

// A tiny struct will always be more efficient owning its own data and being Copy
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct ValueTypeID {
    bytes: [u8; { ValueTypeID::LENGTH }],
}

impl ValueTypeID {
    const LENGTH: usize = 1;

    pub const fn new(bytes: [u8; { ValueTypeID::LENGTH }]) -> Self {
        ValueTypeID { bytes: bytes }
    }

    pub fn bytes(&self) -> [u8; { InfixID::LENGTH }] {
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
    /*
    Datetime, // TODO: consider splitting with/without timezone
    Duration, // Naming: 'interval'?
     */
}

macro_rules! value_type_functions {
    ($(
        $name:ident => $bytes:tt
    ),*) => {
        pub const fn value_type_id(&self) -> ValueTypeID {
            let bytes = match self {
                $(
                    Self::$name => {&$bytes}
                )*
            };
            ValueTypeID::new(*bytes)
        }

        pub fn from_value_type_id(value_type_id: ValueTypeID) -> Self {
            match value_type_id.bytes() {
                $(
                    $bytes => {Self::$name}
                )*
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
        String => [3]
    );
}
