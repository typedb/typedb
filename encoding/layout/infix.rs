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

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub(crate) struct InfixID {
    bytes: [u8; InfixID::LENGTH],
}

impl InfixID {
    pub(crate) const LENGTH: usize = 1;

    pub(crate) const fn new(bytes: [u8; InfixID::LENGTH]) -> Self {
        InfixID { bytes }
    }

    pub(crate) fn bytes(&self) -> [u8; InfixID::LENGTH] {
        self.bytes
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum Infix {
    PropertyLabel,
    PropertyValueType,
    PropertyAnnotationAbstract,
    PropertyAnnotationDistinct,
    PropertyAnnotationIndependent,
}

macro_rules! infix_functions {
    ($(
        $name:ident => $bytes:tt
    );*) => {
        pub(crate) const fn infix_id(&self) -> InfixID {
            let bytes = match self {
                $(
                    Self::$name => {&$bytes}
                )*
            };
            InfixID::new(*bytes)
        }

        pub(crate) fn from_infix_id(infix_id: InfixID) -> Self {
            match infix_id.bytes() {
                $(
                    $bytes => {Self::$name}
                )*
                _ => unreachable!(),
            }
       }
   };
}

impl Infix {
    infix_functions!(
        PropertyLabel => [0];
        PropertyValueType => [1];
        PropertyAnnotationAbstract => [20];
        PropertyAnnotationDistinct => [21];
        PropertyAnnotationIndependent => [22]
    );
}
