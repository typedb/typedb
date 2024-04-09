/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
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
    PropertyAnnotationCardinality,
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
        PropertyAnnotationIndependent => [22];
        PropertyAnnotationCardinality => [23]
    );
}
