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



// A tiny struct will always be more efficient owning its own data and being Copy
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

// #[derive(Debug, Eq, PartialEq)]
// pub(crate) enum Direction {
//     Canonical,
//     Reverse,
// }
//
/*
Infixes are always stored behind certain types Prefixes, so they could be partitioned per prefix.
For example, type edge infixes are always going to follow type vertex prefixes.
Also, annotation infixes will always follow an annotation prefix and a vertex.

However, we group them all together for
1) easier refactoring of prefixes without clashes in the infixes after refactoring
2) easier overview of what types of 'middle' infix bytes are possible
*/
#[derive(Debug, Eq, PartialEq)]
pub(crate) enum InfixType {
    EdgeSub,
    EdgeSubReverse,
    EdgeOwns,
    EdgeOwnsReverse,
    EdgePlays,
    EdgePlaysReverse,
    EdgeRelates,
    EdgeRelatesReverse,

    EdgeHas,
    EdgeHasReverse,

    PropertyLabel,
    PropertyValueType,
    PropertyAnnotationAbstract,
}

macro_rules! infix_functions {
    ($(
        $name:ident => $bytes:tt //, Direction::$direction:ident
    ),*) => {
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

       // pub(crate) const fn direction(&self) -> Direction {
       //      match self {
       //          $(
       //              Self::$name => {Direction::$direction}
       //          )*
       //      }
       // }

   };
}

impl InfixType {
    infix_functions!(
        EdgeSub => [20], // Direction::Canonical;
        EdgeSubReverse => [21], // Direction::Reverse;
        EdgeOwns => [22], // Direction::Canonical;
        EdgeOwnsReverse => [23], // Direction::Reverse;
        EdgePlays => [24], // Direction::Canonical;
        EdgePlaysReverse => [25], // Direction::Reverse;
        EdgeRelates => [26], // Direction::Canonical;
        EdgeRelatesReverse => [27], // Direction::Reverse;

        EdgeHas => [50], // Direction::Canonical;
        EdgeHasReverse => [51], // Direction::Reverse

        PropertyLabel => [100],
        PropertyValueType => [101],
        PropertyAnnotationAbstract => [110]
    );
}
