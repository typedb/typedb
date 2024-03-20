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

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub(crate) enum InfixGroup {
    Edge,
    Property,
}

/*
Infixes are always stored behind certain types Prefixes, so they could be partitioned per prefix.
For example, type edge infixes are always going to follow type vertex prefixes.
Also, annotation infixes will always follow an annotation prefix and a vertex.

However, we group them all together for
1) easier refactoring of prefixes without clashes in the infixes after refactoring
2) easier overview of what types of 'middle' infix bytes are possible
*/
#[derive(Debug, Eq, PartialEq)]
pub enum InfixType {
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
        $name:ident => $bytes:tt, InfixGroup::$group:ident
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

       pub(crate) const fn group(&self) -> InfixGroup {
            match self {
                $(
                    Self::$name => {InfixGroup::$group}
                )*
            }
       }
   };
}

impl InfixType {
    infix_functions!(
        EdgeSub => [20], InfixGroup::Edge;
        EdgeSubReverse => [21], InfixGroup::Edge;
        EdgeOwns => [22], InfixGroup::Edge;
        EdgeOwnsReverse => [23], InfixGroup::Edge;
        EdgePlays => [24], InfixGroup::Edge;
        EdgePlaysReverse => [25], InfixGroup::Edge;
        EdgeRelates => [26], InfixGroup::Edge;
        EdgeRelatesReverse => [27], InfixGroup::Edge;

        EdgeHas => [50], InfixGroup::Edge;
        EdgeHasReverse => [51], InfixGroup::Edge;

        PropertyLabel => [100], InfixGroup::Property;
        PropertyValueType => [101], InfixGroup::Property;
        PropertyAnnotationAbstract => [110], InfixGroup::Property
    );
}
