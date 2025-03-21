/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::fmt;

use structural_equality::{ordered_hash_combine, StructuralEquality};

#[derive(Copy, Clone, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub struct Variable {
    id: VariableId,
    anonymous: bool,
}

impl Variable {
    pub fn new(id: u16) -> Self {
        Self { id: VariableId { id }, anonymous: false }
    }

    pub fn new_anonymous(id: u16) -> Self {
        Self { id: VariableId { id }, anonymous: true }
    }

    pub fn is_anonymous(&self) -> bool {
        self.anonymous
    }

    pub fn is_named(&self) -> bool {
        !self.anonymous
    }
}

impl fmt::Display for Variable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.anonymous {
            write!(f, "$_{}", self.id)
        } else {
            write!(f, "${}", self.id)
        }
    }
}

impl fmt::Debug for Variable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.anonymous {
            write!(f, "$_{}", self.id)
        } else {
            write!(f, "${}", self.id)
        }
    }
}

#[derive(Debug, Copy, Clone, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) struct VariableId {
    // TODO: retain line/character from original query at which point this Variable was declared
    id: u16,
}

impl fmt::Display for VariableId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.id)
    }
}

impl StructuralEquality for Variable {
    fn hash(&self) -> u64 {
        ordered_hash_combine(self.anonymous as u64, self.id.id as u64)
    }

    fn equals(&self, other: &Self) -> bool {
        self == other && self.anonymous == other.anonymous
    }
}
