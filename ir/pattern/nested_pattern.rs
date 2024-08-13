/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::fmt;

use crate::pattern::{disjunction::Disjunction, negation::Negation, optional::Optional};

#[derive(Debug, Clone)]
pub enum NestedPattern {
    Disjunction(Disjunction),
    Negation(Negation),
    Optional(Optional),
}

impl NestedPattern {
    pub fn as_disjunction(&self) -> Option<&Disjunction> {
        match self {
            NestedPattern::Disjunction(disjunction) => Some(disjunction),
            _ => None,
        }
    }

    pub(crate) fn as_disjunction_mut(&mut self) -> Option<&mut Disjunction> {
        match self {
            NestedPattern::Disjunction(disjunction) => Some(disjunction),
            _ => None,
        }
    }

    pub(crate) fn as_negation(&self) -> Option<&Negation> {
        match self {
            NestedPattern::Negation(negation) => Some(negation),
            _ => None,
        }
    }

    pub(crate) fn as_negation_mut(&mut self) -> Option<&mut Negation> {
        match self {
            NestedPattern::Negation(negation) => Some(negation),
            _ => None,
        }
    }

    pub(crate) fn as_optional(&self) -> Option<&Optional> {
        match self {
            NestedPattern::Optional(optional) => Some(optional),
            _ => None,
        }
    }

    pub(crate) fn as_optional_mut(&mut self) -> Option<&mut Optional> {
        match self {
            NestedPattern::Optional(optional) => Some(optional),
            _ => None,
        }
    }
}

impl fmt::Display for NestedPattern {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NestedPattern::Disjunction(pattern) => fmt::Display::fmt(pattern, f),
            NestedPattern::Negation(pattern) => fmt::Display::fmt(pattern, f),
            NestedPattern::Optional(pattern) => fmt::Display::fmt(pattern, f),
        }
    }
}
