/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    fmt::{Debug, Display, Formatter},
    sync::MutexGuard,
};

use crate::{
    pattern::{disjunction::Disjunction, negation::Negation, optional::Optional},
    program::block::BlockContext,
};

#[derive(Debug)]
pub enum NestedPattern {
    Disjunction(Disjunction),
    Negation(Negation),
    Optional(Optional),
}

impl NestedPattern {
    pub(crate) fn context(&self) -> MutexGuard<BlockContext> {
        match self {
            NestedPattern::Disjunction(disjunction) => disjunction.context(),
            NestedPattern::Negation(negation) => negation.context(),
            NestedPattern::Optional(optional) => optional.context(),
        }
    }

    pub(crate) fn as_disjunction(&self) -> Option<&Disjunction> {
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

impl Display for NestedPattern {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            NestedPattern::Disjunction(pattern) => Display::fmt(pattern, f),
            NestedPattern::Negation(pattern) => Display::fmt(pattern, f),
            NestedPattern::Optional(pattern) => Display::fmt(pattern, f),
        }
    }
}
