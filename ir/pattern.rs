/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::{Arc, Mutex};
use crate::conjunction::Conjunction;
use crate::context::PatternContext;
use crate::disjunction::Disjunction;
use crate::negation::Negation;
use crate::optional::Optional;
use crate::ScopeId;

#[derive(Debug)]
pub(crate) struct Patterns {
    scope: ScopeId,
    context: Arc<Mutex<PatternContext>>,
    patterns: Vec<Pattern>,
}

impl Patterns {

    pub(crate) fn new(scope: ScopeId, context: Arc<Mutex<PatternContext>>) -> Self {
        Self { scope, context, patterns: Vec::new() }
    }

    pub(crate) fn add_conjunction(&mut self) -> &mut Conjunction {
        let conjunction = Conjunction::new_child(self.scope, self.context.clone());
        self.patterns.push(Pattern::Conjunction(conjunction));
        self.patterns.last_mut().unwrap().as_conjunction_mut().unwrap()
    }

    pub(crate) fn add_disjunction(&mut self) -> &mut Disjunction {
        let disjunction = Disjunction::new_child(self.scope, self.context.clone());
        self.patterns.push(Pattern::Disjunction(disjunction));
        self.patterns.last_mut().unwrap().as_disjunction_mut().unwrap()
    }
}


#[derive(Debug)]
enum Pattern {
    Conjunction(Conjunction),
    Disjunction(Disjunction),
    Negation(Negation),
    Optional(Optional),
}

impl Pattern {
    fn as_conjunction(&self) -> Option<&Conjunction> {
        match self {
            Pattern::Conjunction(conjunction) => Some(conjunction),
            _ => None
        }
    }

    fn as_conjunction_mut(&mut self) -> Option<&mut Conjunction> {
        match self {
            Pattern::Conjunction(conjunction) => Some(conjunction),
            _ => None
        }
    }

    fn as_disjunction(&self) -> Option<&Disjunction> {
        match self {
            Pattern::Disjunction(disjunction) => Some(disjunction),
            _ => None
        }
    }

    fn as_disjunction_mut(&mut self) -> Option<&mut Disjunction> {
        match self {
            Pattern::Disjunction(disjunction) => Some(disjunction),
            _ => None
        }
    }

    fn as_negation(&self) -> Option<&Negation> {
        match self {
            Pattern::Negation(negation) => Some(negation),
            _ => None
        }
    }

    fn as_negation_mut(&mut self) -> Option<&mut Negation> {
        match self {
            Pattern::Negation(negation) => Some(negation),
            _ => None
        }
    }
    
    fn as_optional(&self) -> Option<&Optional> {
        match self {
            Pattern::Optional(optional) => Some(optional),
            _ => None
        }
    }

    fn as_optional_mut(&mut self) -> Option<&mut Optional> {
        match self {
            Pattern::Optional(optional) => Some(optional),
            _ => None
        }
    }
}
