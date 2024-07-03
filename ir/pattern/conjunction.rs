/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    fmt::{Display, Formatter},
    sync::{Arc, Mutex, MutexGuard},
};

use itertools::Itertools;

use answer::variable::Variable;

use crate::{
    pattern::{constraint::Constraints, Scope, ScopeId},
    PatternDefinitionError,
};
use crate::pattern::disjunction::Disjunction;
use crate::pattern::negation::Negation;
use crate::pattern::nested_pattern::NestedPattern;
use crate::pattern::optional::Optional;
use crate::program::block::BlockContext;

#[derive(Debug)]
pub struct Conjunction {
    scope_id: ScopeId,
    context: Arc<Mutex<BlockContext>>,

    constraints: Constraints,
    nested_patterns: Vec<NestedPattern>,
}

impl Conjunction {
    pub fn new(scope_id: ScopeId, context: Arc<Mutex<BlockContext>>) -> Self {
        Conjunction {
            scope_id,
            context: context.clone(),
            constraints: Constraints::new(scope_id, context.clone()),
            nested_patterns: Vec::new(),
        }
    }

    pub(crate) fn new_child(parent_scope_id: ScopeId, context: Arc<Mutex<BlockContext>>) -> Self {
        let scope_id = context.lock().unwrap().create_child_scope(parent_scope_id);
        Conjunction {
            scope_id,
            context: context.clone(),
            constraints: Constraints::new(scope_id, context.clone()),
            nested_patterns: Vec::new(),
        }
    }

    pub(crate) fn scope_id(&self) -> ScopeId {
        self.scope_id
    }

    pub fn constraints(&self) -> &Constraints {
        &self.constraints
    }

    pub fn nested_patterns(&self) -> &Vec<NestedPattern> {
        &self.nested_patterns
    }

    pub fn constraints_mut(&mut self) -> &mut Constraints {
        &mut self.constraints
    }

   pub(crate) fn add_disjunction(&mut self) -> &mut Disjunction {
        let disjunction = Disjunction::new_child(self.scope_id, self.context.clone());
        self.nested_patterns.push(NestedPattern::Disjunction(disjunction));
        self.nested_patterns.last_mut().unwrap().as_disjunction_mut().unwrap()
   }

    pub(crate) fn add_negation(&mut self) -> &mut Negation {
        let negation = Negation::new_child(self.scope_id, self.context.clone());
        self.nested_patterns.push(NestedPattern::Negation(negation));
        self.nested_patterns.last_mut().unwrap().as_negation_mut().unwrap()
    }

    pub(crate) fn add_optional(&mut self) -> &mut Optional {
        let optional = Optional::new_child(self.scope_id, self.context.clone());
        self.nested_patterns.push(NestedPattern::Optional(optional));
        self.nested_patterns.last_mut().unwrap().as_optional_mut().unwrap()
    }

        pub(crate) fn context(&self) -> MutexGuard<BlockContext> {
        self.context.lock().unwrap()
    }

    pub fn get_or_declare_variable(&mut self, name: &str) -> Result<Variable, PatternDefinitionError> {
        self.context.lock().unwrap().get_or_declare_variable_named(name, self)
    }
}

impl Scope for Conjunction {
    fn scope_id(&self) -> ScopeId {
        self.scope_id
    }
}

impl Display for Conjunction {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let current_width = f.width().unwrap_or(0);
        let indent = (0..current_width).map(|_| " ").join("");
        writeln!(f, "{}{} Conjunction", indent, self.scope_id)?;
        write!(f, "{:>width$}", &self.constraints, width = current_width + 2)?;
        for pattern in &self.nested_patterns {
            write!(f, "{:>width$}", pattern, width = current_width + 2)?;
        }
        write!(f, "{}", self.context.lock().unwrap())?;
        Ok(())
    }
}
