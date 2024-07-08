/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::fmt;

use answer::variable::Variable;

use super::{
    constraint::Constraints, disjunction::Disjunction, negation::Negation, nested_pattern::NestedPattern,
    optional::Optional, Scope, ScopeId,
};
use crate::{program::block::BlockContext, PatternDefinitionError};

#[derive(Debug)]
pub struct Conjunction {
    scope_id: ScopeId,
    constraints: Constraints,
    nested_patterns: Vec<NestedPattern>,
}

impl Conjunction {
    pub fn new(scope_id: ScopeId) -> Self {
        Self { scope_id, constraints: Constraints::new(scope_id), nested_patterns: Vec::new() }
    }

    pub(crate) fn build_from_typeql_patterns(
        context: &mut BlockContext,
        scope_id: ScopeId,
        patterns: &[typeql::Pattern],
    ) -> Result<Self, PatternDefinitionError> {
        let mut constraints = Constraints::new(scope_id);
        let nested_patterns = Vec::new();
        for item in patterns {
            match item {
                typeql::Pattern::Conjunction(_) => todo!(),
                typeql::Pattern::Disjunction(_) => todo!(),
                typeql::Pattern::Negation(_) => todo!(),
                typeql::Pattern::Try(_) => todo!(),
                typeql::Pattern::Statement(stmt) => constraints.extend_from_typeql_statement(context, stmt)?,
            }
        }
        Ok(Self { scope_id, constraints, nested_patterns })
    }

    pub(crate) fn new_child(parent_scope_id: ScopeId, context: &mut BlockContext) -> Self {
        let scope_id = context.create_child_scope(parent_scope_id);
        Conjunction { scope_id, constraints: Constraints::new(scope_id), nested_patterns: Vec::new() }
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

    pub(crate) fn add_disjunction(&mut self, context: &mut BlockContext) -> &mut Disjunction {
        let disjunction = Disjunction::new_child(self.scope_id, context);
        self.nested_patterns.push(NestedPattern::Disjunction(disjunction));
        self.nested_patterns.last_mut().unwrap().as_disjunction_mut().unwrap()
    }

    pub(crate) fn add_negation(&mut self, context: &mut BlockContext) -> &mut Negation {
        let negation = Negation::new_child(self.scope_id, context);
        self.nested_patterns.push(NestedPattern::Negation(negation));
        self.nested_patterns.last_mut().unwrap().as_negation_mut().unwrap()
    }

    pub(crate) fn add_optional(&mut self, context: &mut BlockContext) -> &mut Optional {
        let optional = Optional::new_child(self.scope_id, context);
        self.nested_patterns.push(NestedPattern::Optional(optional));
        self.nested_patterns.last_mut().unwrap().as_optional_mut().unwrap()
    }

    pub fn get_or_declare_variable(
        &mut self,
        context: &mut BlockContext,
        name: &str,
    ) -> Result<Variable, PatternDefinitionError> {
        context.get_or_declare_variable_named(name, self.scope_id())
    }
}

impl Scope for Conjunction {
    fn scope_id(&self) -> ScopeId {
        self.scope_id
    }
}

impl fmt::Display for Conjunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let current_width = f.width().unwrap_or(0);
        let indent = " ".repeat(current_width);
        writeln!(f, "{}{} Conjunction", indent, self.scope_id)?;
        write!(f, "{:>width$}", &self.constraints, width = current_width + 2)?;
        for pattern in &self.nested_patterns {
            write!(f, "{:>width$}", pattern, width = current_width + 2)?;
        }
        // write!(f, "{}", self.constraints.context())?;
        Ok(())
    }
}
