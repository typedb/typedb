/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::fmt;

use answer::variable::Variable;

use super::{
    constraint::{Constraints, ConstraintsBuilder},
    disjunction::{Disjunction, DisjunctionBuilder},
    negation::Negation,
    nested_pattern::NestedPattern,
    optional::Optional,
    Scope, ScopeId,
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

    pub fn build_from_typeql_patterns(
        context: &mut BlockContext,
        scope_id: ScopeId,
        patterns: &[typeql::Pattern],
    ) -> Result<Self, PatternDefinitionError> {
        let mut conjunction = Conjunction::new(scope_id);
        ConjunctionBuilder::new(context, &mut conjunction).and_typeql_patterns(patterns)?;
        Ok(conjunction)
    }

    pub fn constraints(&self) -> &Constraints {
        &self.constraints
    }

    pub fn nested_patterns(&self) -> &Vec<NestedPattern> {
        &self.nested_patterns
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

pub struct ConjunctionBuilder<'cx> {
    context: &'cx mut BlockContext,
    conjunction: &'cx mut Conjunction,
}

impl<'cx> ConjunctionBuilder<'cx> {
    pub fn new(context: &'cx mut BlockContext, conjunction: &'cx mut Conjunction) -> Self {
        Self { context, conjunction }
    }

    pub fn constraints_mut(&mut self) -> ConstraintsBuilder<'_> {
        ConstraintsBuilder::new(self.context, &mut self.conjunction.constraints)
    }

    pub fn and_typeql_patterns(self, patterns: &[typeql::Pattern]) -> Result<Self, PatternDefinitionError> {
        patterns.iter().try_fold(self, |mut this, item| match item {
            typeql::Pattern::Conjunction(conjunction) => this.and_typeql_patterns(patterns),
            typeql::Pattern::Disjunction(disjunction) => this.and_typeql_disjunction(disjunction),
            typeql::Pattern::Negation(negation) => this.and_typeql_negation(negation),
            typeql::Pattern::Optional(optional) => this.and_typeql_optional(optional),
            typeql::Pattern::Statement(stmt) => {
                this.constraints_mut().extend_from_typeql_statement(stmt)?;
                Ok(this)
            }
        })
    }

    fn and_typeql_disjunction(
        self,
        disjunction: &typeql::pattern::Disjunction,
    ) -> Result<Self, PatternDefinitionError> {
        let disjunction = Disjunction::build_child_from_typeql_patterns(
            self.context,
            self.conjunction.scope_id,
            &disjunction.branches,
        )?;
        self.conjunction.nested_patterns.push(NestedPattern::Disjunction(disjunction));
        Ok(self)
    }

    fn and_typeql_negation(self, negation: &typeql::pattern::Negation) -> Result<Self, PatternDefinitionError> {
        let negation =
            Negation::build_child_from_typeql_patterns(self.context, self.conjunction.scope_id, &negation.patterns)?;
        self.conjunction.nested_patterns.push(NestedPattern::Negation(negation));
        Ok(self)
    }

    fn and_typeql_optional(self, optional: &typeql::pattern::Optional) -> Result<Self, PatternDefinitionError> {
        let optional =
            Optional::build_child_from_typeql_patterns(self.context, self.conjunction.scope_id, &optional.patterns)?;
        self.conjunction.nested_patterns.push(NestedPattern::Optional(optional));
        Ok(self)
    }

    pub fn add_disjunction(&mut self) -> DisjunctionBuilder<'_> {
        let disjunction = Disjunction::new();
        self.conjunction.nested_patterns.push(NestedPattern::Disjunction(disjunction));
        let Some(NestedPattern::Disjunction(disjunction)) = self.conjunction.nested_patterns.last_mut() else {
            unreachable!()
        };
        DisjunctionBuilder::new(self.context, self.conjunction.scope_id, disjunction)
    }

    pub fn get_or_declare_variable(&mut self, name: &str) -> Result<Variable, PatternDefinitionError> {
        self.context.get_or_declare_variable_named(name, self.conjunction.scope_id)
    }
}
