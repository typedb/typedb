/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{HashMap, hash_map},
    fmt,
    hash::{DefaultHasher, Hasher},
};

use answer::variable::Variable;
use itertools::Itertools;
use structural_equality::StructuralEquality;
use typeql::common::Span;

use crate::{
    RepresentationError,
    pattern::{
        BindingMode, ContextualisedBindingMode, Pattern, PatternVariables, Scope, ScopeId,
        constraint::{Constraint, Constraints, ConstraintsBuilder, Unsatisfiable},
        disjunction::{DisjunctionBuilder, DisjunctionBuilderWithContext},
        impl_pattern_from_pattern_variables,
        negation::NegationBuilder,
        nested_pattern::NestedPattern,
        optional::OptionalBuilder,
    },
    pipeline::block::BlockBuilderContext,
};

#[derive(Debug, Clone)]
pub struct Conjunction {
    scope_id: ScopeId,
    constraints: Constraints,
    nested_patterns: Vec<NestedPattern>,
    pattern_variables: PatternVariables,
}

impl Conjunction {
    pub fn constraints(&self) -> &[Constraint<Variable>] {
        self.constraints.constraints()
    }

    pub fn constraints_mut(&mut self) -> &mut Constraints {
        &mut self.constraints
    }

    pub fn nested_patterns(&self) -> &[NestedPattern] {
        &self.nested_patterns
    }

    pub fn nested_patterns_mut(&mut self) -> &mut [NestedPattern] {
        &mut self.nested_patterns
    }

    pub fn set_unsatisfiable(&mut self) {
        let scope_id = self.scope_id;
        let binding_modes = self.pattern_variables.clone();
        let mut swapped_conjunction = Self {
            scope_id,
            constraints: Constraints::new(scope_id),
            nested_patterns: Vec::new(),
            pattern_variables: binding_modes,
        };
        std::mem::swap(self, &mut swapped_conjunction);
        self.constraints.constraints_mut().push(Constraint::Unsatisfiable(Unsatisfiable::new(swapped_conjunction)));
    }

    pub fn is_set_to_unsatisfiable(&self) -> bool {
        match self.constraints().iter().exactly_one() {
            Ok(Constraint::Unsatisfiable(_)) => true,
            Ok(_) | Err(_) => false,
        }
    }

    pub fn register_variable_copy(&mut self, source: Variable, copy: Variable) {
        if let Some(value) = self.pattern_variables.0.get(&source).cloned() {
            self.pattern_variables.0.insert(copy, value);
        }
    }
}

impl_pattern_from_pattern_variables!(Conjunction);

impl Scope for Conjunction {
    fn scope_id(&self) -> ScopeId {
        self.scope_id
    }
}

impl StructuralEquality for Conjunction {
    fn hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.constraints().hash_into(&mut hasher);
        self.nested_patterns().hash_into(&mut hasher);
        hasher.finish()
    }

    fn equals(&self, other: &Self) -> bool {
        self.constraints().equals(other.constraints()) && self.nested_patterns().equals(other.nested_patterns())
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

#[derive(Debug)]
pub(crate) enum NestedPatternBuilder {
    Disjunction(DisjunctionBuilder),
    Negation(NegationBuilder),
    Optional(OptionalBuilder),
}

impl NestedPatternBuilder {
    pub(crate) fn finish(self, parent_modes: &ContextualisedBindingMode) -> NestedPattern {
        match self {
            NestedPatternBuilder::Disjunction(disjunction) => disjunction.finish(parent_modes),
            NestedPatternBuilder::Negation(negation) => negation.finish(parent_modes),
            NestedPatternBuilder::Optional(optional) => optional.finish(parent_modes),
        }
    }
}

impl NestedPatternBuilder {
    fn variable_binding_modes(&self) -> HashMap<Variable, BindingMode> {
        match self {
            NestedPatternBuilder::Disjunction(inner) => inner.variable_binding_modes(),
            NestedPatternBuilder::Negation(inner) => inner.variable_binding_modes(),
            NestedPatternBuilder::Optional(inner) => inner.variable_binding_modes(),
        }
    }
}

#[derive(Debug)]
pub struct ConjunctionBuilder {
    scope_id: ScopeId,
    constraints: Constraints,
    nested_patterns: Vec<NestedPatternBuilder>,
}

impl ConjunctionBuilder {
    pub fn new(scope_id: ScopeId) -> Self {
        Self { constraints: Constraints::new(scope_id), scope_id, nested_patterns: Vec::new() }
    }

    pub(crate) fn finish(self, parent_modes: &ContextualisedBindingMode) -> Conjunction {
        let binding_modes = ContextualisedBindingMode::from(self.variable_binding_modes(), parent_modes);
        let Self { scope_id, constraints, nested_patterns } = self;
        let nested_patterns = nested_patterns.into_iter().map(|builder| builder.finish(&binding_modes)).collect();
        let variable_requirements = PatternVariables::from(&binding_modes);
        Conjunction { scope_id, constraints, nested_patterns, pattern_variables: variable_requirements }
    }

    pub fn scope_id(&self) -> ScopeId {
        self.scope_id
    }

    pub fn constraints(&self) -> &[Constraint<Variable>] {
        self.constraints.constraints()
    }

    pub(crate) fn nested_patterns(&self) -> &[NestedPatternBuilder] {
        &self.nested_patterns
    }

    pub(crate) fn variable_binding_modes(&self) -> HashMap<Variable, BindingMode> {
        let mut binding_modes = self.constraints.variable_binding_modes();
        for nested in self.nested_patterns.iter() {
            let nested_pattern_modes = nested.variable_binding_modes();
            for (var, mode) in nested_pattern_modes {
                *binding_modes.entry(var).or_default() &= mode;
            }
        }
        binding_modes
    }
}

#[derive(Debug)]
pub struct ConjunctionBuilderWithContext<'ctx, 'reg> {
    context: &'ctx mut BlockBuilderContext<'reg>,
    conjunction: &'ctx mut ConjunctionBuilder,
}

impl<'ctx, 'reg> ConjunctionBuilderWithContext<'ctx, 'reg> {
    pub(crate) fn new(context: &'ctx mut BlockBuilderContext<'reg>, conjunction: &'ctx mut ConjunctionBuilder) -> Self {
        Self { context, conjunction }
    }

    pub fn constraints_mut(&mut self) -> ConstraintsBuilder<'_, 'reg> {
        ConstraintsBuilder::new(&mut self.context, &mut self.conjunction.constraints)
    }

    pub fn add_disjunction(&mut self) -> DisjunctionBuilderWithContext<'_, 'reg> {
        let nested_scope_id = self.context.next_scope_id();
        self.conjunction
            .nested_patterns
            .push(NestedPatternBuilder::Disjunction(DisjunctionBuilder::new(nested_scope_id)));
        let NestedPatternBuilder::Disjunction(builder) = self.conjunction.nested_patterns.last_mut().unwrap() else {
            unreachable!();
        };
        DisjunctionBuilderWithContext::new(&mut self.context, builder)
    }

    pub fn add_negation(&mut self) -> ConjunctionBuilderWithContext<'_, 'reg> {
        let nested_scope_id = self.context.next_scope_id();
        let negation = NegationBuilder::new(nested_scope_id);
        self.conjunction.nested_patterns.push(NestedPatternBuilder::Negation(negation));
        let Some(NestedPatternBuilder::Negation(builder)) = self.conjunction.nested_patterns.last_mut() else {
            unreachable!()
        };
        ConjunctionBuilderWithContext::new(&mut self.context, builder.conjunction_mut())
    }

    pub fn add_optional(
        &mut self,
        source_span: Option<Span>,
    ) -> Result<ConjunctionBuilderWithContext<'_, 'reg>, RepresentationError> {
        let nested_scope_id = self.context.next_scope_id();
        let conjunction = ConjunctionBuilder::new(nested_scope_id);
        let optional = OptionalBuilder::new(nested_scope_id, self.context.next_branch_id());
        self.conjunction.nested_patterns.push(NestedPatternBuilder::Optional(optional));
        let Some(NestedPatternBuilder::Optional(optional)) = self.conjunction.nested_patterns.last_mut() else {
            unreachable!()
        };
        Ok(ConjunctionBuilderWithContext::new(&mut self.context, optional.conjunction_mut()))
    }
}
