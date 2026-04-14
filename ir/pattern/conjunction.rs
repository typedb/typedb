/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::HashMap,
    fmt,
    hash::{DefaultHasher, Hasher},
};
use answer::variable::Variable;
use itertools::Itertools;
use structural_equality::StructuralEquality;
use typeql::common::Span;

use crate::{
    pattern::{
        constraint::{Constraint, Constraints, ConstraintsBuilder, Unsatisfiable},
        disjunction::DisjunctionBuilder,
        nested_pattern::NestedPattern,
        BindingMode, Pattern, Scope, ScopeId,
    },
    pipeline::block::{BlockBuilderContext, ScopeType},
    RepresentationError,
};
use crate::pattern::ContextualisedBindingMode;
use crate::pattern::negation::NegationBuilder;
use crate::pattern::optional::OptionalBuilder;

#[derive(Debug, Clone)]
pub struct Conjunction {
    scope_id: ScopeId,
    constraints: Constraints,
    nested_patterns: Vec<NestedPattern>,
    binding_modes: ContextualisedBindingMode,
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
        let binding_modes = self.binding_modes.clone();
        let mut swapped_conjunction = Self {
            scope_id,
            constraints: Constraints::new(scope_id),
            nested_patterns: Vec::new(),
            binding_modes,
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
}

impl Pattern for Conjunction {
    fn visible_referenced_variables(&self) -> impl Iterator<Item = Variable> + '_ {
        self.binding_modes.visible_referenced_variables()
    }

    fn required_inputs(&self) -> impl Iterator<Item=Variable> + '_ {
        self.binding_modes.required_inputs()
    }

    fn TEST_ONLY_contextualised_binding_modes(&self) -> &HashMap<Variable, BindingMode> {
        &self.binding_modes.0
    }
}

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
    // TODO: Remove context out of here, and accept it as parameter where needed.
    // Then do the nice stuff of creating separate builders. Get Scope out of Constraints while you're there
    scope_id: ScopeId,
    constraints: Constraints,
    nested_patterns: Vec<NestedPatternBuilder>,
    // binding_modes: HashMap<Variable, BindingMode>,
}

impl ConjunctionBuilder {
    pub fn new(scope_id: ScopeId) -> Self {
        Self { constraints: Constraints::new(scope_id), scope_id, nested_patterns: Vec::new() } //, binding_modes: HashMap::new() }
    }

    pub(crate) fn finish(self, parent_modes: &ContextualisedBindingMode) -> Conjunction {
        let binding_modes = ContextualisedBindingMode::from(self.variable_binding_modes(), parent_modes);
        let Self { scope_id, constraints, nested_patterns } = self;
        let nested_patterns = nested_patterns.into_iter().map(|builder| builder.finish(&binding_modes)).collect();
        Conjunction { scope_id, constraints, nested_patterns, binding_modes }
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

    pub fn constraints_mut<'ctx, 'reg>(&'ctx mut self, context: &'ctx mut BlockBuilderContext<'reg>) -> ConstraintsBuilder<'ctx, 'reg> {
        ConstraintsBuilder::new(context, &mut self.constraints)
    }

    pub fn add_disjunction<'reg>(&mut self, context: &mut BlockBuilderContext<'reg>) -> &mut DisjunctionBuilder {
        let nested_scope_id = context.create_child_scope(self.scope_id, ScopeType::Disjunction);
        self.nested_patterns.push(NestedPatternBuilder::Disjunction(DisjunctionBuilder::new(nested_scope_id)));
        let NestedPatternBuilder::Disjunction(builder) = self.nested_patterns.last_mut().unwrap() else {
            unreachable!();
        };
        builder
    }

    pub fn add_negation<'reg>(&mut self, context: &mut BlockBuilderContext<'reg>) -> &mut NegationBuilder {
        let nested_scope_id = context.create_child_scope(self.scope_id, ScopeType::Negation);
        let negation = NegationBuilder::new(nested_scope_id);
        self.nested_patterns.push(NestedPatternBuilder::Negation(negation));
        let Some(NestedPatternBuilder::Negation(builder)) = self.nested_patterns.last_mut() else {
            unreachable!()
        };
        builder
    }

    pub fn add_optional(
        &mut self,
        source_span: Option<Span>,
        context: &mut BlockBuilderContext<'_>,
    ) ->  Result<&mut OptionalBuilder, RepresentationError> {
        let nested_scope_id = context.create_child_scope(self.scope_id, ScopeType::Optional);
        let conjunction = ConjunctionBuilder::new(nested_scope_id);
        let optional = OptionalBuilder::new(nested_scope_id, context.next_branch_id());
        self.validate_optional_not_in_negation(&optional, source_span, context)?;
        self.nested_patterns.push(NestedPatternBuilder::Optional(optional));
        let Some(NestedPatternBuilder::Optional(optional)) = self.nested_patterns.last_mut() else {
            unreachable!()
        };
        Ok(optional)
    }

    fn validate_optional_not_in_negation(
        &self,
        optional: &OptionalBuilder,
        source_span: Option<Span>,
        context: & mut BlockBuilderContext<'_>,
    ) -> Result<(), RepresentationError> {
        let mut scope = optional.conjunction().scope_id;
        while let Some(parent_scope) = context.get_parent_scope(scope) {
            let parent_scope_type = context.get_scope_type(parent_scope);
            if parent_scope_type == ScopeType::Negation {
                return Err(RepresentationError::OptionalInNegation { source_span });
            } else {
                scope = parent_scope;
            }
        }
        Ok(())
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
