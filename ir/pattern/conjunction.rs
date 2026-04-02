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
        disjunction::{Disjunction, DisjunctionBuilder},
        negation::Negation,
        nested_pattern::NestedPattern,
        optional::Optional,
        BindingMode, Pattern, Scope, ScopeId,
    },
    pipeline::block::{BlockBuilderContext, BlockContext, ScopeType},
    RepresentationError,
};

#[derive(Debug, Clone)]
pub struct Conjunction {
    scope_id: ScopeId,
    constraints: Constraints,
    nested_patterns: Vec<NestedPattern>,
    binding_modes: HashMap<Variable, BindingMode>,
}

impl Conjunction {
    pub fn new(scope_id: ScopeId) -> Self {
        Self {
            scope_id,
            constraints: Constraints::new(scope_id),
            nested_patterns: Vec::new(),
            binding_modes: HashMap::new(),
        }
    }

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
        let mut swapped_conjunction = Self::new(self.scope_id);
        std::mem::swap(self, &mut swapped_conjunction);
        self.constraints.constraints_mut().push(Constraint::Unsatisfiable(Unsatisfiable::new(swapped_conjunction)));
    }

    pub fn is_set_to_unsatisfiable(&self) -> bool {
        match self.constraints().iter().exactly_one() {
            Ok(Constraint::Unsatisfiable(_)) => true,
            Ok(_) | Err(_) => false,
        }
    }

    pub fn local_variables<'a>(&'a self, block_context: &'a BlockContext) -> impl Iterator<Item = Variable> + 'a {
        self.referenced_variables().filter(|var| block_context.is_variable_available_in(self.scope_id, *var))
    }
}

impl Pattern for Conjunction {
    fn referenced_variables(&self) -> impl Iterator<Item = Variable> + '_ {
        self.constraints()
            .iter()
            .flat_map(|constraint| constraint.ids())
            .chain(self.nested_patterns.iter().flat_map(|nested| -> Box<dyn Iterator<Item = Variable>> {
                match nested {
                    NestedPattern::Disjunction(disjunction) => Box::new(disjunction.referenced_variables()),
                    NestedPattern::Negation(negation) => Box::new(negation.referenced_variables()),
                    NestedPattern::Optional(optional) => Box::new(optional.referenced_variables()),
                }
            }))
            .unique()
    }

    fn variable_binding_modes(&self) -> HashMap<Variable, BindingMode> {
        self.binding_modes.clone()
        // let mut binding_modes = self.constraints.variable_binding_modes();
        // for nested in self.nested_patterns.iter() {
        //     let nested_pattern_modes = nested.variable_binding_modes();
        //     for (var, mode) in nested_pattern_modes {
        //         *binding_modes.entry(var).or_default() &= mode;
        //     }
        // }
        // binding_modes
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

pub struct ConjunctionBuilder<'cx, 'reg> {
    context: &'cx mut BlockBuilderContext<'reg>,
    conjunction: &'cx mut Conjunction,
}

impl<'cx, 'reg> ConjunctionBuilder<'cx, 'reg> {
    pub fn new(context: &'cx mut BlockBuilderContext<'reg>, conjunction: &'cx mut Conjunction) -> Self {
        Self { context, conjunction }
    }

    pub fn scope_id(&self) -> ScopeId {
        self.conjunction.scope_id()
    }

    pub fn constraints_mut(&mut self) -> ConstraintsBuilder<'_, 'reg> {
        ConstraintsBuilder::new(self.context, &mut self.conjunction.constraints)
    }

    pub fn add_disjunction(&mut self) -> DisjunctionBuilder<'_, 'reg> {
        let nested_scope_id = self.context.create_child_scope(self.conjunction.scope_id, ScopeType::Disjunction);
        self.conjunction.nested_patterns.push(NestedPattern::Disjunction(Disjunction::new(nested_scope_id)));
        let disjunction =
            self.conjunction.nested_patterns.last_mut().and_then(NestedPattern::as_disjunction_mut).unwrap();
        DisjunctionBuilder::new(self.context, nested_scope_id, disjunction)
    }

    pub fn add_negation(&mut self) -> ConjunctionBuilder<'_, 'reg> {
        let nested_scope_id = self.context.create_child_scope(self.conjunction.scope_id, ScopeType::Negation);
        let negation = Negation::new(nested_scope_id);
        self.conjunction.nested_patterns.push(NestedPattern::Negation(negation));
        let Some(NestedPattern::Negation(negation)) = self.conjunction.nested_patterns.last_mut() else {
            unreachable!()
        };
        Negation::new_builder(self.context, negation)
    }

    pub fn add_optional(
        &mut self,
        source_span: Option<Span>,
    ) -> Result<ConjunctionBuilder<'_, 'reg>, RepresentationError> {
        let nested_scope_id = self.context.create_child_scope(self.conjunction.scope_id, ScopeType::Optional);
        let optional = Optional::new(nested_scope_id, self.context.next_branch_id());
        self.validate_optional_not_in_negation(&optional, source_span)?;
        self.conjunction.nested_patterns.push(NestedPattern::Optional(optional));
        let Some(NestedPattern::Optional(optional)) = self.conjunction.nested_patterns.last_mut() else {
            unreachable!()
        };
        Ok(Optional::new_builder(self.context, optional))
    }

    fn validate_optional_not_in_negation(
        &self,
        optional: &Optional,
        source_span: Option<Span>,
    ) -> Result<(), RepresentationError> {
        let mut scope = optional.scope_id();
        while let Some(parent_scope) = self.context.get_parent_scope(scope) {
            let parent_scope_type = self.context.get_scope_type(parent_scope);
            if parent_scope_type == ScopeType::Negation {
                return Err(RepresentationError::OptionalInNegation { source_span });
            } else {
                scope = parent_scope;
            }
        }
        Ok(())
    }

    pub(crate) fn compute_and_set_variable_optionality(&mut self) {
        let Self { conjunction, context } = self;
        conjunction
            .nested_patterns()
            .iter()
            .filter_map(|nested| match nested {
                NestedPattern::Optional(optional) => Some(optional),
                _ => None,
            })
            .for_each(|optional| {
                for var in optional.conjunction().referenced_variables() {
                    // if the variable is available in the parent scope, it's bound externally and passed in so not optional
                    if !context.is_variable_available_in(conjunction.scope_id(), var) {
                        context.set_variable_optionality(var, true)
                    }
                }
            });
    }

    pub(crate) fn compute_and_set_variable_binding_modes(&mut self) {
        compute_bottom_up_binding_modes_and_set(&mut self.conjunction);
        self.conjunction.binding_modes.iter_mut().for_each(|(var, mode)| {
            if self.context.is_variable_input(*var) {
                // TODO: Should this optional if it's an optional input? I don't think so.
                *mode = BindingMode::AlwaysBinding;
            }
        });
        update_required_for_locally_and_optionally_binding(&mut self.conjunction);
    }
}

fn update_required_for_locally_and_optionally_binding(conjunction: &mut Conjunction) {
    fn find_variables_to_set<'a>(
        parent_modes: &HashMap<Variable, BindingMode>,
        nested_modes: impl Iterator<Item = (&'a Variable, &'a BindingMode)>,
    ) -> Vec<Variable> {
        nested_modes
            .into_iter()
            .filter(|(var, nested_mode)| {
                debug_assert!(parent_modes.contains_key(var));
                let parent_mode = parent_modes[var];
                (nested_mode.is_optionally_binding() || nested_mode.is_locally_binding_in_child())
                    && (parent_modes[var].is_require_prebound() || parent_modes[var].is_always_binding())
            })
            .map(|(var, _)| *var)
            .collect::<Vec<_>>()
    }

    fn set_for_variables(nested_modes: &mut HashMap<Variable, BindingMode>, variables: &[Variable]) {
        variables.iter().for_each(|variable| {
            if let Some(mode) = nested_modes.get_mut(variable) {
                *mode = BindingMode::RequirePrebound;
            }
        })
    }

    let Conjunction { nested_patterns, binding_modes, .. } = conjunction;

    for nested in nested_patterns.iter_mut() {
        match nested {
            NestedPattern::Disjunction(disjunction) => {
                let to_set = find_variables_to_set(binding_modes, disjunction.variable_binding_modes().iter());
                disjunction.conjunctions_mut().iter_mut().for_each(|c| {
                    set_for_variables(&mut c.binding_modes, &to_set);
                    update_required_for_locally_and_optionally_binding(c);
                });
            }
            NestedPattern::Negation(negation) => {
                let to_set = find_variables_to_set(binding_modes, negation.variable_binding_modes().iter());
                set_for_variables(&mut negation.conjunction_mut().binding_modes, &to_set);
                update_required_for_locally_and_optionally_binding(negation.conjunction_mut());
            }
            NestedPattern::Optional(optional) => {
                let to_set = find_variables_to_set(binding_modes, optional.variable_binding_modes().iter());
                set_for_variables(&mut optional.conjunction_mut().binding_modes, &to_set);
                update_required_for_locally_and_optionally_binding(optional.conjunction_mut());
            }
        }
    }
}

pub(super) fn compute_bottom_up_binding_modes_and_set(conjunction: &mut Conjunction) {
    fn update_binding_modes(
        of: &mut HashMap<Variable, BindingMode>,
        from: impl Iterator<Item = (Variable, BindingMode)>,
    ) {
        for (var, mode) in from {
            *of.entry(var).or_default() &= mode;
        }
    }
    let Conjunction { constraints, nested_patterns, binding_modes, .. } = conjunction;

    update_binding_modes(binding_modes, constraints.variable_binding_modes().into_iter());
    for nested in nested_patterns.iter_mut() {
        match nested {
            NestedPattern::Disjunction(disjunction) => {
                disjunction.conjunctions_mut().iter_mut().for_each(|c| {
                    compute_bottom_up_binding_modes_and_set(c);
                });
                update_binding_modes(binding_modes, disjunction.variable_binding_modes().into_iter());
            }
            NestedPattern::Negation(negation) => {
                compute_bottom_up_binding_modes_and_set(negation.conjunction_mut());
                update_binding_modes(binding_modes, negation.variable_binding_modes().into_iter());
            }
            NestedPattern::Optional(optional) => {
                compute_bottom_up_binding_modes_and_set(optional.conjunction_mut());
                update_binding_modes(binding_modes, optional.variable_binding_modes().into_iter());
            }
        }
    }
}
