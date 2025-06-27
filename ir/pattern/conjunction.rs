/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{hash_map, HashMap},
    fmt,
    hash::{DefaultHasher, Hasher},
    ops::ControlFlow,
};

use answer::variable::Variable;
use error::unimplemented_feature;
use itertools::Itertools;
use structural_equality::StructuralEquality;
use typeql::common::Span;

use crate::{pattern::{
    constraint::{Constraint, Constraints, ConstraintsBuilder, Unsatisfiable},
    disjunction::{Disjunction, DisjunctionBuilder},
    negation::Negation,
    nested_pattern::NestedPattern,
    optional::Optional,
    Scope, ScopeId, VariableBindingMode,
}, pipeline::block::{BlockBuilderContext, BlockContext}, RepresentationError};
use crate::pattern::variable_category::VariableOptionality;
use crate::pipeline::block::ScopeType;

#[derive(Debug, Clone)]
pub struct Conjunction {
    scope_id: ScopeId,
    constraints: Constraints,
    nested_patterns: Vec<NestedPattern>,
}

impl Conjunction {
    pub fn new(scope_id: ScopeId) -> Self {
        Self { scope_id, constraints: Constraints::new(scope_id), nested_patterns: Vec::new() }
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
        self.referenced_variables().filter(|var| block_context.is_in_scope_or_parent(self.scope_id, *var))
    }

    pub fn referenced_variables(&self) -> impl Iterator<Item = Variable> + '_ {
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

    pub fn named_visible_binding_variables(&self, block_context: &BlockContext) -> impl Iterator<Item = Variable> + '_ {
        self.visible_binding_variables(block_context).filter(Variable::is_named)
    }

    fn visible_binding_variables(&self, block_context: &BlockContext) -> impl Iterator<Item = Variable> + '_ {
        self.variable_binding_modes().into_iter().filter_map(|(v, mode)| {
            (mode.is_always_binding() || mode.is_optionally_binding()).then_some(v)
        })
    }

    pub fn required_inputs(&self, block_context: &BlockContext) -> impl Iterator<Item = Variable> + '_ {
        self.variable_binding_modes().into_iter().filter_map(|(v, mode)| mode.is_non_binding().then_some(v))
    }

    pub fn variable_binding_modes(&self) -> HashMap<Variable, VariableBindingMode<'_>> {
        let mut binding_modes = self.constraints.variable_binding_modes();
        for nested in self.nested_patterns.iter() {
            let nested_pattern_modes = nested.variable_binding_modes();
            for (var, mode) in nested_pattern_modes {
                match binding_modes.entry(var) {
                    hash_map::Entry::Occupied(mut entry) => {
                        // Eg. if it's binding in one part of the conjunction, but non-binding in another, it's still binding
                        //   in this whole conjunction.
                        *entry.get_mut() &= mode
                    },
                    hash_map::Entry::Vacant(vacant_entry) => {
                        vacant_entry.insert(mode);
                    }
                }
            }
        }
        binding_modes
    }

    pub(crate) fn find_disjoint_variable(&self, block_context: &BlockContext) -> ControlFlow<(Variable, Option<Span>)> {
        for (var, mode) in self.variable_binding_modes() {
            let scope = block_context.get_declaring_scope(&var).unwrap();
            // variables present in sibling scopes are "declared" in their common ancestor
            // variables are only considered locally binding in child if the variable is locally bound in all children it is present in
            //   because locally-binding loses to both non-binding and binding modes
            // therefore: fail if the variable is only bound in >=1 child, but "declared" here
            if scope == self.scope_id && (mode.is_locally_binding_in_child() || mode.is_optionally_binding()) {
                return ControlFlow::Break((var, mode.referencing_constraints().first().and_then(|c| c.source_span())));
            }

            // TODO: this check currently won't catch ( ( A(x) ) or ( B(x) ) ) & ( C(x) or D(y) ) ??
            //   --> actually this might be find, it's equal to Z(x) & ( C(x) or D(y) ) where x is bound outside...
        }
        for nested in &self.nested_patterns {
            nested.find_disjoint_variable(block_context)?;
        }
        ControlFlow::Continue(())
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
    pub(crate) context: &'cx mut BlockBuilderContext<'reg>,
    pub(crate) conjunction: &'cx mut Conjunction,
}

impl<'cx, 'reg> ConjunctionBuilder<'cx, 'reg> {
    pub fn new(context: &'cx mut BlockBuilderContext<'reg>, conjunction: &'cx mut Conjunction) -> Self {
        Self { context, conjunction }
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

    pub fn add_optional(&mut self, source_span: Option<Span>) -> Result<ConjunctionBuilder<'_, 'reg>, RepresentationError> {
        let nested_scope_id =
            self.context.create_child_scope(self.conjunction.scope_id, ScopeType::Optional);
        let optional = Optional::new(nested_scope_id, self.context.next_branch_id());
        self.validate_optional_not_in_negation(&optional, source_span)?;
        self.conjunction.nested_patterns.push(NestedPattern::Optional(optional));
        let Some(NestedPattern::Optional(optional)) = self.conjunction.nested_patterns.last_mut() else {
            unreachable!()
        };
        Ok(Optional::new_builder(self.context, optional))
    }

    fn validate_optional_not_in_negation(&self, optional: &Optional, source_span: Option<Span>) -> Result<(), RepresentationError> {
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
}
