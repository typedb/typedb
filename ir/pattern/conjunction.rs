/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{hash_map, HashMap},
    fmt,
    hash::{DefaultHasher, Hasher},
};

use answer::variable::Variable;
use error::unimplemented_feature;
use itertools::Itertools;
use structural_equality::StructuralEquality;

use crate::{
    pattern::{
        constraint::{Constraint, Constraints, ConstraintsBuilder, Unsatisfiable},
        disjunction::{Disjunction, DisjunctionBuilder},
        negation::Negation,
        nested_pattern::NestedPattern,
        optional::Optional,
        Scope, ScopeId, VariableDependency,
    },
    pipeline::block::{BlockBuilderContext, BlockContext, ScopeTransparency},
};

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

    pub fn captured_variables<'a>(&'a self, block_context: &'a BlockContext) -> impl Iterator<Item = Variable> + 'a {
        let self_scope = self.scope_id;
        self.referenced_variables().filter(move |var| {
            let scope = block_context.get_scope(var).unwrap();
            self_scope == scope && var.is_named() && block_context.is_transparent(self_scope)
                || block_context.is_child_scope(self_scope, scope)
                || self_scope != scope && var.is_named()
        })
    }

    pub fn referenced_variables(&self) -> impl Iterator<Item = Variable> + '_ {
        self.constraints()
            .iter()
            .flat_map(|constraint| constraint.ids())
            .chain(self.nested_patterns.iter().flat_map(|nested| -> Box<dyn Iterator<Item = Variable>> {
                match nested {
                    NestedPattern::Disjunction(disjunction) => Box::new(disjunction.referenced_variables()),
                    NestedPattern::Negation(negation) => Box::new(negation.referenced_variables()),
                    NestedPattern::Optional(_) => unimplemented_feature!(Optionals),
                }
            }))
            .unique()
    }

    pub fn local_variables<'a>(&self, block_context: &'a BlockContext) -> impl Iterator<Item = Variable> + 'a {
        let self_scope = self.scope_id;
        block_context
            .get_variable_scopes()
            .filter(move |&(var, scope)| {
                scope == self_scope || block_context.is_visible_child(scope, self_scope) && var.is_named()
            })
            .map(|(var, _)| var)
            .unique()
    }

    pub fn named_producible_variables(&self, block_context: &BlockContext) -> impl Iterator<Item = Variable> + '_ {
        self.producible_variables(block_context).filter(Variable::is_named)
    }

    fn producible_variables(&self, block_context: &BlockContext) -> impl Iterator<Item = Variable> + '_ {
        self.variable_dependency(block_context).into_iter().filter_map(|(v, mode)| mode.is_producing().then_some(v))
    }

    pub fn required_inputs(&self, block_context: &BlockContext) -> impl Iterator<Item = Variable> + '_ {
        self.variable_dependency(block_context).into_iter().filter_map(|(v, mode)| mode.is_required().then_some(v))
    }

    pub fn variable_dependency(&self, block_context: &BlockContext) -> HashMap<Variable, VariableDependency<'_>> {
        let mut dependencies = self.constraints.variable_dependency();
        for nested in self.nested_patterns.iter() {
            let nested_pattern_data_modes = nested.variable_dependency(block_context);
            for (var, mode) in nested_pattern_data_modes {
                match dependencies.entry(var) {
                    hash_map::Entry::Occupied(mut entry) => *entry.get_mut() &= mode,
                    hash_map::Entry::Vacant(vacant_entry) => {
                        vacant_entry.insert(mode);
                    }
                }
            }
        }
        dependencies
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

    pub fn constraints_mut(&mut self) -> ConstraintsBuilder<'_, 'reg> {
        ConstraintsBuilder::new(self.context, &mut self.conjunction.constraints)
    }

    pub fn add_disjunction(&mut self) -> DisjunctionBuilder<'_, 'reg> {
        self.conjunction.nested_patterns.push(NestedPattern::Disjunction(Disjunction::new()));
        let disjunction =
            self.conjunction.nested_patterns.last_mut().and_then(NestedPattern::as_disjunction_mut).unwrap();
        DisjunctionBuilder::new(self.context, self.conjunction.scope_id, disjunction)
    }

    pub fn add_negation(&mut self) -> ConjunctionBuilder<'_, 'reg> {
        let nested_scope_id = self.context.create_child_scope(self.conjunction.scope_id, ScopeTransparency::Opaque);
        let negation = Negation::new(nested_scope_id);
        self.conjunction.nested_patterns.push(NestedPattern::Negation(negation));
        let Some(NestedPattern::Negation(negation)) = self.conjunction.nested_patterns.last_mut() else {
            unreachable!()
        };
        Negation::new_builder(self.context, negation)
    }

    pub fn add_optional(&mut self) -> ConjunctionBuilder<'_, 'reg> {
        let nested_scope_id =
            self.context.create_child_scope(self.conjunction.scope_id, ScopeTransparency::Transparent);
        let optional = Optional::new(nested_scope_id);
        self.conjunction.nested_patterns.push(NestedPattern::Optional(optional));
        let Some(NestedPattern::Optional(optional)) = self.conjunction.nested_patterns.last_mut() else {
            unreachable!()
        };
        Optional::new_builder(self.context, optional)
    }
}
