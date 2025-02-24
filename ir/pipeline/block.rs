/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::{HashMap, HashSet};

use answer::variable::Variable;
use itertools::Itertools;
use structural_equality::StructuralEquality;
use typeql::common::Span;

use crate::{
    pattern::{
        conjunction::{Conjunction, ConjunctionBuilder},
        constraint::Constraint,
        variable_category::VariableCategory,
        Scope, ScopeId,
    },
    pipeline::{ParameterRegistry, VariableCategorySource, VariableRegistry},
    RepresentationError,
};

#[derive(Debug, Clone)]
pub struct Block {
    block_context: BlockContext, // TODO: We only need this for type annotations
    conjunction: Conjunction,
}

impl Block {
    pub fn builder(context: BlockBuilderContext<'_>) -> BlockBuilder<'_> {
        BlockBuilder::new(context)
    }

    pub fn conjunction(&self) -> &Conjunction {
        &self.conjunction
    }

    pub fn conjunction_mut(&mut self) -> &mut Conjunction {
        &mut self.conjunction
    }

    pub fn block_context(&self) -> &BlockContext {
        &self.block_context
    }

    pub fn scope_id(&self) -> ScopeId {
        Scope::scope_id(self)
    }

    fn variable_scopes(&self) -> impl Iterator<Item = (&Variable, &ScopeId)> + '_ {
        self.block_context.variable_declaration.iter()
    }

    pub fn variables(&self) -> impl Iterator<Item = Variable> + '_ {
        self.block_context.referenced_variables.iter().cloned()
    }

    pub fn block_variables(&self) -> impl Iterator<Item = Variable> + '_ {
        self.variable_scopes().filter_map(|(&v, scope)| if scope != &ScopeId::INPUT { Some(v) } else { None })
    }

    pub fn input_variables(&self) -> impl Iterator<Item = Variable> + '_ {
        self.variable_scopes().filter_map(|(&v, scope)| if scope == &ScopeId::INPUT { Some(v) } else { None })
    }

    pub fn into_conjunction(self) -> Conjunction {
        self.conjunction
    }
}

impl Scope for Block {
    fn scope_id(&self) -> ScopeId {
        ScopeId::ROOT
    }
}

impl StructuralEquality for Block {
    fn hash(&self) -> u64 {
        self.conjunction().hash()
    }

    fn equals(&self, other: &Self) -> bool {
        self.conjunction().equals(other.conjunction())
    }
}

#[derive(Debug)]
pub struct BlockBuilder<'reg> {
    context: BlockBuilderContext<'reg>,
    conjunction: Conjunction,
}

impl<'reg> BlockBuilder<'reg> {
    fn new(context: BlockBuilderContext<'reg>) -> Self {
        Self { conjunction: Conjunction::new(ScopeId::ROOT), context }
    }

    pub fn finish(self) -> Result<Block, Box<RepresentationError>> {
        let Self { conjunction, context: BlockBuilderContext { block_context, variable_registry, .. } } = self;
        validate_conjunction(&conjunction, variable_registry)?;
        Ok(Block { conjunction, block_context })
    }

    pub fn conjunction_mut(&mut self) -> ConjunctionBuilder<'_, 'reg> {
        ConjunctionBuilder::new(&mut self.context, &mut self.conjunction)
    }

    pub fn context_mut(&mut self) -> &mut BlockBuilderContext<'reg> {
        &mut self.context
    }
}

fn validate_conjunction(
    conjunction: &Conjunction,
    variable_registry: &VariableRegistry,
) -> Result<(), Box<RepresentationError>> {
    let unbound = conjunction.referenced_variables().find(|&variable| {
        matches!(variable_registry.get_variable_category(variable), Some(VariableCategory::AttributeOrValue) | None)
    });
    if let Some(variable) = unbound {
        return Err(Box::new(RepresentationError::UnboundVariable {
            variable: variable_registry.get_variable_name(variable).cloned().unwrap_or(String::new()),
            source_span: variable_registry.source_span(variable),
        }));
    }
    let is_with_mismatched_category = conjunction.constraints().iter().filter_map(|c| c.as_is()).find_or_first(|is| {
        let lhs_category = variable_registry.get_variable_category(is.lhs().as_variable().unwrap()).unwrap();
        let rhs_category = variable_registry.get_variable_category(is.rhs().as_variable().unwrap()).unwrap();
        lhs_category.narrowest(rhs_category).is_none()
    });
    if let Some(is) = is_with_mismatched_category {
        let lhs = is.lhs().as_variable().unwrap();
        let rhs = is.rhs().as_variable().unwrap();
        let lhs_category = variable_registry.get_variable_category(lhs).unwrap();
        let rhs_category = variable_registry.get_variable_category(rhs).unwrap();
        let lhs_variable = variable_registry.get_variable_name(lhs).map_or("_", |s| s.as_str()).to_owned();
        let rhs_variable = variable_registry.get_variable_name(rhs).map_or("_", |s| s.as_str()).to_owned();
        return Err(Box::new(RepresentationError::VariableCategoryMismatchInIs {
            lhs_variable,
            rhs_variable,
            lhs_category,
            rhs_category,
            source_span: is.source_span(),
        }));
    }
    Ok(())
}

#[derive(Debug, Clone, Default, Eq, PartialEq)]
pub struct BlockContext {
    variable_declaration: HashMap<Variable, ScopeId>,
    scope_parents: HashMap<ScopeId, ScopeId>,
    scope_transparency: HashMap<ScopeId, ScopeTransparency>,
    referenced_variables: HashSet<Variable>,
}

impl BlockContext {
    fn new() -> Self {
        Default::default()
    }

    fn add_input_declaration(&mut self, var: Variable) {
        self.variable_declaration.insert(var, ScopeId::INPUT);
    }

    fn add_variable_declaration(&mut self, var: Variable, scope: ScopeId) {
        self.variable_declaration.insert(var, scope);
        self.add_referenced_variable(var);
    }

    fn set_scope_parent(&mut self, scope_id: ScopeId, parent_scope_id: ScopeId) {
        self.scope_parents.insert(scope_id, parent_scope_id);
    }

    fn may_update_declaration_scope(
        &mut self,
        var: Variable,
        var_name: &str,
        source_span: Option<Span>,
        scope: ScopeId,
    ) -> Result<(), Box<RepresentationError>> {
        debug_assert!(self.variable_declaration.contains_key(&var));
        self.add_referenced_variable(var);
        let recorded_scope = self.variable_declaration[&var];
        if is_equal_or_parent_scope(&self.scope_parents, scope, recorded_scope) {
            // Parent defines same name: ok, reuse the variable
        } else if is_child_scope(&self.scope_parents, scope, recorded_scope) {
            // Child defines the same name: ok, reuse the variable, and change the declaration scope to the current one
            *self.variable_declaration.get_mut(&var).unwrap() = scope;
        } else {
            let ancestor = common_ancestor(&self.scope_parents, recorded_scope, scope);
            if !self.is_visible_child(scope, ancestor) || !self.is_visible_child(recorded_scope, ancestor) {
                return Err(Box::new(RepresentationError::DisjointVariableReuse {
                    name: var_name.to_owned(),
                    source_span,
                }));
            }
            *self.variable_declaration.get_mut(&var).unwrap() = ancestor;
        }
        Ok(())
    }

    pub fn get_scope(&self, var: &Variable) -> Option<ScopeId> {
        self.variable_declaration.get(var).cloned()
    }

    pub(crate) fn is_transparent(&self, scope: ScopeId) -> bool {
        self.scope_transparency[&scope] == ScopeTransparency::Transparent
    }

    fn set_scope_transparency(&mut self, scope: ScopeId, transparency: ScopeTransparency) {
        self.scope_transparency.insert(scope, transparency);
    }

    fn add_referenced_variable(&mut self, var: Variable) {
        self.referenced_variables.insert(var);
    }

    pub fn referenced_variables(&self) -> impl Iterator<Item = Variable> + '_ {
        self.referenced_variables.iter().copied()
    }

    pub fn is_variable_available(&self, scope: ScopeId, variable: Variable) -> bool {
        let variable_scope = self.variable_declaration.get(&variable);
        variable_scope
            .is_some_and(|variable_scope| is_equal_or_parent_scope(&self.scope_parents, scope, *variable_scope))
    }

    pub fn is_child_scope(&self, child: ScopeId, ancestor: ScopeId) -> bool {
        let Some(&parent) = self.scope_parents.get(&child) else { return false };
        parent == ancestor || self.is_child_scope(parent, ancestor)
    }

    pub fn is_visible_child(&self, child: ScopeId, ancestor: ScopeId) -> bool {
        if !self.is_transparent(child) {
            return false;
        }
        let Some(&parent) = self.scope_parents.get(&child) else { return false };
        parent == ancestor || self.is_visible_child(parent, ancestor)
    }

    pub fn get_variable_scopes(&self) -> impl Iterator<Item = (Variable, ScopeId)> + '_ {
        self.variable_declaration.iter().map(|(&var, &scope)| (var, scope))
    }

    pub fn visible_variables(&self, root: ScopeId) -> impl Iterator<Item = Variable> + '_ {
        self.get_variable_scopes().filter_map(move |(var, scope)| {
            (scope == ScopeId::INPUT || scope == root || self.is_visible_child(scope, root)).then_some(var)
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ScopeTransparency {
    Transparent,
    Opaque,
}

#[derive(Debug)]
pub struct BlockBuilderContext<'a> {
    variable_registry: &'a mut VariableRegistry,
    variable_names_index: &'a mut HashMap<String, Variable>,
    parameters: &'a mut ParameterRegistry,

    block_context: BlockContext,
    scope_id_allocator: u16,
}

impl<'a> BlockBuilderContext<'a> {
    pub(crate) fn new(
        variable_registry: &'a mut VariableRegistry,
        available_input_names: &'a mut HashMap<String, Variable>,
        parameters: &'a mut ParameterRegistry,
    ) -> BlockBuilderContext<'a> {
        let mut block_context = BlockContext::new();
        available_input_names.values().for_each(|v| {
            block_context.add_input_declaration(*v);
        });
        block_context.set_scope_parent(ScopeId::ROOT, ScopeId::INPUT);
        block_context.set_scope_transparency(ScopeId::ROOT, ScopeTransparency::Transparent);
        block_context.set_scope_transparency(ScopeId::INPUT, ScopeTransparency::Transparent);
        Self {
            variable_registry,
            variable_names_index: available_input_names,
            parameters,
            scope_id_allocator: 2, // `0`, `1` are reserved for INPUT, ROOT respectively.
            block_context,
        }
    }

    pub fn get_variable_named(&self, name: &str) -> Option<&Variable> {
        self.variable_names_index.get(name)
    }

    pub(crate) fn get_or_declare_variable(
        &mut self,
        name: &str,
        scope: ScopeId,
        source_span: Option<Span>,
    ) -> Result<Variable, Box<RepresentationError>> {
        match self.variable_names_index.get(name) {
            None => {
                let variable = self.variable_registry.register_variable_named(name.to_string(), source_span);
                self.block_context.add_variable_declaration(variable, scope);
                self.variable_names_index.insert(name.to_string(), variable);
                Ok(variable)
            }
            Some(&existing_variable) => {
                self.block_context.may_update_declaration_scope(
                    existing_variable,
                    name,
                    self.variable_registry.source_span(existing_variable),
                    scope,
                )?;
                Ok(existing_variable)
            }
        }
    }

    pub(crate) fn create_anonymous_variable(
        &mut self,
        scope: ScopeId,
        source_span: Option<Span>,
    ) -> Result<Variable, Box<RepresentationError>> {
        let variable = self.variable_registry.register_anonymous_variable(source_span);
        self.block_context.add_variable_declaration(variable, scope);
        Ok(variable)
    }

    pub fn named_variable_mapping(&self) -> &HashMap<String, Variable> {
        self.variable_names_index
    }

    pub fn is_variable_available(&self, scope: ScopeId, variable: Variable) -> bool {
        self.block_context.is_variable_available(scope, variable)
    }

    pub(crate) fn create_child_scope(&mut self, parent: ScopeId, transparency: ScopeTransparency) -> ScopeId {
        let scope = ScopeId::new(self.scope_id_allocator);
        debug_assert_ne!(scope, ScopeId::ROOT);
        self.scope_id_allocator += 1;
        self.block_context.set_scope_parent(scope, parent);
        self.block_context.set_scope_transparency(scope, transparency);
        scope
    }

    pub(crate) fn set_variable_category(
        &mut self,
        variable: Variable,
        category: VariableCategory,
        source: Constraint<Variable>,
    ) -> Result<(), Box<RepresentationError>> {
        self.variable_registry.set_variable_category(variable, category, VariableCategorySource::Constraint(source))
    }

    pub(crate) fn set_variable_is_optional(&mut self, variable: Variable, optional: bool) {
        self.variable_registry.set_variable_is_optional(variable, optional)
    }

    pub fn parameters(&mut self) -> &mut ParameterRegistry {
        self.parameters
    }
}

fn is_equal_or_parent_scope(parents: &HashMap<ScopeId, ScopeId>, scope: ScopeId, maybe_parent: ScopeId) -> bool {
    scope == maybe_parent || parents.get(&scope).is_some_and(|&p| is_equal_or_parent_scope(parents, p, maybe_parent))
}

fn is_child_scope(parents: &HashMap<ScopeId, ScopeId>, scope: ScopeId, maybe_child: ScopeId) -> bool {
    parents.get(&maybe_child).is_some_and(|&c| c == scope || is_child_scope(parents, scope, c))
}

fn common_ancestor(parents: &HashMap<ScopeId, ScopeId>, left: ScopeId, right: ScopeId) -> ScopeId {
    if left == right || is_child_scope(parents, left, right) {
        left
    } else if is_child_scope(parents, right, left) {
        right
    } else {
        common_ancestor(parents, parents[&left], parents[&right])
    }
}
