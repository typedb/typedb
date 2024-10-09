/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::{HashMap, HashSet};

use answer::variable::Variable;

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

    pub fn scope_context(&self) -> &BlockContext {
        &self.block_context
    }

    pub fn scope_id(&self) -> ScopeId {
        Scope::scope_id(self)
    }

    pub fn variable_scopes(&self) -> impl Iterator<Item = (&Variable, &ScopeId)> + '_ {
        self.block_context.variable_declaration.iter()
    }

    pub fn block_variables(&self) -> impl Iterator<Item = Variable> + '_ {
        self.variable_scopes().filter_map(|(&v, scope)| if scope != &ScopeId::INPUT { Some(v) } else { None })
    }

    pub fn input_variables(&self) -> impl Iterator<Item = Variable> + '_ {
        self.variable_scopes().filter_map(|(&v, scope)| if scope == &ScopeId::INPUT { Some(v) } else { None })
    }
}

impl Scope for Block {
    fn scope_id(&self) -> ScopeId {
        ScopeId::ROOT
    }
}

pub struct BlockBuilder<'reg> {
    context: BlockBuilderContext<'reg>,
    conjunction: Conjunction,
}

impl<'reg> BlockBuilder<'reg> {
    fn new(context: BlockBuilderContext<'reg>) -> Self {
        Self { conjunction: Conjunction::new(ScopeId::ROOT), context }
    }

    pub fn finish(self) -> Block {
        let Self { conjunction, context: block_context_builder } = self;
        let BlockBuilderContext { block_context, .. } = block_context_builder;
        Block { conjunction, block_context }
    }

    pub fn conjunction_mut(&mut self) -> ConjunctionBuilder<'_, 'reg> {
        ConjunctionBuilder::new(&mut self.context, &mut self.conjunction)
    }

    pub fn context_mut(&mut self) -> &mut BlockBuilderContext<'reg> {
        &mut self.context
    }
}

#[derive(Debug, Clone, Default)]
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
    }

    fn set_scope_parent(&mut self, scope_id: ScopeId, parent_scope_id: ScopeId) {
        self.scope_parents.insert(scope_id, parent_scope_id);
    }

    fn may_update_declaration_scope(
        &mut self,
        var: Variable,
        var_name: &str,
        scope: ScopeId,
    ) -> Result<(), RepresentationError> {
        debug_assert!(self.variable_declaration.contains_key(&var));
        let existing_scope = self.variable_declaration.get_mut(&var).unwrap();
        if is_equal_or_parent_scope(&self.scope_parents, scope, *existing_scope) {
            // Parent defines same name: ok, reuse the variable
            Ok(())
        } else if is_child_scope(&self.scope_parents, scope, *existing_scope) {
            // Child defines the same name: ok, reuse the variable, and change the declaration scope to the current one
            *existing_scope = scope;
            Ok(())
        } else {
            Err(RepresentationError::DisjointVariableReuse { name: var_name.to_string() })
        }
    }

    fn get_scope(&self, var: &Variable) -> Option<ScopeId> {
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
        match variable_scope {
            None => false,
            Some(variable_scope) => is_equal_or_parent_scope(&self.scope_parents, scope, *variable_scope),
        }
    }

    pub fn is_parent_scope(&self, scope: ScopeId, child: ScopeId) -> bool {
        self.scope_parents.get(&child).is_some_and(|&parent| scope == parent || self.is_parent_scope(scope, parent))
    }

    pub fn is_visible_child(&self, child: ScopeId, candidate: ScopeId) -> bool {
        self.is_transparent(child)
            && self
                .scope_parents
                .get(&child)
                .is_some_and(|&parent| candidate == parent || self.is_visible_child(parent, candidate))
    }

    pub fn get_variable_scopes(&self) -> impl Iterator<Item = (Variable, ScopeId)> + '_ {
        self.variable_declaration.iter().map(|(&var, &scope)| (var, scope))
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
        input_variable_names: &'a mut HashMap<String, Variable>,
        parameters: &'a mut ParameterRegistry,
    ) -> BlockBuilderContext<'a> {
        let mut block_context = BlockContext::new();
        input_variable_names.values().for_each(|v| {
            block_context.add_input_declaration(*v);
        });
        block_context.set_scope_parent(ScopeId::ROOT, ScopeId::INPUT);
        block_context.set_scope_transparency(ScopeId::ROOT, ScopeTransparency::Transparent);
        block_context.set_scope_transparency(ScopeId::INPUT, ScopeTransparency::Transparent);
        Self {
            variable_registry,
            variable_names_index: input_variable_names,
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
    ) -> Result<Variable, RepresentationError> {
        match self.variable_names_index.get(name) {
            None => {
                let variable = self.variable_registry.register_variable_named(name.to_string());
                self.block_context.add_variable_declaration(variable, scope);
                self.variable_names_index.insert(name.to_string(), variable);
                Ok(variable)
            }
            Some(existing_variable) => self
                .block_context
                .may_update_declaration_scope(*existing_variable, name, scope)
                .map(|_| *existing_variable),
        }
    }

    pub(crate) fn create_anonymous_variable(&mut self, scope: ScopeId) -> Result<Variable, RepresentationError> {
        let variable = self.variable_registry.register_anonymous_variable();
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
    ) -> Result<(), RepresentationError> {
        self.record_variable_reference(variable);
        self.variable_registry.set_variable_category(variable, category, VariableCategorySource::Constraint(source))
    }

    pub(crate) fn record_variable_reference(&mut self, variable: Variable) {
        self.block_context.add_referenced_variable(variable.clone());
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
