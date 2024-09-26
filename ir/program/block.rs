/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{HashMap, HashSet},
    ops::Index,
};

use answer::variable::Variable;
use encoding::value::value::Value;

use crate::{
    pattern::{
        conjunction::{Conjunction, ConjunctionBuilder},
        constraint::Constraint,
        variable_category::VariableCategory,
        ParameterID, Scope, ScopeId,
    },
    program::{
        modifier::{Limit, Modifier, Offset, Select, Sort},
        VariableCategorySource, VariableRegistry,
    },
    PatternDefinitionError,
};

// A functional block is exactly 1 Conjunction + any number of modifiers

#[derive(Debug, Clone)]
pub struct FunctionalBlock {
    scope_context: ScopeContext, // TODO: We only need this for type annotations
    conjunction: Conjunction,
    modifiers: Vec<Modifier>,
}

impl FunctionalBlock {
    pub fn builder(context: BlockContext<'_>) -> FunctionalBlockBuilder<'_> {
        FunctionalBlockBuilder::new(context)
    }

    pub fn conjunction(&self) -> &Conjunction {
        &self.conjunction
    }

    pub fn modifiers(&self) -> &[Modifier] {
        &self.modifiers
    }

    pub fn scope_context(&self) -> &ScopeContext {
        &self.scope_context
    }

    pub fn scope_id(&self) -> ScopeId {
        Scope::scope_id(self)
    }

    pub fn variable_scopes(&self) -> impl Iterator<Item = (&Variable, &ScopeId)> + '_ {
        self.scope_context.variable_declaration.iter()
    }

    pub fn block_variables(&self) -> impl Iterator<Item = Variable> + '_ {
        self.variable_scopes().filter_map(|(&v, scope)| if scope != &ScopeId::INPUT { Some(v) } else { None })
    }

    pub fn input_variables(&self) -> impl Iterator<Item = Variable> + '_ {
        self.variable_scopes().filter_map(|(&v, scope)| if scope == &ScopeId::INPUT { Some(v) } else { None })
    }
}

impl Scope for FunctionalBlock {
    fn scope_id(&self) -> ScopeId {
        ScopeId::ROOT
    }
}

pub struct FunctionalBlockBuilder<'reg> {
    context: BlockContext<'reg>,
    conjunction: Conjunction,
    modifiers: Vec<Modifier>,
}

impl<'reg> FunctionalBlockBuilder<'reg> {
    fn new(context: BlockContext<'reg>) -> Self {
        Self { conjunction: Conjunction::new(ScopeId::ROOT), modifiers: Vec::new(), context }
    }

    pub fn finish(self) -> FunctionalBlock {
        let Self { conjunction, modifiers, context: block_context } = self;
        let BlockContext { variable_declaration, scope_parents, referenced_variables, .. } = block_context;
        let scope_context = ScopeContext { variable_declaration, scope_parents, referenced_variables };
        FunctionalBlock { conjunction, modifiers, scope_context }
    }

    pub fn conjunction_mut(&mut self) -> ConjunctionBuilder<'_, 'reg> {
        ConjunctionBuilder::new(&mut self.context, &mut self.conjunction)
    }

    pub fn context_mut(&mut self) -> &mut BlockContext<'reg> {
        &mut self.context
    }

    pub fn add_limit(&mut self, limit: u64) {
        self.modifiers.push(Modifier::Limit(Limit::new(limit)));
    }

    pub fn add_offset(&mut self, offset: u64) {
        self.modifiers.push(Modifier::Offset(Offset::new(offset)))
    }

    pub fn add_sort(&mut self, sort_variables: Vec<(Variable, bool)>) -> &Modifier {
        let sort = Sort::new(sort_variables);
        self.modifiers.push(Modifier::Sort(sort));
        self.modifiers.last().unwrap()
    }

    pub fn add_select(&mut self, variables: HashSet<Variable>) -> &Modifier {
        let select = Select::new(variables);
        self.modifiers.push(Modifier::Select(select));
        self.modifiers.last().unwrap()
    }
}

#[derive(Clone, Debug, Default)]
pub struct ParameterRegistry {
    registry: HashMap<ParameterID, Value<'static>>,
}

impl ParameterRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub(crate) fn register(&mut self, value: Value<'static>) -> ParameterID {
        let id = ParameterID { id: self.registry.len() };
        let _prev = self.registry.insert(id, value);
        debug_assert_eq!(_prev, None);
        id
    }

    pub fn get(&self, id: ParameterID) -> Option<&Value<'static>> {
        self.registry.get(&id)
    }
}

impl Index<ParameterID> for ParameterRegistry {
    type Output = Value<'static>;

    fn index(&self, id: ParameterID) -> &Self::Output {
        self.registry.index(&id)
    }
}

#[derive(Debug, Clone)]
pub struct ScopeContext {
    variable_declaration: HashMap<Variable, ScopeId>,
    scope_parents: HashMap<ScopeId, ScopeId>,
    referenced_variables: HashSet<Variable>,
}

impl ScopeContext {
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
    pub fn get_variable_scopes(&self) -> impl Iterator<Item = (&Variable, &ScopeId)> + '_ {
        self.variable_declaration.iter()
    }
}

#[derive(Debug)]
pub struct BlockContext<'a> {
    variable_registry: &'a mut VariableRegistry,
    variable_declaration: HashMap<Variable, ScopeId>,
    variable_names_index: &'a mut HashMap<String, Variable>,

    parameters: &'a mut ParameterRegistry,

    scope_id_allocator: u16,
    scope_parents: HashMap<ScopeId, ScopeId>,
    referenced_variables: HashSet<Variable>, // Involved in a constraint in this block
}

impl<'a> BlockContext<'a> {
    pub(crate) fn new(
        variable_registry: &'a mut VariableRegistry,
        input_variable_names: &'a mut HashMap<String, Variable>,
        parameters: &'a mut ParameterRegistry,
    ) -> BlockContext<'a> {
        let mut variable_declaration = HashMap::new();
        input_variable_names.values().for_each(|v| {
            variable_declaration.insert(*v, ScopeId::INPUT);
        });
        let mut scope_parents = HashMap::new();
        scope_parents.insert(ScopeId::ROOT, ScopeId::INPUT);
        Self {
            variable_registry,
            variable_declaration,
            variable_names_index: input_variable_names,
            parameters,
            scope_id_allocator: 2, // `0`, `1` are reserved for INPUT, ROOT respectively.
            scope_parents,
            referenced_variables: HashSet::new(),
        }
    }

    pub fn variables_declared(&self) -> impl Iterator<Item = Variable> + '_ {
        self.variable_declaration.keys().cloned()
    }

    pub fn get_variable_named(&self, name: &str, scope: ScopeId) -> Option<&Variable> {
        self.variable_names_index.get(name)
    }

    pub(crate) fn get_or_declare_variable(
        &mut self,
        name: &str,
        scope: ScopeId,
    ) -> Result<Variable, PatternDefinitionError> {
        match self.variable_names_index.get(name) {
            None => {
                let variable = self.variable_registry.register_variable_named(name.to_string());
                self.variable_declaration.insert(variable, scope);
                self.variable_names_index.insert(name.to_string(), variable);
                Ok(variable)
            }
            Some(existing_variable) => {
                let existing_scope = self.variable_declaration.get_mut(existing_variable).unwrap();
                if is_equal_or_parent_scope(&self.scope_parents, scope, *existing_scope) {
                    // Parent defines same name: ok, reuse the variable
                    Ok(*existing_variable)
                } else if is_child_scope(&self.scope_parents, scope, *existing_scope) {
                    // Child defines the same name: ok, reuse the variable, and change the declaration scope to the current one
                    *existing_scope = scope;
                    Ok(*existing_variable)
                } else {
                    Err(PatternDefinitionError::DisjointVariableReuse { name: name.to_string() })
                }
            }
        }
    }

    pub(crate) fn create_anonymous_variable(&mut self, scope: ScopeId) -> Result<Variable, PatternDefinitionError> {
        let variable = self.variable_registry.register_anonymous_variable();
        self.variable_declaration.insert(variable, scope);
        Ok(variable)
    }

    pub fn get_variable(&self, name: &str) -> Option<Variable> {
        self.variable_names_index.get(name).cloned()
    }

    pub fn get_variable_scopes(&self) -> impl Iterator<Item = (&Variable, &ScopeId)> + '_ {
        self.variable_declaration.iter()
    }

    pub fn named_variable_mapping(&self) -> &HashMap<String, Variable> {
        self.variable_names_index
    }

    pub fn is_variable_available(&self, scope: ScopeId, variable: Variable) -> bool {
        let variable_scope = self.variable_declaration.get(&variable);
        match variable_scope {
            None => false,
            Some(variable_scope) => is_equal_or_parent_scope(&self.scope_parents, scope, *variable_scope),
        }
    }

    pub(crate) fn create_child_scope(&mut self, parent: ScopeId) -> ScopeId {
        let scope = ScopeId::new(self.scope_id_allocator);
        debug_assert_ne!(scope, ScopeId::ROOT);
        self.scope_id_allocator += 1;
        self.scope_parents.insert(scope, parent);
        scope
    }

    pub(crate) fn set_variable_category(
        &mut self,
        variable: Variable,
        category: VariableCategory,
        source: Constraint<Variable>,
    ) -> Result<(), PatternDefinitionError> {
        self.record_variable_reference(variable);
        self.variable_registry.set_variable_category(variable, category, source)
    }

    pub(crate) fn record_variable_reference(&mut self, variable: Variable) {
        self.referenced_variables.insert(variable);
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
