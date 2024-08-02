/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::HashMap,
    fmt::{Display, Formatter},
};

use answer::variable::Variable;
use itertools::Itertools;

use crate::{
    pattern::{
        conjunction::{Conjunction, ConjunctionBuilder},
        constraint::Constraint,
        variable_category::{VariableCategory, VariableOptionality},
        Scope, ScopeId,
    },
    program::modifier::{Filter, Limit, Modifier, ModifierDefinitionError, Offset, Sort},
    PatternDefinitionError,
};

// A functional block is exactly 1 Conjunction + any number of modifiers

#[derive(Debug, Clone)]
pub struct FunctionalBlock {
    context: BlockContext,
    conjunction: Conjunction,
    modifiers: Vec<Modifier>,
}

impl FunctionalBlock {
    pub fn builder() -> FunctionalBlockBuilder {
        FunctionalBlockBuilder::new()
    }

    pub fn context(&self) -> &BlockContext {
        &self.context
    }

    pub fn conjunction(&self) -> &Conjunction {
        &self.conjunction
    }

    pub fn modifiers(&self) -> &[Modifier] {
        &self.modifiers
    }

    pub fn scope_id(&self) -> ScopeId {
        Scope::scope_id(self)
    }
}

impl Scope for FunctionalBlock {
    fn scope_id(&self) -> ScopeId {
        ScopeId::ROOT
    }
}

pub struct FunctionalBlockBuilder {
    context: BlockContext,
    conjunction: Conjunction,
    modifiers: Vec<Modifier>,
}

impl FunctionalBlockBuilder {
    fn new() -> Self {
        Self { conjunction: Conjunction::new(ScopeId::ROOT), modifiers: Vec::new(), context: BlockContext::new() }
    }

    pub fn finish(self) -> FunctionalBlock {
        let Self { context, conjunction, modifiers, .. } = self;
        FunctionalBlock { context, conjunction, modifiers }
    }

    pub fn conjunction_mut(&mut self) -> ConjunctionBuilder<'_> {
        ConjunctionBuilder::new(&mut self.context, &mut self.conjunction)
    }

    pub fn context_mut(&mut self) -> &mut BlockContext {
        &mut self.context
    }

    pub fn add_limit(&mut self, limit: u64) {
        self.modifiers.push(Modifier::Limit(Limit::new(limit)));
    }

    pub fn add_offset(&mut self, offset: u64) {
        self.modifiers.push(Modifier::Offset(Offset::new(offset)))
    }

    pub fn add_sort(&mut self, sort_variables: Vec<(&str, bool)>) -> Result<&Modifier, ModifierDefinitionError> {
        let sort = Sort::new(sort_variables, &self.context)?;
        self.modifiers.push(Modifier::Sort(sort));
        Ok(self.modifiers.last().unwrap())
    }

    pub fn add_filter(&mut self, variables: Vec<&str>) -> Result<&Modifier, ModifierDefinitionError> {
        let filter = Filter::new(variables, &self.context)?;
        self.modifiers.push(Modifier::Filter(filter));
        Ok(self.modifiers.last().unwrap())
    }
}

#[derive(Debug, Clone)]
pub struct BlockContext {
    variable_names: HashMap<Variable, String>,
    variable_declaration: HashMap<Variable, ScopeId>,
    variable_names_index: HashMap<String, Variable>,
    variable_id_allocator: u16,

    scope_id_allocator: u16,
    scope_parents: HashMap<ScopeId, ScopeId>,

    variable_categories: HashMap<Variable, (VariableCategory, Constraint<Variable>)>,
    variable_optionality: HashMap<Variable, VariableOptionality>,
}

impl BlockContext {
    pub fn new() -> BlockContext {
        Self {
            variable_names: HashMap::new(),
            variable_declaration: HashMap::new(),
            variable_names_index: HashMap::new(),
            variable_id_allocator: 0,
            scope_id_allocator: 1, // `0` is reserved for ROOT
            scope_parents: HashMap::new(),
            variable_categories: HashMap::new(),
            variable_optionality: HashMap::new(),
        }
    }

    pub fn get_variables_named(&self) -> &HashMap<Variable, String> {
        &self.variable_names
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
                let variable = self.allocate_variable();
                self.variable_names.insert(variable, name.to_string());
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
                    Err(PatternDefinitionError::DisjointVariableReuse { variable_name: name.to_string() })
                }
            }
        }
    }

    pub(crate) fn create_anonymous_variable(&mut self, scope: ScopeId) -> Result<Variable, PatternDefinitionError> {
        let variable = self.allocate_variable();
        self.variable_declaration.insert(variable, scope);
        Ok(variable)
    }

    pub fn get_variable(&self, name: &str) -> Option<Variable> {
        self.variable_names_index.get(name).cloned()
    }

    pub fn variables(&self) -> impl Iterator<Item = Variable> + '_ {
        self.variable_declaration.keys().cloned()
    }

    pub fn variable_categories(&self) -> impl Iterator<Item = (Variable, VariableCategory)> + '_ {
        self.variable_categories.iter().map(|(&variable, &(category, _))| (variable, category))
    }

    pub fn get_variable_scopes(&self) -> impl Iterator<Item = (&Variable, &ScopeId)> + '_ {
        self.variable_declaration.iter()
    }

    fn allocate_variable(&mut self) -> Variable {
        let variable = Variable::new(self.variable_id_allocator);
        self.variable_id_allocator += 1;
        variable
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

    pub fn get_variable_category(&self, variable: Variable) -> Option<VariableCategory> {
        self.variable_categories.get(&variable).map(|(category, _optionality)| *category)
    }

    pub(crate) fn set_variable_category(
        &mut self,
        variable: Variable,
        category: VariableCategory,
        source: Constraint<Variable>,
    ) -> Result<(), PatternDefinitionError> {
        let existing_category = self.variable_categories.get_mut(&variable);
        match existing_category {
            None => {
                self.variable_categories.insert(variable, (category, source));
                Ok(())
            }
            Some((existing_category, existing_source)) => {
                let narrowest = existing_category.narrowest(category);
                match narrowest {
                    None => Err(PatternDefinitionError::VariableCategoryMismatch {
                        variable,
                        variable_name: self.variable_names.get(&variable).cloned(),
                        category_1: category,
                        category_1_source: source,
                        category_2: *existing_category,
                        category_2_source: existing_source.clone(),
                    }),
                    Some(narrowed) => {
                        if narrowed == *existing_category {
                            Ok(())
                        } else {
                            *existing_category = narrowed;
                            *existing_source = source;
                            Ok(())
                        }
                    }
                }
            }
        }
    }

    pub(crate) fn set_variable_is_optional(&mut self, variable: Variable, optional: bool) {
        match optional {
            true => self.variable_optionality.insert(variable, VariableOptionality::Optional),
            false => self.variable_optionality.remove(&variable),
        };
    }

    pub(crate) fn is_variable_optional(&self, variable: Variable) -> bool {
        match self.variable_optionality.get(&variable).unwrap_or(&VariableOptionality::Required) {
            VariableOptionality::Required => false,
            VariableOptionality::Optional => true,
        }
    }
}

fn is_equal_or_parent_scope(parents: &HashMap<ScopeId, ScopeId>, scope: ScopeId, maybe_parent: ScopeId) -> bool {
    scope == maybe_parent || parents.get(&scope).is_some_and(|&p| is_equal_or_parent_scope(parents, p, maybe_parent))
}

fn is_child_scope(parents: &HashMap<ScopeId, ScopeId>, scope: ScopeId, maybe_child: ScopeId) -> bool {
    parents.get(&maybe_child).is_some_and(|&c| c == scope || is_child_scope(parents, scope, c))
}

impl Default for BlockContext {
    fn default() -> Self {
        Self::new()
    }
}

impl Display for BlockContext {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Named variables:")?;
        for var in self.variable_names.keys().sorted_unstable() {
            writeln!(f, "  {}: ${}", var, self.variable_names[var])?;
        }
        writeln!(f, "Variable categories:")?;
        for var in self.variable_categories.keys().sorted_unstable() {
            writeln!(f, "  {}: {}", var, self.variable_categories[var].0)?;
        }
        writeln!(f, "Optional variables:")?;
        for var in self.variable_optionality.keys().sorted_unstable() {
            writeln!(f, "  {}", var)?;
        }
        Ok(())
    }
}
