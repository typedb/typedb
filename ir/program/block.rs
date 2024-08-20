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
    scope_context: ScopeContext, // TODO: We only need this for type annotations
    conjunction: Conjunction,
    modifiers: Vec<Modifier>,
}

impl FunctionalBlock {
    pub fn builder<'a>(context: BlockContext<'a>) -> FunctionalBlockBuilder<'a> {
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
        self.variable_scopes().filter_map(|(v, scope)| if scope != &ScopeId::INPUT { Some(v.clone()) } else { None })
    }

    pub fn input_variables(&self) -> impl Iterator<Item = Variable> + '_ {
        self.variable_scopes().filter_map(|(v, scope)| if scope == &ScopeId::INPUT { Some(v.clone()) } else { None })
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
        let BlockContext { variable_declaration, scope_parents, .. } = block_context;
        let scope_context = ScopeContext { variable_declaration, scope_parents };
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
pub struct VariableRegistry {
    variable_names: HashMap<Variable, String>,
    variable_id_allocator: u16,
    variable_categories: HashMap<Variable, (VariableCategory, Constraint<Variable>)>,
    variable_optionality: HashMap<Variable, VariableOptionality>,
}

impl VariableRegistry {
    pub(crate) fn new() -> VariableRegistry {
        Self {
            variable_names: HashMap::new(),
            variable_id_allocator: 0,
            variable_categories: HashMap::new(),
            variable_optionality: HashMap::new(),
        }
    }

    fn register_variable_named(&mut self, name: String) -> Variable {
        let variable = self.allocate_variable();
        println!("Registered variable {} to {}", name.clone(), variable);
        self.variable_names.insert(variable, name);
        variable
    }

    fn register_anonymous_variable(&mut self) -> Variable {
        self.allocate_variable()
    }

    fn allocate_variable(&mut self) -> Variable {
        let variable = Variable::new(self.variable_id_allocator);
        self.variable_id_allocator += 1;
        variable
    }

    fn set_variable_category(
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

    fn set_variable_is_optional(&mut self, variable: Variable, optional: bool) {
        match optional {
            true => self.variable_optionality.insert(variable, VariableOptionality::Optional),
            false => self.variable_optionality.remove(&variable),
        };
    }

    pub fn variable_categories(&self) -> impl Iterator<Item = (Variable, VariableCategory)> + '_ {
        self.variable_categories.iter().map(|(&variable, &(category, _))| (variable, category))
    }

    pub fn get_variables_named(&self) -> &HashMap<Variable, String> {
        &self.variable_names
    }

    pub fn get_variable_category(&self, variable: Variable) -> Option<VariableCategory> {
        self.variable_categories.get(&variable).map(|(category, _constraint)| *category)
    }

    pub fn get_variable_optionality(&self, variable: Variable) -> Option<VariableOptionality> {
        self.variable_optionality.get(&variable).cloned()
    }

    pub(crate) fn is_variable_optional(&self, variable: Variable) -> bool {
        match self.variable_optionality.get(&variable).unwrap_or(&VariableOptionality::Required) {
            VariableOptionality::Required => false,
            VariableOptionality::Optional => true,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ScopeContext {
    variable_declaration: HashMap<Variable, ScopeId>,
    scope_parents: HashMap<ScopeId, ScopeId>,
}

impl ScopeContext {
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

    scope_id_allocator: u16,
    scope_parents: HashMap<ScopeId, ScopeId>,
}

impl<'a> BlockContext<'a> {
    pub(crate) fn new(
        variable_registry: &'a mut VariableRegistry,
        input_variable_names: &'a mut HashMap<String, Variable>,
    ) -> BlockContext<'a> {
        let mut variable_declaration = HashMap::new();
        input_variable_names.values().for_each(|v| {
            variable_declaration.insert(v.clone(), ScopeId::INPUT);
        });
        let mut scope_parents = HashMap::new();
        scope_parents.insert(ScopeId::ROOT, ScopeId::INPUT);
        Self {
            variable_registry,
            variable_declaration,
            variable_names_index: input_variable_names,
            scope_id_allocator: 2, // `0`, `1` are reserved for INPUT, ROOT respectively.
            scope_parents,
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
                    Err(PatternDefinitionError::DisjointVariableReuse { variable_name: name.to_string() })
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
        &self.variable_names_index
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
        self.variable_registry.set_variable_category(variable, category, source)
    }

    pub(crate) fn set_variable_is_optional(&mut self, variable: Variable, optional: bool) {
        self.variable_registry.set_variable_is_optional(variable, optional)
    }
}

fn is_equal_or_parent_scope(parents: &HashMap<ScopeId, ScopeId>, scope: ScopeId, maybe_parent: ScopeId) -> bool {
    scope == maybe_parent || parents.get(&scope).is_some_and(|&p| is_equal_or_parent_scope(parents, p, maybe_parent))
}

fn is_child_scope(parents: &HashMap<ScopeId, ScopeId>, scope: ScopeId, maybe_child: ScopeId) -> bool {
    parents.get(&maybe_child).is_some_and(|&c| c == scope || is_child_scope(parents, scope, c))
}

impl Display for VariableRegistry {
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
