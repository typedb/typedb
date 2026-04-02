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
        nested_pattern::NestedPattern,
        variable_category::VariableCategory,
        BranchID, Pattern, Scope, ScopeId,
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
        self.block_context.referenced_variables()
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

    pub fn finish(mut self) -> Result<Block, Box<RepresentationError>> {
        self.conjunction_mut().compute_and_set_variable_binding_modes();
        self.conjunction.variable_binding_modes().iter().for_each(|(v, mode)| {
            if mode.is_optionally_binding() {
                self.context.set_variable_optionality(*v, true);
            }
        });
        let Self {
            conjunction,
            context:
                BlockBuilderContext { block_context, variable_registry, variable_names_index: visible_variables, .. },
        } = self;
        validate_conjunction(&conjunction, variable_registry, &block_context)?;
        let conjunction_visible: HashSet<_> = conjunction.named_visible_binding_variables().collect();
        visible_variables
            .retain(|name, var| conjunction_visible.contains(var) || block_context.is_block_input_variable(var));
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
    block_context: &BlockContext,
) -> Result<(), Box<RepresentationError>> {
    let unbound = conjunction.referenced_variables().find(|&variable| {
        matches!(variable_registry.get_variable_category(variable), Some(VariableCategory::AttributeOrValue) | None)
    });
    // TODO: unbound variable somewhere in the pattern is insufficient - a variable could be bound, but
    //   it's actually only required in a sibling part of the pattern ??
    if let Some(variable) = unbound {
        return Err(Box::new(RepresentationError::UnboundVariable {
            variable: variable_registry.get_variable_name_or_unnamed(variable).to_owned(),
            source_span: variable_registry.source_span(variable),
        }));
    }

    for (var, mode) in conjunction.variable_binding_modes() {
        if mode.is_require_prebound() && !block_context.is_block_input_variable(&var) {
            let variable = variable_registry.get_variable_name_or_unnamed(var).to_owned();
            // let spans = mode.referencing_constraints().iter().map(|s| s.source_span()).collect_vec();
            return Err(Box::new(RepresentationError::UnboundRequiredVariable {
                variable,
                // source_span: spans[0],
                // _rest: spans,
            }));
        }
    }

    validate_is_variables_have_same_category(conjunction, variable_registry)?;
    Ok(())
}

fn validate_is_variables_have_same_category(
    conjunction: &Conjunction,
    variable_registry: &VariableRegistry,
) -> Result<(), Box<RepresentationError>> {
    let is_with_mismatched_category = conjunction.constraints().iter().filter_map(|c| c.as_is()).find(|is| {
        let lhs_category = variable_registry.get_variable_category(is.lhs().as_variable().unwrap()).unwrap();
        let rhs_category = variable_registry.get_variable_category(is.rhs().as_variable().unwrap()).unwrap();
        lhs_category.narrowest(rhs_category).is_none()
    });
    if let Some(is) = is_with_mismatched_category {
        let lhs = is.lhs().as_variable().unwrap();
        let rhs = is.rhs().as_variable().unwrap();
        let lhs_category = variable_registry.get_variable_category(lhs).unwrap();
        let rhs_category = variable_registry.get_variable_category(rhs).unwrap();
        let lhs_variable = variable_registry.get_variable_name_or_unnamed(lhs).to_owned();
        let rhs_variable = variable_registry.get_variable_name_or_unnamed(rhs).to_owned();
        return Err(Box::new(RepresentationError::VariableCategoryMismatchInIs {
            lhs_variable,
            rhs_variable,
            lhs_category,
            rhs_category,
            source_span: is.source_span(),
        }));
    }

    conjunction.nested_patterns().iter().try_for_each(|nested| match nested {
        NestedPattern::Disjunction(disjunction) => disjunction
            .conjunctions()
            .iter()
            .try_for_each(|inner| validate_is_variables_have_same_category(inner, variable_registry)),
        NestedPattern::Negation(negation) => {
            validate_is_variables_have_same_category(negation.conjunction(), variable_registry)
        }
        NestedPattern::Optional(optional) => {
            validate_is_variables_have_same_category(optional.conjunction(), variable_registry)
        }
    })?;

    Ok(())
}

#[derive(Debug, Clone, Default, Eq, PartialEq)]
pub struct BlockContext {
    variable_declaration: HashMap<Variable, ScopeId>,
    scope_parents: HashMap<ScopeId, ScopeId>,
    scope_types: HashMap<ScopeId, ScopeType>,
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

    fn declare_scope(&mut self, scope_id: ScopeId, scope_type: ScopeType) {
        debug_assert!(!self.scope_types.contains_key(&scope_id));
        self.scope_types.insert(scope_id, scope_type);
    }

    fn set_scope_parent(&mut self, scope_id: ScopeId, parent_scope_id: ScopeId) {
        debug_assert!(self.scope_types.contains_key(&scope_id));
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
        let recorded_scope = self.variable_declaration[&var];
        if is_equal_or_parent_scope(&self.scope_parents, scope, recorded_scope) {
            // Parent defines same name: ok, reuse the variable
        } else if is_child_scope(&self.scope_parents, scope, recorded_scope) {
            // Child defines the same name: ok, reuse the variable, and change the declaration scope to the current one
            *self.variable_declaration.get_mut(&var).unwrap() = scope;
        } else {
            // Sibling scope declares the name
            // This could be "{ A(x) } or { B(x) }" or "{ A(x) } or { B(y) }; { C(x) } or { D(y) };"
            // or even " not { A(x) }; not { B(x) }; "
            // These same-named variables will for now be all treated as identical so the constraints using assigned VarId don't have to get rewritten later
            // Future: we can allow scope-local variables with re-use across branches by always assigning new variables and equating them elsewhere
            // In these cases the 'x' and 'y' declarations will be pulled up the root conjunction
            // We can only later validate the requirements about whether each pattern conforms to the required variable usages & visibility

            let ancestor = lowest_common_ancestor(&self.scope_parents, recorded_scope, scope);
            let ancestor_conjunction =
                lowest_parent_conjunction_or_negation(&self.scope_parents, &self.scope_types, ancestor);
            *self.variable_declaration.get_mut(&var).unwrap() = ancestor_conjunction;
        }
        Ok(())
    }

    pub fn is_block_input_variable(&self, var: &Variable) -> bool {
        self.variable_declaration.get(var).copied() != Some(ScopeId::INPUT)
    }

    fn referenced_variables(&self) -> impl Iterator<Item = Variable> + '_ {
        self.variable_declaration.keys().copied()
    }

    #[cfg(debug_assertions)]
    fn is_variable_available_in(&self, scope: ScopeId, variable: Variable) -> bool {
        let variable_scope = self.variable_declaration.get(&variable);
        let in_scope = variable_scope
            .is_some_and(|variable_scope| is_equal_or_parent_scope(&self.scope_parents, scope, *variable_scope));
        in_scope
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ScopeType {
    Input,
    Conjunction,
    Disjunction,
    Negation,
    Optional,
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
        block_context.declare_scope(ScopeId::ROOT, ScopeType::Conjunction);
        block_context.declare_scope(ScopeId::INPUT, ScopeType::Input);
        block_context.set_scope_parent(ScopeId::ROOT, ScopeId::INPUT);
        Self {
            variable_registry,
            variable_names_index: available_input_names,
            parameters,
            scope_id_allocator: 2, // `0`, `1` are reserved for INPUT, ROOT respectively.
            block_context,
        }
    }

    pub(crate) fn next_branch_id(&mut self) -> BranchID {
        self.variable_registry.next_branch_id()
    }

    pub(crate) fn get_variable_name(&self, variable: Variable) -> Option<&String> {
        self.variable_registry.get_variable_name(variable)
    }

    pub(crate) fn get_parent_scope(&self, scope: ScopeId) -> Option<ScopeId> {
        self.block_context.scope_parents.get(&scope).copied()
    }

    pub(crate) fn get_scope_type(&self, scope: ScopeId) -> ScopeType {
        debug_assert!(self.block_context.scope_types.contains_key(&scope));
        *self.block_context.scope_types.get(&scope).unwrap()
    }

    pub(crate) fn get_or_declare_variable(
        &mut self,
        name: &str,
        scope: ScopeId,
        source_span: Option<Span>,
    ) -> Result<Variable, Box<RepresentationError>> {
        match self.variable_names_index.get(name) {
            None => {
                let variable = self.variable_registry.register_variable_named(name.to_string(), source_span)?;
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
        let variable = self.variable_registry.register_anonymous_variable(source_span)?;
        self.block_context.add_variable_declaration(variable, scope);
        Ok(variable)
    }

    #[cfg(debug_assertions)]
    pub(crate) fn is_variable_available_in(&self, scope: ScopeId, variable: Variable) -> bool {
        self.block_context.is_variable_available_in(scope, variable)
    }

    pub(crate) fn is_variable_input(&self, variable: Variable) -> bool {
        self.block_context.variable_declaration.get(&variable) == Some(&ScopeId::INPUT)
    }

    pub(crate) fn input_variables(&self) -> impl Iterator<Item = Variable> + '_ {
        self.block_context.variable_declaration.keys().copied().filter(|var| self.is_variable_input(*var))
    }

    pub(crate) fn create_child_scope(&mut self, parent: ScopeId, scope_type: ScopeType) -> ScopeId {
        let scope = ScopeId::new(self.scope_id_allocator);
        debug_assert_ne!(scope, ScopeId::ROOT);
        self.scope_id_allocator += 1;
        self.block_context.declare_scope(scope, scope_type);
        self.block_context.set_scope_parent(scope, parent);
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

    pub(crate) fn set_variable_optionality(&mut self, variable: Variable, is_optional: bool) {
        self.variable_registry.set_variable_is_optional(variable, is_optional);
    }

    pub(crate) fn parameters(&mut self) -> &mut ParameterRegistry {
        self.parameters
    }
}

fn is_equal_or_parent_scope(parents: &HashMap<ScopeId, ScopeId>, scope: ScopeId, maybe_parent: ScopeId) -> bool {
    scope == maybe_parent || parents.get(&scope).is_some_and(|&p| is_equal_or_parent_scope(parents, p, maybe_parent))
}

fn is_child_scope(parents: &HashMap<ScopeId, ScopeId>, scope: ScopeId, maybe_child: ScopeId) -> bool {
    parents.get(&maybe_child).is_some_and(|&c| c == scope || is_child_scope(parents, scope, c))
}

fn lowest_common_ancestor(parents: &HashMap<ScopeId, ScopeId>, left: ScopeId, right: ScopeId) -> ScopeId {
    if left == right || is_child_scope(parents, left, right) {
        left
    } else if is_child_scope(parents, right, left) {
        right
    } else {
        lowest_common_ancestor(parents, parents[&left], parents[&right])
    }
}

fn lowest_parent_conjunction_or_negation(
    parents: &HashMap<ScopeId, ScopeId>,
    scope_types: &HashMap<ScopeId, ScopeType>,
    scope_id: ScopeId,
) -> ScopeId {
    debug_assert!(scope_types.contains_key(&scope_id));
    let scope_type = *scope_types.get(&scope_id).unwrap();
    if scope_type == ScopeType::Conjunction || scope_type == ScopeType::Negation {
        scope_id
    } else {
        lowest_parent_conjunction_or_negation(parents, scope_types, *parents.get(&scope_id).unwrap())
    }
}
