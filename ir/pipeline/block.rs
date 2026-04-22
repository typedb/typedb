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
    RepresentationError,
    pattern::{
        BindingMode, BranchID, ContextualisedBindingMode, ScopeId,
        conjunction::{Conjunction, ConjunctionBuilder, ConjunctionBuilderWithContext, NestedPatternBuilder},
        constraint::Constraint,
        variable_category::VariableCategory,
    },
    pipeline::{ParameterRegistry, VariableCategorySource, VariableRegistry},
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
    conjunction: ConjunctionBuilder,
}

impl<'reg> BlockBuilder<'reg> {
    fn new(context: BlockBuilderContext<'reg>) -> Self {
        Self { conjunction: ConjunctionBuilder::new(ScopeId::ROOT), context }
    }

    pub fn finish(mut self) -> Result<Block, Box<RepresentationError>> {
        let block_binding_modes = self.variable_binding_modes();
        validate_no_optionals_in_negations(&self.conjunction, false)?;
        validate_all_required_variables_can_be_bound(&self, &block_binding_modes, &self.context.variable_registry)?;
        validate_no_unbound_variable_categories(&self.conjunction, &self.context)?;
        validate_is_variables_have_same_category(&self.conjunction, &self.context.variable_registry)?;

        // Update
        block_binding_modes
            .iter()
            .filter(|(_, mode)| mode.is_optionally_binding())
            .for_each(|(v, _)| self.context.set_variable_optionality(*v, true));
        self.context
            .variable_names_index
            .retain(|_, var| block_binding_modes.get(var).copied() != Some(BindingMode::LocallyBindingInChild));
        let conjunction = self.conjunction.finish(&ContextualisedBindingMode::for_block(block_binding_modes));
        let block_context = self.context.block_context;
        Ok(Block { conjunction, block_context })
    }

    pub fn conjunction_mut<'ctx>(&'ctx mut self) -> ConjunctionBuilderWithContext<'ctx, 'reg> {
        ConjunctionBuilderWithContext::new(&mut self.context, &mut self.conjunction)
    }

    pub fn context_mut(&mut self) -> &mut BlockBuilderContext<'reg> {
        &mut self.context
    }

    fn variable_binding_modes(&self) -> HashMap<Variable, BindingMode> {
        let mut block_binding_modes = self.conjunction.variable_binding_modes();
        block_binding_modes.extend(self.context.input_variables().map(|v| (v, BindingMode::AlwaysBinding)));
        block_binding_modes
    }
}

fn validate_no_unbound_variable_categories(
    conjunction: &ConjunctionBuilder,
    context: &BlockBuilderContext<'_>,
) -> Result<(), Box<RepresentationError>> {
    let unbound = context.block_context.registered_variables().find(|&variable| {
        matches!(
            context.variable_registry.get_variable_category(variable),
            Some(VariableCategory::AttributeOrValue) | None
        )
    });
    if let Some(variable) = unbound {
        Err(Box::new(RepresentationError::UnboundVariable {
            variable: context.variable_registry.get_variable_name_or_unnamed(variable).to_owned(),
            source_span: context.variable_registry.source_span(variable),
        }))
    } else {
        Ok(())
    }
}

fn validate_no_optionals_in_negations(
    conjunction: &ConjunctionBuilder,
    this_conjunction_in_negation: bool,
) -> Result<(), Box<RepresentationError>> {
    if this_conjunction_in_negation {
        if let Some(optional) = conjunction
            .nested_patterns()
            .iter()
            .filter_map(|nested| match nested {
                NestedPatternBuilder::Optional(optional) => Some(optional),
                _ => None,
            })
            .next()
        {
            return Err(Box::new(RepresentationError::OptionalInNegation {}));
        }
    }
    conjunction.nested_patterns().iter().try_for_each(|nested| match nested {
        NestedPatternBuilder::Disjunction(disjunction) => disjunction
            .conjunctions()
            .try_for_each(|c| validate_no_optionals_in_negations(c, this_conjunction_in_negation)),
        NestedPatternBuilder::Negation(negation) => validate_no_optionals_in_negations(negation.conjunction(), true),
        NestedPatternBuilder::Optional(optional) => {
            validate_no_optionals_in_negations(optional.conjunction(), this_conjunction_in_negation)
        }
    })
}

fn validate_is_variables_have_same_category(
    conjunction: &ConjunctionBuilder,
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
        NestedPatternBuilder::Disjunction(disjunction) => disjunction
            .conjunctions()
            .try_for_each(|inner| validate_is_variables_have_same_category(inner, variable_registry)),
        NestedPatternBuilder::Negation(negation) => {
            validate_is_variables_have_same_category(negation.conjunction(), variable_registry)
        }
        NestedPatternBuilder::Optional(optional) => {
            validate_is_variables_have_same_category(optional.conjunction(), variable_registry)
        }
    })?;

    Ok(())
}

fn validate_all_required_variables_can_be_bound(
    block: &BlockBuilder<'_>,
    block_binding_modes: &HashMap<Variable, BindingMode>,
    variable_registry: &VariableRegistry,
) -> Result<(), Box<RepresentationError>> {
    for (var, mode) in block_binding_modes.iter() {
        if mode.is_require_prebound() {
            let mut all_spans = Vec::new();
            find_constraints_referencing_variable(&block.conjunction, *var, &mut all_spans);
            let variable = variable_registry.get_variable_name_or_unnamed(*var).to_owned();
            let source_span = variable_registry.source_span(*var);
            return Err(Box::new(RepresentationError::UnboundRequiredVariable {
                variable,
                source_span,
                _all_spans: all_spans,
            }));
        }
    }
    Ok(())
}

fn find_constraints_referencing_variable(conjunction: &ConjunctionBuilder, variable: Variable, spans: &mut Vec<Span>) {
    spans.extend(
        conjunction.constraints().iter().filter(|c| c.ids().contains(&variable)).filter_map(|c| c.source_span()),
    );
    conjunction.nested_patterns().iter().for_each(|nested| match nested {
        NestedPatternBuilder::Disjunction(disjunction) => {
            disjunction.conjunctions().for_each(|c| find_constraints_referencing_variable(c, variable, spans));
        }
        NestedPatternBuilder::Negation(negation) => {
            find_constraints_referencing_variable(negation.conjunction(), variable, spans)
        }
        NestedPatternBuilder::Optional(optional) => {
            find_constraints_referencing_variable(optional.conjunction(), variable, spans)
        }
    })
}

#[derive(Debug, Clone, Default, Eq, PartialEq)]
pub struct BlockContext {
    input_variables: HashSet<Variable>,
    variable_declaration: HashSet<Variable>,
}

impl BlockContext {
    fn new() -> Self {
        Default::default()
    }

    fn add_input_declaration(&mut self, var: Variable) {
        self.add_variable_declaration(var);
        self.input_variables.insert(var);
    }

    fn add_variable_declaration(&mut self, var: Variable) {
        self.variable_declaration.insert(var);
    }

    fn is_block_input_variable(&self, var: &Variable) -> bool {
        self.input_variables.contains(var)
    }

    pub fn registered_variables(&self) -> impl Iterator<Item = Variable> + '_ {
        self.variable_declaration.iter().copied()
    }
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

    pub(crate) fn get_or_declare_variable(
        &mut self,
        name: &str,
        source_span: Option<Span>,
    ) -> Result<Variable, Box<RepresentationError>> {
        match self.variable_names_index.get(name) {
            None => {
                let variable = self.variable_registry.register_variable_named(name.to_string(), source_span)?;
                self.block_context.add_variable_declaration(variable);
                self.variable_names_index.insert(name.to_string(), variable);
                Ok(variable)
            }
            Some(&existing_variable) => Ok(existing_variable),
        }
    }

    pub(crate) fn create_anonymous_variable(
        &mut self,
        source_span: Option<Span>,
    ) -> Result<Variable, Box<RepresentationError>> {
        let variable = self.variable_registry.register_anonymous_variable(source_span)?;
        self.block_context.add_variable_declaration(variable);
        Ok(variable)
    }

    pub(crate) fn is_block_input_variable(&self, variable: Variable) -> bool {
        self.block_context.is_block_input_variable(&variable)
    }

    pub(crate) fn input_variables(&self) -> impl Iterator<Item = Variable> + '_ {
        self.block_context.registered_variables().filter(|var| self.is_block_input_variable(*var))
    }

    pub(crate) fn next_scope_id(&mut self) -> ScopeId {
        let scope = ScopeId::new(self.scope_id_allocator);
        debug_assert_ne!(scope, ScopeId::ROOT);
        self.scope_id_allocator += 1;
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
