/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    fmt,
};

use answer::variable::Variable;
use itertools::Itertools;
use structural_equality::StructuralEquality;
use typeql::common::Span;

use crate::{
    RepresentationError,
    pattern::{
        BindingMode, BranchID, Pattern, PatternVariables, ScopeId,
        conjunction::{Conjunction, ConjunctionBuilder, ConjunctionBuilderWithContext, NestedPatternBuilder},
        constraint::Constraint,
        nested_pattern::NestedPattern,
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
        validate_optional_returns(&self.context, &self.conjunction)?;
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
        let conjunction =
            self.conjunction.finish(&PatternVariables::for_block(block_binding_modes, self.context.input_variables()));

        let input_variables = self.context.input_variables().collect();
        validate_is_plannable(&conjunction, &input_variables, &self.context.variable_registry)?;
        validate_expressions_assignments_are_unique(&conjunction, &input_variables, &self.context.variable_registry)?;
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

fn validate_optional_returns(
    context: &BlockBuilderContext<'_>,
    conjunction: &ConjunctionBuilder,
) -> Result<(), Box<RepresentationError>> {
    let mut optional_assignments = HashMap::new();
    validate_optional_returns_recursive(context, conjunction, &mut optional_assignments)
}

fn validate_optional_returns_recursive(
    context: &BlockBuilderContext<'_>,
    conjunction: &ConjunctionBuilder,
    acc: &mut HashMap<Variable, Option<Span>>,
) -> Result<(), Box<RepresentationError>> {
    let conjunction_binding_modes = conjunction.variable_binding_modes();
    conjunction.nested_patterns().iter().try_for_each(|nested| match nested {
        NestedPatternBuilder::Disjunction(disjunction) => {
            disjunction.conjunctions().try_for_each(|branch| validate_optional_returns_recursive(context, branch, acc))
        }
        NestedPatternBuilder::Negation(negation) => {
            validate_optional_returns_recursive(context, negation.conjunction(), acc)
        }
        NestedPatternBuilder::Optional(optional) => {
            validate_optional_returns_recursive(context, optional.conjunction(), acc)
        }
    })?;
    conjunction.constraints().iter().filter_map(|c| c.as_function_call_binding()).for_each(|call| {
        for (var, mode) in call.binding_modes() {
            if mode == BindingMode::OptionallyBinding {
                acc.insert(var, call.source_span());
            }
        }
    });
    // Check at each level
    let reused_optional_return_opt = acc.iter().find(|(var, _)| match conjunction_binding_modes.get(var) {
        None => false,
        Some(mode) => *mode != BindingMode::OptionallyBinding,
    });
    if let Some((var, &source_span)) = reused_optional_return_opt {
        let variable = context.get_variable_name_or_unnamed(*var).to_owned();
        // TODO: This has to wait till we finalize the spec
        // // Err(Box::new(RepresentationError::OptionalFunctionReturnReferenced { variable, source_span }))
        // use error::TypeDBError;
        // tracing::warn!(
        //     "Function call reuses optionally assigned variable. This will fail in the next version:\n{}",
        //     RepresentationError::OptionalFunctionReturnReferenced { variable, source_span }.format_description()
        // );
        Ok(())
    } else {
        Ok(())
    }
}

fn validate_expressions_assignments_are_unique(
    conjunction: &Conjunction,
    input_variables: &BTreeSet<Variable>,
    variable_registry: &VariableRegistry,
) -> Result<(), Box<RepresentationError>> {
    let mut assigned_in_block = BTreeMap::new();
    validate_expressions_assignments_are_unique_impl(conjunction, &mut assigned_in_block, variable_registry)?;

    if let Some((&variable, &source_span)) = assigned_in_block.iter().find(|(var, _)| input_variables.contains(var)) {
        let variable = variable_registry.get_variable_name_or_unnamed(variable).to_owned();
        Err(Box::new(RepresentationError::AssigningToInputVariable { variable, source_span }))
    } else {
        Ok(())
    }
}
fn validate_expressions_assignments_are_unique_impl(
    conjunction: &Conjunction,
    assigned: &mut BTreeMap<Variable, Option<Span>>,
    variable_registry: &VariableRegistry,
) -> Result<(), Box<RepresentationError>> {
    // TODO: Can we absorb this change into BindingModes if we introduce an "Assigned" variant?
    fn add_or_error(
        variable_registry: &VariableRegistry,
        assigned: &mut BTreeMap<Variable, Option<Span>>,
        (id, source_span): (Variable, Option<Span>),
    ) -> Result<(), Box<RepresentationError>> {
        if let Some(other_span) = assigned.insert(id, source_span) {
            let variable = variable_registry.get_variable_name_or_unnamed(id).to_owned();
            Err(Box::new(RepresentationError::MultipleAssignmentsForVariable { variable, source_span, other_span }))
        } else {
            Ok(())
        }
    }

    conjunction
        .constraints()
        .iter()
        .filter_map(|constraint| constraint.as_expression_binding())
        .flat_map(|expr| expr.ids_assigned().map(|id| (id, expr.source_span())))
        .try_for_each(|id_span| add_or_error(variable_registry, assigned, id_span))?;

    conjunction
        .constraints()
        .iter()
        .filter_map(|constraint| constraint.as_function_call_binding())
        .flat_map(|func_call| func_call.ids_assigned().map(|id| (id, func_call.source_span())))
        .try_for_each(|id_span| add_or_error(variable_registry, assigned, id_span))?;

    for nested in conjunction.nested_patterns() {
        match nested {
            NestedPattern::Optional(optional) => {
                validate_expressions_assignments_are_unique_impl(optional.conjunction(), assigned, variable_registry)?;
            }
            NestedPattern::Negation(negation) => {
                validate_expressions_assignments_are_unique_impl(negation.conjunction(), assigned, variable_registry)?;
            }
            NestedPattern::Disjunction(disjunction) => {
                let mut disjunction_assigned = BTreeMap::new();
                disjunction.conjunctions().iter().try_for_each(|branch| {
                    let mut branch_assigned = BTreeMap::new();
                    validate_expressions_assignments_are_unique_impl(branch, &mut branch_assigned, variable_registry)?;
                    disjunction_assigned.extend(branch_assigned);
                    Ok::<_, Box<RepresentationError>>(())
                })?;
                disjunction_assigned
                    .into_iter()
                    .try_for_each(|id_span| add_or_error(variable_registry, assigned, id_span))?;
            }
        }
    }
    Ok(())
}

fn validate_is_plannable(
    conjunction: &Conjunction,
    input_variables: &BTreeSet<Variable>,
    variable_registry: &VariableRegistry,
) -> Result<(), Box<RepresentationError>> {
    validate_conjunction_has_valid_constraint_ordering(conjunction, input_variables, variable_registry)?;
    // Note: Passing ONLY required inputs may be too strict if we change behaviour in future.
    // Then just push this recursive check alongside the shallow check in the _impl method.
    conjunction.nested_patterns().iter().try_for_each(|nested| match nested {
        NestedPattern::Disjunction(disjunction) => {
            let inner_inputs = disjunction.required_inputs().collect();
            disjunction
                .conjunctions()
                .iter()
                .try_for_each(|inner| validate_is_plannable(inner, &inner_inputs, variable_registry))
        }
        NestedPattern::Optional(inner) => {
            let inner_inputs = inner.required_inputs().collect();
            validate_is_plannable(inner.conjunction(), &inner_inputs, variable_registry)
        }
        NestedPattern::Negation(inner) => {
            let inner_inputs = inner.required_inputs().collect();
            validate_is_plannable(inner.conjunction(), &inner_inputs, variable_registry)
        }
    })?;
    Ok(())
}

fn validate_conjunction_has_valid_constraint_ordering(
    conjunction: &Conjunction,
    input_variables: &BTreeSet<Variable>,
    variable_registry: &VariableRegistry,
) -> Result<(), Box<RepresentationError>> {
    let constraint_at = |i: usize| &conjunction.constraints()[i];

    let disjunction_at = |i: usize| conjunction.nested_patterns()[i].as_disjunction().unwrap();

    let mut remaining_constraint_indices: HashSet<usize> =
        conjunction.constraints().iter().enumerate().map(|(i, _)| i).collect();
    let mut remaining_disjunction_indices: HashSet<usize> =
        conjunction.nested_patterns().iter().positions(|n| n.as_disjunction().is_some()).collect();

    let mut bound_variables = input_variables.clone();

    loop {
        let enabled_constraint_indices = remaining_constraint_indices
            .iter()
            .copied()
            .filter(|i| {
                let mut required_vars =
                    constraint_at(*i).binding_modes().filter_map(|(id, mode)| mode.is_require_prebound().then_some(id));
                required_vars.all(|v| bound_variables.contains(&v))
            })
            .collect::<HashSet<usize>>();
        let fresh_constraint_vars = enabled_constraint_indices.iter().flat_map(|i| constraint_at(*i).ids());

        let enabled_disjunction_indices = remaining_disjunction_indices
            .iter()
            .copied()
            .filter(|i| {
                let mut required_vars = disjunction_at(*i).required_inputs();
                required_vars.all(|v| bound_variables.contains(&v))
            })
            .collect::<HashSet<usize>>();
        let fresh_disjunction_vars =
            enabled_disjunction_indices.iter().flat_map(|i| disjunction_at(*i).visible_referenced_variables());

        remaining_constraint_indices.retain(|i| !enabled_constraint_indices.contains(i));
        remaining_disjunction_indices.retain(|i| !enabled_disjunction_indices.contains(i));

        bound_variables.extend(fresh_constraint_vars);
        bound_variables.extend(fresh_disjunction_vars);
        if enabled_constraint_indices.is_empty() && enabled_disjunction_indices.is_empty() {
            break;
        }
    }
    let remaining_constraints = remaining_constraint_indices.iter().map(|i| constraint_at(*i));
    let remaining_nested_patterns = conjunction
        .nested_patterns()
        .iter()
        .enumerate()
        .filter(|(i, nested)| {
            match nested {
                NestedPattern::Disjunction(_) => {
                    remaining_disjunction_indices.contains(i) // In remaining_disjunction_indices,
                }
                NestedPattern::Optional(optional) => {
                    optional.required_inputs().any(|id| !bound_variables.contains(&id))
                }
                NestedPattern::Negation(negation) => {
                    negation.required_inputs().any(|id| !bound_variables.contains(&id))
                }
            }
        })
        .map(|(i, nested)| nested);
    let unplannable_constraints = UnplannableConstraints::build(
        &bound_variables,
        variable_registry,
        remaining_constraints,
        remaining_nested_patterns,
    );
    if unplannable_constraints.constraints_and_requirements.is_empty() {
        Ok(())
    } else {
        let earliest_span = unplannable_constraints
            .constraints_and_requirements
            .iter()
            .filter_map(|(_, _, span)| *span)
            .min_by_key(|span| span.begin_offset);
        Err(Box::new(RepresentationError::UnplannableConjunction { span: earliest_span, unplannable_constraints }))
    }
}

#[derive(Debug, Clone)]
pub struct UnplannableConstraints {
    constraints_and_requirements: Vec<(String, String, Option<Span>)>,
}

impl UnplannableConstraints {
    fn build<'conj>(
        bound_variables: &BTreeSet<Variable>,
        variable_registry: &VariableRegistry,
        remaining_constraints: impl Iterator<Item = &'conj Constraint<Variable>>,
        remaining_nested_patterns: impl Iterator<Item = &'conj NestedPattern>,
    ) -> Self {
        macro_rules! unsatisfied_vars {
            ($iter:expr) => {
                $iter
                    .filter(|id| !bound_variables.contains(id))
                    .map(|id| variable_registry.get_variable_name_or_unnamed(id))
                    .join(", ")
            };
        }
        let mut constraints_and_requirements = Vec::new();
        constraints_and_requirements.extend(remaining_constraints.map(|c| {
            let required_vars = c.binding_modes().filter_map(|(id, mode)| mode.is_require_prebound().then_some(id));
            (c.name().to_owned(), unsatisfied_vars!(required_vars), c.source_span())
        }));
        constraints_and_requirements.extend(remaining_nested_patterns.map(|nested| {
            let (name, required_vars) = match nested {
                NestedPattern::Disjunction(disj) => ("Disjunction", unsatisfied_vars!(disj.required_inputs())),
                NestedPattern::Negation(negation) => ("Negation", unsatisfied_vars!(negation.required_inputs())),
                NestedPattern::Optional(optional) => ("Optional", unsatisfied_vars!(optional.required_inputs())),
            };
            (name.to_owned(), required_vars, nested.source_span())
        }));
        Self { constraints_and_requirements }
    }
}

impl fmt::Display for UnplannableConstraints {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (name, required_vars, _span) in &self.constraints_and_requirements {
            writeln!(f, "- {name}: [{required_vars}]")?;
        }
        Ok(())
    }
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

    fn registered_variables(&self) -> impl Iterator<Item = Variable> + '_ {
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

    pub(crate) fn get_variable_name_or_unnamed(&self, variable: Variable) -> &str {
        self.variable_registry.get_variable_name_or_unnamed(variable)
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
