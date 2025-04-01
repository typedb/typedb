/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::{BTreeMap, HashMap, HashSet};

use answer::variable::Variable;
use concept::type_::type_manager::TypeManager;
use ir::{
    pattern::{
        conjunction::Conjunction,
        constraint::{Constraint, ExpressionBinding},
        disjunction::Disjunction,
        nested_pattern::NestedPattern,
        variable_category::VariableCategory,
        Vertex,
    },
    pipeline::{block::Block, ParameterRegistry, VariableRegistry},
};
use itertools::Itertools;
use storage::snapshot::ReadableSnapshot;
use typeql::common::Span;
use ir::pattern::{Scope, ScopeId};

use crate::annotation::{
    expression::{
        compiled_expression::{ExecutableExpression, ExpressionValueType},
        expression_compiler::ExpressionCompilationContext,
        ExpressionCompileError,
    },
    type_annotations::BlockAnnotations,
    type_inference::resolve_value_types,
};

type AssignmentIndex<'a> = HashMap<Variable, Vec<(ScopeId, &'a ExpressionBinding<Variable>)>>;

struct BlockExpressionsCompilationContext<'block, Snapshot: ReadableSnapshot> {
    block: &'block Block,
    variable_registry: &'block VariableRegistry,
    parameters: &'block ParameterRegistry,

    snapshot: &'block Snapshot,
    type_manager: &'block TypeManager,
    block_annotations: &'block BlockAnnotations,

    variable_value_types: HashMap<Variable, ExpressionValueType>,
    compiled_expressions: HashMap<ExpressionBinding<Variable>, ExecutableExpression<Variable>>,
    visited_expressions: HashSet<Variable>,
}

impl<'block, Snapshot: ReadableSnapshot> BlockExpressionsCompilationContext<'block, Snapshot> {
    pub(crate) fn variable_name(&self, variable: &Variable) -> String {
        self.variable_registry
            .variable_names()
            .get(variable)
            .cloned()
            .unwrap_or_else(|| VariableRegistry::UNNAMED_VARIABLE_DISPLAY_NAME.to_string())
    }
}

pub fn compile_expressions<'block, Snapshot: ReadableSnapshot>(
    snapshot: &'block Snapshot,
    type_manager: &'block TypeManager,
    block: &'block Block,
    variable_registry: &'block mut VariableRegistry,
    parameters: &'block ParameterRegistry,
    type_annotations: &'block BlockAnnotations,
    input_value_type_annotations: &mut BTreeMap<Variable, ExpressionValueType>,
) -> Result<HashMap<ExpressionBinding<Variable>, ExecutableExpression<Variable>>, Box<ExpressionCompileError>> {
    let mut context = BlockExpressionsCompilationContext {
        block,
        variable_registry,
        parameters,
        snapshot,
        type_manager,
        block_annotations: type_annotations,
        variable_value_types: input_value_type_annotations.iter().map(|(&k, v)| (k, v.clone())).collect(),
        visited_expressions: HashSet::new(),
        compiled_expressions: HashMap::new(),
    };
    let mut expression_index = HashMap::new();
    index_expressions_conjunction(&context, block.conjunction(), &mut expression_index)?;
    if let Some(var) = expression_index.keys().find(|var| input_value_type_annotations.contains_key(var)) {
        return Err(Box::new(ExpressionCompileError::ReassigningValueVariableFromPreviousStage {
            variable: context.variable_name(var),
        }));
    }
    for (var, _) in &expression_index {
        if !context.visited_expressions.contains(var) {
            let _value_type = try_value_type_from_assignments(&mut context, *var, &expression_index)?;
            debug_assert!(_value_type.is_some());
        }
    }

    let BlockExpressionsCompilationContext { compiled_expressions, .. } = context;
    for (binding, compiled) in &compiled_expressions {
        let assigned = binding.left().as_variable().unwrap();
        let source = Constraint::ExpressionBinding(binding.clone());
        variable_registry
            .set_assigned_value_variable_category(assigned, compiled.return_category(), source)
            .map_err(|typedb_source| Box::new(ExpressionCompileError::Representation { typedb_source }))?;
    }
    Ok(compiled_expressions)
}

fn index_expressions_conjunction<'block, Snapshot: ReadableSnapshot>(
    context: &BlockExpressionsCompilationContext<'_, Snapshot>,
    conjunction: &'block Conjunction,
    index: &mut AssignmentIndex<'block>,
) -> Result<(), Box<ExpressionCompileError>> {
    for expression_binding in conjunction.constraints().iter().filter_map(|c| c.as_expression_binding()) {
        let left = expression_binding.left().as_variable().unwrap();
        if index.insert(left, vec![(conjunction.scope_id(), expression_binding)]).is_some() {
            return Err(Box::new(ExpressionCompileError::MultipleAssignmentsForVariable {
                variable: context.variable_name(&left),
                source_span: expression_binding.source_span(),
            }))?;
        }
    }

    for nested in conjunction.nested_patterns() {
        match nested {
            NestedPattern::Disjunction(disjunction) => {
                index_expressions_disjunction(context, disjunction, index)?;
            }
            NestedPattern::Negation(negation) => {
                index_expressions_conjunction(context, negation.conjunction(), index)?;
            }
            NestedPattern::Optional(optional) => {
                index_expressions_conjunction(context, optional.conjunction(), index)?;
            }
        }
    }
    Ok(())
}

fn index_expressions_disjunction<'block, Snapshot: ReadableSnapshot>(
    context: &BlockExpressionsCompilationContext<'_, Snapshot>,
    disjunction: &'block Disjunction,
    index: &mut AssignmentIndex<'block>,
) -> Result<(), Box<ExpressionCompileError>> {
    let mut combined_indices: AssignmentIndex<'block> = HashMap::new();
    let branch_indices = disjunction
        .conjunctions()
        .iter()
        .map(|branch| {
            let mut branch_index = HashMap::new();
            index_expressions_conjunction(context, branch, &mut branch_index).map(|_| branch_index)
        })
        .collect::<Result<Vec<_>, _>>()?;
    branch_indices
        .into_iter()
        .flat_map(|branch_index| branch_index.into_iter())
        .for_each(|(var, expressions)| combined_indices.entry(var).or_default().extend(expressions));
    combined_indices.into_iter().try_for_each(|(var, expressions)| match index.insert(var.clone(), expressions) {
        Some(_) => {
            debug_assert!(!index.get(&var).unwrap().is_empty());
            Err(ExpressionCompileError::MultipleAssignmentsForVariable {
                variable: context.variable_name(&var),
                source_span: index.get(&var).unwrap().first().unwrap().1.source_span(),
            })
        }
        None => Ok(()),
    })?;
    Ok(())
}

#[allow(clippy::map_entry, reason = "false positive, this is not a trivial `contains_key()` followed by `insert()`")]
fn resolve_type_for_variable<'a, Snapshot: ReadableSnapshot>(
    context: &mut BlockExpressionsCompilationContext<'a, Snapshot>,
    scope_id: ScopeId,
    variable: Variable,
    expression_assignments: &AssignmentIndex<'a>,
    assignment_span: Option<Span>,
) -> Result<ExpressionValueType, Box<ExpressionCompileError>> {
    if let Some(value) = context.variable_value_types.get(&variable) {
        Ok(value.clone())
    } else if let Some(value) = try_value_type_from_assignments(context, variable, expression_assignments)? {
        Ok(value)
    } else if let Some(value) = try_value_type_from_type_annotations(context, scope_id, variable, assignment_span)? {
        Ok(value)
    } else {
        Err(Box::new(ExpressionCompileError::CouldNotDetermineValueTypeForVariable {
            variable: context.variable_name(&variable),
            source_span: assignment_span,
        }))
    }
}

fn try_value_type_from_assignments<'a, Snapshot: ReadableSnapshot>(
    context: &mut BlockExpressionsCompilationContext<'a, Snapshot>,
    variable: Variable,
    expression_assignments: &AssignmentIndex<'a>,
) -> Result<Option<ExpressionValueType>, Box<ExpressionCompileError>> {
    if let Some(assignments_for_variable) = expression_assignments.get(&variable) {
        if !context.visited_expressions.insert(variable) {
            return Err(Box::new(ExpressionCompileError::CircularDependency {
                variable: context.variable_name(&variable),
                source_span: None,
            }));
        }
        let mut return_types = HashSet::new();
        for (scope_id, assignment) in assignments_for_variable {
            assignment.expression().variables().try_for_each(|var| {
                resolve_type_for_variable(context, *scope_id, var, expression_assignments, assignment.source_span()).map(|_| ())
            })?;
            let compiled = ExpressionCompilationContext::compile(
                assignment.expression(),
                &context.variable_value_types,
                context.parameters,
            )?;
            return_types.insert(compiled.return_type.clone());
            context.compiled_expressions.insert((*assignment).clone(), compiled);
        }
        if let Ok(value_type) = return_types.iter().exactly_one() {
            context.variable_value_types.insert(variable, value_type.clone());
            Ok(Some(value_type.clone()))
        } else {
            debug_assert!(return_types.len() > 1);
            Err(Box::new(ExpressionCompileError::ValueVariableConflictingAssignmentTypes {
                variable: context.variable_name(&variable),
                value_types: return_types.iter().join(", "),
                source_span: None,
            }))
        }
    } else {
        Ok(None)
    }
}

fn try_value_type_from_type_annotations<Snapshot: ReadableSnapshot>(
    context: &mut BlockExpressionsCompilationContext<'_, Snapshot>,
    scope_id: ScopeId,
    variable: Variable,
    source_span: Option<Span>,
) -> Result<Option<ExpressionValueType>, Box<ExpressionCompileError>> {
    let type_annotations = context.block_annotations.type_annotations_of_scope(scope_id).unwrap();
    let Some(annotations) = type_annotations.vertex_annotations_of(&Vertex::Variable(variable)) else {
        return Ok(None);
    };
    let variable_category = context.variable_registry.get_variable_category(variable).unwrap();
    let is_list = match variable_category {
        VariableCategory::Value | VariableCategory::Attribute | VariableCategory::Thing => false,
        VariableCategory::ValueList | VariableCategory::AttributeList | VariableCategory::ThingList => true,
        _ => {
            return Err(Box::new(ExpressionCompileError::VariableMustBeValueOrAttribute {
                variable: context.variable_name(&variable),
                category: variable_category,
                source_span,
            }));
        }
    };
    let value_types = resolve_value_types(annotations, context.snapshot, context.type_manager).map_err(|_source| {
        Box::new(ExpressionCompileError::CouldNotDetermineValueTypeForVariable {
            variable: context.variable_name(&variable),
            source_span,
        })
    })?;
    let unique_value_type = value_types
        .iter()
        .exactly_one()
        .map_err(|_| {
            Box::new(ExpressionCompileError::VariableMultipleValueTypes {
                variable: context.variable_name(&variable),
                value_types: value_types.iter().join(", "),
                source_span,
            })
        })?
        .clone();
    let expression_value_type = match is_list {
        true => ExpressionValueType::List(unique_value_type),
        false => ExpressionValueType::Single(unique_value_type),
    };
    context.variable_value_types.insert(variable, expression_value_type.clone());
    Ok(Some(expression_value_type))
}
