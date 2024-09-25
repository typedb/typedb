/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::{HashMap, HashSet};

use answer::{variable::Variable, Type};
use concept::type_::type_manager::TypeManager;
use ir::{
    pattern::{
        conjunction::Conjunction,
        constraint::{Constraint, ExpressionBinding},
        nested_pattern::NestedPattern,
        variable_category::VariableCategory,
        Vertex,
    },
    program::block::{FunctionalBlock, ParameterRegistry, VariableRegistry},
};
use itertools::Itertools;
use storage::snapshot::ReadableSnapshot;

use crate::{
    expression::{
        compiled_expression::{CompiledExpression, ExpressionValueType},
        expression_compiler::ExpressionCompilationContext,
        ExpressionCompileError,
    },
    match_::inference::{type_annotations::TypeAnnotations, type_inference::resolve_value_types, TypeInferenceError},
};

struct BlockExpressionsCompilationContext<'block, Snapshot: ReadableSnapshot> {
    block: &'block FunctionalBlock,
    variable_registry: &'block VariableRegistry,
    parameters: &'block ParameterRegistry,

    snapshot: &'block Snapshot,
    type_manager: &'block TypeManager,
    type_annotations: &'block TypeAnnotations,

    compiled_expressions: HashMap<Variable, CompiledExpression>,
    variable_value_types: HashMap<Variable, ExpressionValueType>,
    visited_expressions: HashSet<Variable>,
}

pub fn compile_expressions<'block, Snapshot: ReadableSnapshot>(
    snapshot: &'block Snapshot,
    type_manager: &'block TypeManager,
    block: &'block FunctionalBlock,
    variable_registry: &'block mut VariableRegistry,
    parameters: &'block ParameterRegistry,
    type_annotations: &'block TypeAnnotations,
) -> Result<HashMap<Variable, CompiledExpression>, ExpressionCompileError> {
    let mut expression_index = HashMap::new();
    index_expressions(block.conjunction(), &mut expression_index)?;
    let assigned_variables = expression_index.keys().cloned().collect_vec();
    let mut context = BlockExpressionsCompilationContext {
        block,
        variable_registry,
        parameters,
        snapshot,
        type_manager,
        type_annotations,
        variable_value_types: HashMap::new(),
        visited_expressions: HashSet::new(),
        compiled_expressions: HashMap::new(),
    };

    for variable in assigned_variables {
        compile_expressions_recursive(&mut context, variable, &expression_index)?
    }

    let BlockExpressionsCompilationContext { compiled_expressions, variable_value_types, .. } = context;
    for (var, compiled) in &compiled_expressions {
        let category = match &compiled.return_type {
            ExpressionValueType::Single(_) => VariableCategory::Value,
            ExpressionValueType::List(_) => VariableCategory::ValueList,
        };
        let existing_category = variable_registry.get_variable_category(var.clone());
        if existing_category.is_none() {
            let source = Constraint::ExpressionBinding((*expression_index.get(var).unwrap()).clone());
            variable_registry.set_assigned_value_variable_category(var.clone(), category, source).unwrap();
        } else if Some(category) != existing_category {
            Err(ExpressionCompileError::DerivedConflictingVariableCategory {
                variable_name: variable_registry.variable_names().get(var).unwrap().clone(),
                derived_category: category,
                existing_category: existing_category.unwrap(),
            })?;
        }
    }
    Ok(compiled_expressions)
}

fn index_expressions<'block>(
    conjunction: &'block Conjunction,
    index: &mut HashMap<Variable, &'block ExpressionBinding<Variable>>,
) -> Result<(), ExpressionCompileError> {
    for constraint in conjunction.constraints() {
        if let Some(expression_binding) = constraint.as_expression_binding() {
            let &Vertex::Variable(left) = expression_binding.left() else { unreachable!() };
            if index.contains_key(&left) {
                Err(ExpressionCompileError::MultipleAssignmentsForSingleVariable { assign_variable: left })?;
            }
            index.insert(left, expression_binding);
        }
    }
    for nested in conjunction.nested_patterns() {
        match nested {
            NestedPattern::Disjunction(disjunction) => {
                for nested_conjunction in disjunction.conjunctions() {
                    index_expressions(nested_conjunction, index)?;
                }
            }
            NestedPattern::Negation(negation) => {
                index_expressions(negation.conjunction(), index)?;
            }
            NestedPattern::Optional(optional) => {
                index_expressions(optional.conjunction(), index)?;
            }
        }
    }
    Ok(())
}

fn compile_expressions_recursive<'a, Snapshot: ReadableSnapshot>(
    context: &mut BlockExpressionsCompilationContext<'a, Snapshot>,
    assigned_variable: Variable,
    expression_assignments: &HashMap<Variable, &'a ExpressionBinding<Variable>>,
) -> Result<(), ExpressionCompileError> {
    context.visited_expressions.insert(assigned_variable);
    let expression = expression_assignments.get(&assigned_variable).unwrap().expression();
    for variable in expression.variables() {
        resolve_type_for_variable(context, variable, expression_assignments)?;
    }
    let compiled =
        ExpressionCompilationContext::compile(expression, &context.variable_value_types, &context.parameters)?;
    context.variable_value_types.insert(assigned_variable, compiled.return_type);
    context.compiled_expressions.insert(assigned_variable, compiled);
    Ok(())
}

fn resolve_type_for_variable<'a, Snapshot: ReadableSnapshot>(
    context: &mut BlockExpressionsCompilationContext<'a, Snapshot>,
    variable: Variable,
    expression_assignments: &HashMap<Variable, &'a ExpressionBinding<Variable>>,
) -> Result<(), ExpressionCompileError> {
    if expression_assignments.contains_key(&variable) {
        if !context.compiled_expressions.contains_key(&variable) {
            if context.visited_expressions.contains(&variable) {
                Err(ExpressionCompileError::CircularDependencyInExpressions { assign_variable: variable })
            } else {
                compile_expressions_recursive(context, variable, expression_assignments)?;
                context
                    .variable_value_types
                    .insert(variable, context.compiled_expressions.get(&variable).unwrap().return_type);
                Ok(())
            }
        } else {
            Ok(())
        }
    } else if let Some(types) = context.type_annotations.vertex_annotations_of(&Vertex::Variable(variable)) {
        let value_types = resolve_value_types(types, context.snapshot, context.type_manager).map_err(|source| {
            ExpressionCompileError::CouldNotDetermineValueTypeForVariable { variable: variable.clone() }
        })?;
        if value_types.len() != 1 {
            Err(ExpressionCompileError::VariableDidNotHaveSingleValueType { variable })
        } else {
            let value_type = value_types.iter().find(|_| true).unwrap();
            let variable_category = context.variable_registry.get_variable_category(variable).unwrap();
            match variable_category {
                VariableCategory::Attribute | VariableCategory::Value => {
                    context.variable_value_types.insert(variable, ExpressionValueType::Single(value_type.category()));
                    Ok(())
                }
                VariableCategory::AttributeList | VariableCategory::ValueList => {
                    context.variable_value_types.insert(variable, ExpressionValueType::List(value_type.category()));
                    Ok(())
                }
                _ => Err(ExpressionCompileError::VariableMustBeValueOrAttribute {
                    variable,
                    actual_category: variable_category,
                })?,
            }
        }
    } else {
        Err(ExpressionCompileError::CouldNotDetermineValueTypeForVariable { variable })
    }
}
