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
        nested_pattern::NestedPattern,
        variable_category::VariableCategory,
        Vertex,
    },
    pipeline::{block::Block, ParameterRegistry, VariableRegistry},
};
use itertools::Itertools;
use storage::snapshot::ReadableSnapshot;

use crate::annotation::{
    expression::{
        compiled_expression::{ExecutableExpression, ExpressionValueType},
        expression_compiler::ExpressionCompilationContext,
        ExpressionCompileError,
    },
    type_annotations::TypeAnnotations,
    type_inference::resolve_value_types,
};

struct BlockExpressionsCompilationContext<'block, Snapshot: ReadableSnapshot> {
    block: &'block Block,
    variable_registry: &'block VariableRegistry,
    parameters: &'block ParameterRegistry,

    snapshot: &'block Snapshot,
    type_manager: &'block TypeManager,
    type_annotations: &'block TypeAnnotations,

    compiled_expressions: HashMap<Variable, ExecutableExpression<Variable>>,
    variable_value_types: HashMap<Variable, ExpressionValueType>,
    visited_expressions: HashSet<Variable>,
}

pub fn compile_expressions<'block, Snapshot: ReadableSnapshot>(
    snapshot: &'block Snapshot,
    type_manager: &'block TypeManager,
    block: &'block Block,
    variable_registry: &'block mut VariableRegistry,
    parameters: &'block ParameterRegistry,
    type_annotations: &'block TypeAnnotations,
    input_value_type_annotations: &mut BTreeMap<Variable, ExpressionValueType>,
) -> Result<HashMap<Variable, ExecutableExpression<Variable>>, ExpressionCompileError> {
    let mut context = BlockExpressionsCompilationContext {
        block,
        variable_registry,
        parameters,
        snapshot,
        type_manager,
        type_annotations,
        variable_value_types: input_value_type_annotations.iter().map(|(&k, v)| (k, v.clone())).collect(),
        visited_expressions: HashSet::new(),
        compiled_expressions: HashMap::new(),
    };
    let mut expression_index = HashMap::new();
    index_expressions(&context, block.conjunction(), &mut expression_index)?;
    let assigned_variables = expression_index.keys().cloned().collect_vec();

    for variable in assigned_variables {
        compile_expressions_recursive(&mut context, variable, &expression_index)?
    }

    let BlockExpressionsCompilationContext { compiled_expressions, .. } = context;
    for (&var, compiled) in &compiled_expressions {
        let category = match &compiled.return_type {
            ExpressionValueType::Single(_) => VariableCategory::Value,
            ExpressionValueType::List(_) => VariableCategory::ValueList,
        };
        let existing_category = variable_registry.get_variable_category(var);
        let source = Constraint::ExpressionBinding((*expression_index.get(&var).unwrap()).clone());
        variable_registry.set_assigned_value_variable_category(var, category, source)
            .map_err(|source| { ExpressionCompileError::Representation { source } })?;
    }
    Ok(compiled_expressions)
}

fn index_expressions<'block, Snapshot: ReadableSnapshot>(
    context: &BlockExpressionsCompilationContext<'_, Snapshot>,
    conjunction: &'block Conjunction,
    index: &mut HashMap<Variable, &'block ExpressionBinding<Variable>>,
) -> Result<(), ExpressionCompileError> {
    for constraint in conjunction.constraints() {
        if let Some(expression_binding) = constraint.as_expression_binding() {
            let &Vertex::Variable(left) = expression_binding.left() else { unreachable!() };
            if index.contains_key(&left) {
                Err(ExpressionCompileError::MultipleAssignmentsForSingleVariable {
                    assign_variable: context.variable_registry.variable_names().get(&left).cloned(),
                })?;
            }
            index.insert(left, expression_binding);
        }
    }
    for nested in conjunction.nested_patterns() {
        match nested {
            NestedPattern::Disjunction(disjunction) => {
                for nested_conjunction in disjunction.conjunctions() {
                    index_expressions(context, nested_conjunction, index)?;
                }
            }
            NestedPattern::Negation(negation) => {
                index_expressions(context, negation.conjunction(), index)?;
            }
            NestedPattern::Optional(optional) => {
                index_expressions(context, optional.conjunction(), index)?;
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
        ExpressionCompilationContext::compile(expression, &context.variable_value_types, context.parameters)?;
    context.variable_value_types.insert(assigned_variable, compiled.return_type.clone());
    context.compiled_expressions.insert(assigned_variable, compiled);
    Ok(())
}

#[allow(clippy::map_entry, reason = "false positive, this is not a trivial `contains_key()` followed by `insert()`")]
fn resolve_type_for_variable<'a, Snapshot: ReadableSnapshot>(
    context: &mut BlockExpressionsCompilationContext<'a, Snapshot>,
    variable: Variable,
    expression_assignments: &HashMap<Variable, &'a ExpressionBinding<Variable>>,
) -> Result<(), ExpressionCompileError> {
    if expression_assignments.contains_key(&variable) {
        if !context.compiled_expressions.contains_key(&variable) {
            if context.visited_expressions.contains(&variable) {
                // TODO: Do we catch double assignments?
                Err(ExpressionCompileError::CircularDependencyInExpressions {
                    assign_variable: context.variable_registry.variable_names().get(&variable).cloned(),
                })
            } else {
                compile_expressions_recursive(context, variable, expression_assignments)?;
                context
                    .variable_value_types
                    .insert(variable, context.compiled_expressions.get(&variable).unwrap().return_type.clone());
                Ok(())
            }
        } else {
            Ok(())
        }
    } else if context.variable_value_types.contains_key(&variable) {
        Ok(())
    } else if let Some(types) = context.type_annotations.vertex_annotations_of(&Vertex::Variable(variable)) {
        // resolve_value_types will error if the type_annotations aren't all attribute(list) types
        let value_types = resolve_value_types(types, context.snapshot, context.type_manager).map_err(|_source| {
            ExpressionCompileError::CouldNotDetermineValueTypeForVariable {
                variable: context.variable_registry.variable_names().get(&variable).cloned(),
            }
        })?;
        if value_types.len() != 1 {
            Err(ExpressionCompileError::VariableDidNotHaveSingleValueType {
                variable: context.variable_registry.variable_names().get(&variable).cloned(),
            })
        } else {
            let value_type = value_types.iter().find(|_| true).unwrap();
            let variable_category = context.variable_registry.get_variable_category(variable).unwrap();
            match variable_category {
                VariableCategory::Value | VariableCategory::Attribute | VariableCategory::Thing => {
                    debug_assert!(types.iter().all(|t| matches!(t, answer::Type::Attribute(_))));
                    context.variable_value_types.insert(variable, ExpressionValueType::Single(value_type.clone()));
                    Ok(())
                }
                VariableCategory::ValueList | VariableCategory::AttributeList | VariableCategory::ThingList => {
                    debug_assert!(types.iter().all(|t| matches!(t, answer::Type::Attribute(_))));
                    context.variable_value_types.insert(variable, ExpressionValueType::List(value_type.clone()));
                    Ok(())
                }
                _ => Err(ExpressionCompileError::VariableMustBeValueOrAttribute {
                    variable: context.variable_registry.variable_names().get(&variable).cloned(),
                    actual_category: variable_category,
                })?, // TODO: I think this is practically unreachable?
            }
        }
    } else {
        Err(ExpressionCompileError::CouldNotDetermineValueTypeForVariable {
            variable: context.variable_registry.variable_names().get(&variable).cloned(),
        })
    }
}
