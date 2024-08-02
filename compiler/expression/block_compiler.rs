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
        conjunction::Conjunction, expression::ExpressionTree, nested_pattern::NestedPattern,
        variable_category::VariableCategory,
    },
    program::block::FunctionalBlock,
};
use itertools::Itertools;
use storage::snapshot::ReadableSnapshot;

use crate::{
    expression::{
        compiled_expression::{CompiledExpression, ExpressionValueType},
        expression_compiler::ExpressionCompilationContext,
    },
    inference::{type_annotations::TypeAnnotations, TypeInferenceError},
};
use crate::expression::ExpressionCompileError;

struct BlockExpressionsCompilationContext<'block, Snapshot: ReadableSnapshot> {
    block: &'block FunctionalBlock,
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
    type_annotations: &'block TypeAnnotations,
) -> Result<HashMap<Variable, CompiledExpression>, ExpressionCompileError> {
    let mut expression_index = HashMap::new();
    index_expressions(block.conjunction(), &mut expression_index)?;
    let assigned_variables = expression_index.keys().cloned().collect_vec();
    let mut context = BlockExpressionsCompilationContext {
        block,
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
    Ok(context.compiled_expressions)
}

fn index_expressions<'block>(
    conjunction: &'block Conjunction,
    index: &mut HashMap<Variable, &'block ExpressionTree<Variable>>,
) -> Result<(), ExpressionCompileError> {
    for constraint in conjunction.constraints() {
        if let Some(expression_binding) = constraint.as_expression_binding() {
            if index.contains_key(&expression_binding.left()) {
                Err(ExpressionCompileError::MultipleAssignmentsForSingleVariable { assign_variable: expression_binding.left() })?;
            }
            index.insert(expression_binding.left(), expression_binding.expression());
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
    expression_assignments: &HashMap<Variable, &'a ExpressionTree<Variable>>,
) -> Result<(), ExpressionCompileError> {
    context.visited_expressions.insert(assigned_variable);
    let expression = expression_assignments.get(&assigned_variable).unwrap();
    for variable in expression.variables() {
        resolve_type_for_variable(context, variable, expression_assignments)?;
    }
    let compiled = ExpressionCompilationContext::compile(expression, &context.variable_value_types)?;
    context.compiled_expressions.insert(assigned_variable, compiled);
    Ok(())
}

fn resolve_type_for_variable<'a, Snapshot: ReadableSnapshot>(
    context: &mut BlockExpressionsCompilationContext<'a, Snapshot>,
    variable: Variable,
    expression_assignments: &HashMap<Variable, &'a ExpressionTree<Variable>>,
) -> Result<(), ExpressionCompileError> {
    if expression_assignments.contains_key(&variable) {
        if !context.compiled_expressions.contains_key(&variable) {
            if context.visited_expressions.contains(&variable) {
                return Err(ExpressionCompileError::CircularDependencyInExpressions { assign_variable: variable })?;
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
    } else if let Some(types) = context.type_annotations.variable_annotations_of(variable) {
        let vec = types
            .iter()
            .map(|type_| match type_ {
                Type::Attribute(attribute_type) => attribute_type
                    .get_value_type(context.snapshot, context.type_manager)
                    .map_err(|source| ExpressionCompileError::ConceptRead { source }),
                _ => Ok(None),
            })
            .collect::<Result<HashSet<_>, ExpressionCompileError>>()?;
        if vec.len() != 1 {
            Err(ExpressionCompileError::VariableDidNotHaveSingleValueType { variable })
        } else if let Some(value_type) = &vec.iter().find(|_| true).unwrap() {
            let variable_category = context.block.context().get_variable_category(variable).unwrap();
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
        } else {
            Err(ExpressionCompileError::VariableHasNoValueType { variable })
        }
    } else {
        Err(ExpressionCompileError::CouldNotDetermineValueTypeForVariable { variable })
    }
}
