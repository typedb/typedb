/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::{HashMap, HashSet};

use answer::{Type, variable::Variable};
use concept::type_::type_manager::TypeManager;
use ir::pattern::conjunction::Conjunction;
use ir::pattern::constraint::ExpressionBinding;
use ir::pattern::expression::Expression;
use ir::pattern::nested_pattern::NestedPattern;
use ir::pattern::variable_category::VariableCategory;
use ir::program::block::FunctionalBlock;
use storage::snapshot::ReadableSnapshot;

use crate::expressions::expression_compiler::{CompiledExpression, ExpressionTreeCompiler, ExpressionValueType};
use crate::type_inference::TypeAnnotations;
use crate::TypeInferenceError;

struct ExpressionInferenceContext<'this, Snapshot: ReadableSnapshot> {
    block: &'this FunctionalBlock,
    snapshot: &'this Snapshot,
    type_manager: &'this TypeManager,

    type_annotations: &'this TypeAnnotations,
    expressions_by_assignment: HashMap<Variable, &'this ExpressionBinding<Variable>>,
}

pub fn compile_expressions_in_block<'this, Snapshot: ReadableSnapshot>(
    snapshot: &'this Snapshot,
    type_manager: &'this TypeManager,
    block: &'this FunctionalBlock,
    type_annotations: &'this TypeAnnotations,
) -> Result<HashMap<Variable, Option<CompiledExpression>>, TypeInferenceError> {
    let mut expression_index = HashMap::new();
    index_expressions(block.conjunction(), &mut expression_index)?;
    let expression_bindings: Vec<&ExpressionBinding<Variable>> =
        expression_index.values().map(|by_ref| by_ref.clone()).collect();
    let context = ExpressionInferenceContext {
        block,
        snapshot,
        type_manager,
        type_annotations,
        expressions_by_assignment: expression_index,
    };

    let mut compiled_expressions: HashMap<Variable, Option<CompiledExpression>> = HashMap::new();
    for binding in expression_bindings {
        compile_expressions_recursive(&context, binding, &mut compiled_expressions)?
    }
    Ok(compiled_expressions)
}

fn index_expressions<'block>(
    conjunction: &'block Conjunction,
    index: &mut HashMap<Variable, &'block ExpressionBinding<Variable>>,
) -> Result<(), TypeInferenceError> {
    for constraint in conjunction.constraints() {
        if let Some(expression_binding) = constraint.as_expression_binding() {
            if index.contains_key(&expression_binding.left()) {
                Err(TypeInferenceError::MultipleAssignmentsForSingleVariable {
                    variable: expression_binding.left().clone(),
                })?;
            }
            index.insert(expression_binding.left().clone(), expression_binding);
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

fn compile_expressions_recursive<'context, Snapshot: ReadableSnapshot>(
    context: &ExpressionInferenceContext<'context, Snapshot>,
    binding: &ExpressionBinding<Variable>,
    compiled_expressions: &mut HashMap<Variable, Option<CompiledExpression>>,
) -> Result<(), TypeInferenceError> {
    debug_assert!(!compiled_expressions.contains_key(binding.left()));
    compiled_expressions.insert(binding.left().clone(), None);
    // Compile any dependent expressions first
    let mut variable_value_types: HashMap<Variable, ExpressionValueType> = HashMap::new();
    for expr in binding.expression().tree() {
        let variable_opt = match expr {
            Expression::Variable(variable) => Some(*variable),
            Expression::ListIndex(list_index) => Some(list_index.list_variable()),
            Expression::ListIndexRange(list_range) => Some(list_range.list_variable()),
            Expression::Constant(_) | Expression::Operation(_) | Expression::BuiltInCall(_) | Expression::List(_) => {
                None
            }
        };
        if let Some(variable) = variable_opt {
            variable_value_types
                .insert(variable.clone(), resolve_type_for_variable(context, variable, compiled_expressions)?);
        }
    }
    let compiled = ExpressionTreeCompiler::compile(&binding.expression(), variable_value_types)
        .map_err(|source| TypeInferenceError::ExpressionCompilation { source })?;
    compiled_expressions.insert(binding.left().clone(), Some(compiled));
    Ok(())
}

fn resolve_type_for_variable<'context, Snapshot: ReadableSnapshot>(
    context: &ExpressionInferenceContext<'context, Snapshot>,
    variable: Variable,
    compiled_expressions: &mut HashMap<Variable, Option<CompiledExpression>>,
) -> Result<ExpressionValueType, TypeInferenceError> {
    if let Some(binding) = context.expressions_by_assignment.get(&variable) {
        let compiled_expression = match compiled_expressions.get(&variable) {
            Some(None) => Err(TypeInferenceError::CircularDependencyInExpressions { variable: variable })?,
            Some(Some(compiled_expression)) => compiled_expression,
            None => {
                compile_expressions_recursive(context, binding, compiled_expressions)?;
                compiled_expressions.get(&variable).unwrap().as_ref().unwrap()
            }
        };
        // TODO: We're throwing off the information about the category here
        Ok(compiled_expression.return_type())
    } else if let Some(types) = context.type_annotations.variable_annotations(variable) {
        let vec = types
            .iter()
            .map(|type_| match type_ {
                Type::Attribute(attribute_type) => attribute_type
                    .get_value_type(context.snapshot, context.type_manager)
                    .map_err(|source| TypeInferenceError::ConceptRead { source }),
                _ => Ok(None),
            })
            .collect::<Result<HashSet<_>, TypeInferenceError>>()?;
        if vec.len() != 1 {
            Err(TypeInferenceError::ExpressionVariableDidNotHaveSingleValueType { variable: variable.clone() })
        } else if let Some(value_type) = &vec.iter().find(|_| true).unwrap() {
            let variable_category = context.block.context().get_variable_category(variable.clone()).unwrap();
            match variable_category {
                VariableCategory::Attribute | VariableCategory::Value => {
                    Ok(ExpressionValueType::Single(value_type.category()))
                }
                VariableCategory::AttributeList | VariableCategory::ValueList => {
                    Ok(ExpressionValueType::List(value_type.category()))
                }
                _ => Err(TypeInferenceError::VariableInExpressionMustBeValueOrAttribute {
                    variable: variable.clone(),
                    actual_category: variable_category,
                })?,
            }
        } else {
            Err(TypeInferenceError::ExpressionVariableHasNoValueType { variable: variable.clone() })
        }
    } else {
        Err(TypeInferenceError::CouldNotDetermineValueTypeForVariable { variable: variable.clone() })
    }
}
