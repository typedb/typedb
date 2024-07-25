/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

/*
We want to determine the output value type of each Expression ( -> Comparisons )

We decided that Expressions and Comparisons are not 'constraining', eg `$x + 4` does not enforce value type constraints on $x to be convertible to long.
 */

use std::collections::{HashMap, HashSet};

use answer::{variable::Variable, Type};
use concept::type_::type_manager::TypeManager;
use encoding::value::value_type::ValueTypeCategory;
use itertools::{cloned, Itertools};
use storage::snapshot::ReadableSnapshot;

use crate::{
    expressions::expression_compiler::{CompiledExpressionTree, ExpressionTreeCompiler},
    inference::{type_inference::TypeAnnotations, TypeInferenceError},
    pattern::{
        conjunction::Conjunction, constraint::ExpressionBinding, expression::Expression, nested_pattern::NestedPattern,
    },
    program::block::FunctionalBlock,
};

struct ValueTypeInference<'this, Snapshot: ReadableSnapshot> {
    snapshot: &'this Snapshot,
    type_manager: &'this TypeManager,

    type_annotations: &'this TypeAnnotations,
    expressions_by_assignment: HashMap<Variable, &'this ExpressionBinding<Variable>>,
}

impl<'this, Snapshot: ReadableSnapshot> ValueTypeInference<'this, Snapshot> {
    pub fn compile_expressions_in_block(
        snapshot: &'this Snapshot,
        type_manager: &'this TypeManager,
        block: &'this FunctionalBlock,
        type_annotations: &'this TypeAnnotations,
    ) -> Result<HashMap<Variable, Option<CompiledExpressionTree>>, TypeInferenceError> {
        let mut expression_index = HashMap::new();
        Self::index_expressions(block.conjunction(), &mut expression_index)?;
        let expression_bindings: Vec<&ExpressionBinding<Variable>> =
            expression_index.values().map(|by_ref| by_ref.clone()).collect();
        let this = Self { snapshot, type_manager, type_annotations, expressions_by_assignment: expression_index };

        let mut compiled_expressions: HashMap<Variable, Option<CompiledExpressionTree>> = HashMap::new();
        for binding in expression_bindings {
            this.compile_expressions_recursive(binding, &mut compiled_expressions)?
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
                        Self::index_expressions(nested_conjunction, index)?;
                    }
                }
                NestedPattern::Negation(negation) => {
                    Self::index_expressions(negation.conjunction(), index)?;
                }
                NestedPattern::Optional(optional) => {
                    Self::index_expressions(optional.conjunction(), index)?;
                }
            }
        }
        Ok(())
    }

    fn compile_expressions_recursive<'block>(
        &self,
        binding: &ExpressionBinding<Variable>,
        compiled_expressions: &mut HashMap<Variable, Option<CompiledExpressionTree>>,
    ) -> Result<(), TypeInferenceError> {
        debug_assert!(!compiled_expressions.contains_key(binding.left()));
        compiled_expressions.insert(binding.left().clone(), None);
        // Compile any dependent expressions first
        let mut variable_value_types: HashMap<Variable, ValueTypeCategory> = HashMap::new();
        for expr in binding.expression().tree() {
            let variable_opt = match expr {
                Expression::Variable(variable) => Some(variable),
                Expression::ListIndex(list_index) => Some(&list_index.list_variable),
                Expression::ListIndexRange(list_range) => Some(&list_range.list_variable),
                Expression::Constant(_)
                | Expression::Operation(_)
                | Expression::BuiltInCall(_)
                | Expression::List(_) => None,
            };
            if let Some(variable) = variable_opt {
                variable_value_types
                    .insert(variable.clone(), self.resolve_expression_type(variable, compiled_expressions)?);
            }
        }
        let compiled = ExpressionTreeCompiler::compile(&binding.expression(), variable_value_types)
            .map_err(|source| TypeInferenceError::ExpressionCompilation { source })?;
        compiled_expressions.insert(binding.left().clone(), Some(compiled));
        Ok(())
    }

    fn resolve_expression_type(
        &self,
        variable: &Variable,
        compiled_expressions: &mut HashMap<Variable, Option<CompiledExpressionTree>>,
    ) -> Result<ValueTypeCategory, TypeInferenceError> {
        if let Some(binding) = self.expressions_by_assignment.get(variable) {
            let compiled_expression = match compiled_expressions.get(variable) {
                Some(None) => Err(TypeInferenceError::CircularDependencyInExpressions { variable: variable.clone() })?,
                Some(Some(compiled_expression)) => compiled_expression,
                None => {
                    self.compile_expressions_recursive(binding, compiled_expressions)?;
                    compiled_expressions.get(variable).unwrap().as_ref().unwrap()
                }
            };
            Ok(compiled_expression.return_type())
        } else if let Some(types) = self.type_annotations.variable_annotations(variable.clone()) {
            let vec = types
                .iter()
                .map(|type_| match type_ {
                    Type::Attribute(attribute_type) => attribute_type
                        .get_value_type(self.snapshot, self.type_manager)
                        .map_err(|source| TypeInferenceError::ConceptRead { source }),
                    _ => Ok(None),
                })
                .collect::<Result<HashSet<_>, TypeInferenceError>>()?;
            if vec.len() != 1 {
                Err(TypeInferenceError::ExpressionVariableDidNotHaveSingleValueType { variable: variable.clone() })
            } else if let Some(value_type) = &vec.iter().find(|_| true).unwrap() {
                Ok(value_type.category())
            } else {
                Err(TypeInferenceError::ExpressionVariableHasNoValueType { variable: variable.clone() })
            }
        } else {
            Err(TypeInferenceError::CouldNotDetermineValueTypeForVariable { variable: variable.clone() })
        }
    }
}
