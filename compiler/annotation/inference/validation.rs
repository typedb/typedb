/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::collections::HashMap;

use answer::variable::Variable;
use encoding::value::value_type::ValueType;
use ir::{
    pattern::{Vertex, variable_category::VariableCategory},
    pipeline::VariableRegistry,
};
use itertools::Itertools;

use crate::annotation::{
    TypeInferenceError,
    expression::ExpressionCompileError,
    inference::{
        ValueVertexTypes, VertexAnnotations, VertexTypeAnnotations,
        match_inference::{NestedTypeInferenceGraphDisjunction, TypeInferenceExpression, TypeInferenceGraph},
    },
};

pub(super) fn validate_inferred_types_are_valid(
    graph: &TypeInferenceGraph<'_>,
    variable_registry: &VariableRegistry,
) -> Result<(), TypeInferenceError> {
    validate_category_alignment(graph, variable_registry)?; // Could be a debug_assert
    check_thing_constraints_satisfiable(graph, variable_registry)?;
    check_expressions_were_compiled(graph, variable_registry)?;
    check_uniqueness_of_value_types(graph, variable_registry)?;
    Ok(())
}

fn validate_category_alignment(
    graph: &TypeInferenceGraph<'_>,
    variable_registry: &VariableRegistry,
) -> Result<(), TypeInferenceError> {
    graph.vertices.iter().try_for_each(|(vertex, types)| {
        if let Vertex::Variable(var) = vertex {
            let var_category = variable_registry.get_variable_category(*var).expect("Expected category");
            match (var_category, types) {
                (VariableCategory::Value, VertexTypeAnnotations::Value(_)) => Ok(()),
                (VariableCategory::Value, VertexTypeAnnotations::Concept(_)) => {
                    Err(TypeInferenceError::InternalVertexTypesMismatch { expected: "value".to_owned() })
                }
                (other, VertexTypeAnnotations::Concept(_)) => {
                    debug_assert!(other.is_category_thing() || other.is_category_type());
                    Ok(())
                }
                (_other, VertexTypeAnnotations::Value(_)) => {
                    Err(TypeInferenceError::InternalVertexTypesMismatch { expected: "concept".to_owned() })
                }
            }
        } else {
            Ok(())
        }
    })
}

fn check_thing_constraints_satisfiable(
    graph: &TypeInferenceGraph<'_>,
    variable_registry: &VariableRegistry,
) -> Result<(), TypeInferenceError> {
    let thing_variable_present = graph
        .vertices
        .annotations
        .iter()
        .filter_map(|(var, _)| var.as_variable())
        .any(|var| variable_registry.get_variable_category(var).unwrap().is_category_thing());

    let any_vertex_empty = graph.vertices.annotations.iter().any(|(_, types)| types.is_empty());
    if any_vertex_empty && thing_variable_present {
        return Err(TypeInferenceError::DetectedUnsatisfiablePattern {});
    }
    graph
        .nested_disjunctions
        .iter()
        .flat_map(|d| d.disjunction.iter())
        .try_for_each(|branch| check_thing_constraints_satisfiable(branch, variable_registry))?;
    Ok(())
}

fn check_expressions_were_compiled(
    graph: &TypeInferenceGraph<'_>,
    variable_registry: &VariableRegistry,
) -> Result<(), TypeInferenceError> {
    fn join_names(iter: impl Iterator<Item = ValueType>) -> String {
        iter.map(|v| v.category().name()).join(", ")
    }
    fn collect_uncompiled_recursive<'graph, 'conj>(
        graph: &'graph TypeInferenceGraph<'conj>,
        expressions: &mut HashMap<
            Vertex<Variable>,
            Vec<(&'graph TypeInferenceExpression<'conj>, &'graph VertexAnnotations)>,
        >,
    ) {
        graph
            .expressions
            .iter()
            .filter(|e| e.compiled_expression.is_none())
            .for_each(|e| expressions.entry(e.assigned.clone()).or_default().push((e, &graph.vertices)));
        graph
            .nested_disjunctions
            .iter()
            .flat_map(|d| d.disjunction.iter())
            .for_each(|g| collect_uncompiled_recursive(g, expressions))
    }
    let mut uncompiled = HashMap::new();
    collect_uncompiled_recursive(graph, &mut uncompiled);
    if uncompiled.is_empty() {
        return Ok(());
    }

    let leaf_uncompiled =
        uncompiled.values().flat_map(|v| v.iter()).find(|(e, _)| e.args.iter().all(|v| !uncompiled.contains_key(v)));
    if let Some((expr, vertex_annotations)) = leaf_uncompiled {
        let bad_arg_opt = expr.args.iter().find_map(|arg| {
            let exactly_one_err = match &vertex_annotations[arg] {
                VertexTypeAnnotations::Concept(types) => {
                    expr.attribute_value_types(types).unique().exactly_one().map_err(join_names)
                }
                VertexTypeAnnotations::Value(types) => types.iter().copied().exactly_one().map_err(join_names),
            };
            match exactly_one_err {
                Ok(_) => None,
                Err(types) => Some((arg, types)),
            }
        });
        debug_assert!(bad_arg_opt.is_some(), "Should have been compiled if there isn't a bad arg");
        if let Some((arg, value_types)) = bad_arg_opt {
            let variable = variable_registry.get_variable_name_or_unnamed(arg.as_variable().unwrap()).to_owned();
            let source_span = expr.expression.source_span();
            return Err(TypeInferenceError::ExpressionCompilation {
                typedb_source: Box::new(ExpressionCompileError::VariableMultipleValueTypes {
                    variable,
                    value_types,
                    source_span,
                }),
            });
        }
    }
    debug_assert!(false && leaf_uncompiled.is_some(), "Unreachable: we've already caught circular-dependencies");
    let assigned_variables =
        uncompiled.keys().map(|v| variable_registry.get_variable_name_or_unnamed(v.as_variable().unwrap())).join(", ");
    Err(TypeInferenceError::InternalUnresolvedExpressions { assigned_variables })
}

fn check_uniqueness_of_value_types(
    graph: &TypeInferenceGraph<'_>,
    variable_registry: &VariableRegistry,
) -> Result<(), TypeInferenceError> {
    // I think this can only happen at a disjunction, so we focus on validating those first.
    graph.nested_disjunctions.iter().try_for_each(|disjunction| {
        check_uniqueness_of_value_types_at_disjunctions(disjunction, variable_registry)
            .map_err(|typedb_source| TypeInferenceError::ExpressionCompilation { typedb_source })
    })?;

    check_uniqueness_of_value_types_in_conjunction(graph, variable_registry)
        .map_err(|typedb_source| TypeInferenceError::ExpressionCompilation { typedb_source })?;

    Ok(())
}

fn check_uniqueness_of_value_types_at_disjunctions(
    disjunction: &NestedTypeInferenceGraphDisjunction<'_>,
    variable_registry: &VariableRegistry,
) -> Result<(), Box<ExpressionCompileError>> {
    // We first recurse so we get the closest error span possible:
    disjunction
        .disjunction
        .iter()
        .flat_map(|d| d.nested_disjunctions.iter())
        .try_for_each(|nested| check_uniqueness_of_value_types_at_disjunctions(nested, variable_registry))?;
    if let Some((variable, value_types)) = find_multiply_typed_value_vertex(&disjunction.shared_vertex_annotations) {
        let variable = variable_registry.get_variable_name_or_unnamed(variable).to_owned();
        let value_types = join_value_type_names(value_types.iter());
        let source_span = disjunction.disjunction_pattern.source_span();
        Err(Box::new(ExpressionCompileError::ValueVariableConflictingAssignmentTypes {
            variable,
            value_types,
            source_span,
        }))
    } else {
        Ok(())
    }
}

fn check_uniqueness_of_value_types_in_conjunction(
    graph: &TypeInferenceGraph<'_>,
    variable_registry: &VariableRegistry,
) -> Result<(), Box<ExpressionCompileError>> {
    if let Some((variable, value_types)) = find_multiply_typed_value_vertex(&graph.vertices) {
        let variable = variable_registry.get_variable_name_or_unnamed(variable).to_owned();
        let value_types = join_value_type_names(value_types.iter());
        let source_span = None; // Can't do much with a conjunction
        Err(Box::new(ExpressionCompileError::ValueVariableConflictingAssignmentTypes {
            variable,
            value_types,
            source_span,
        }))
    } else {
        graph
            .nested_disjunctions
            .iter()
            .flat_map(|d| d.disjunction.iter())
            .try_for_each(|branch| check_uniqueness_of_value_types_in_conjunction(branch, variable_registry))
    }
}

fn find_multiply_typed_value_vertex(vertex_annotations: &VertexAnnotations) -> Option<(Variable, &ValueVertexTypes)> {
    vertex_annotations
        .iter()
        .filter_map(|(vertex, types)| {
            let variable = vertex.as_variable()?;
            match types {
                VertexTypeAnnotations::Value(value_types) => Some((variable, value_types)),
                VertexTypeAnnotations::Concept(_) => None,
            }
        })
        .find(|(_, value_types)| value_types.len() > 1)
}

fn join_value_type_names<'a>(value_types: impl Iterator<Item = &'a ValueType>) -> String {
    value_types.map(|value_type| value_type.category()).join(", ")
}
