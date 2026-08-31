/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::collections::HashMap;

use answer::variable::Variable;
use encoding::value::value_type::ValueType;
use ir::{
    pattern::{Vertex, constraint::Constraint, variable_category::VariableCategory},
    pipeline::VariableRegistry,
};
use itertools::Itertools;

use crate::annotation::{
    TypeInferenceError,
    inference::{
        ValueVertexTypes, VertexAnnotations, VertexTypeAnnotations,
        match_inference::{FullTypeInferenceGraph, TypeInferenceExpression},
    },
};

pub(super) fn validate_inferred_types_are_valid(
    graph: &FullTypeInferenceGraph<'_>,
    variable_registry: &VariableRegistry,
) -> Result<(), TypeInferenceError> {
    run_local_validation(graph, variable_registry, validate_category_alignment)?; // Could be a debug_assert
    run_local_validation(graph, variable_registry, check_non_type_constraints_satisfiable)?;

    // check_expressions_were_compiled comes before general uniqueness_of_value_types
    check_expressions_were_compiled(graph, variable_registry)?;

    run_local_validation(graph, variable_registry, check_uniqueness_of_value_types)?;
    Ok(())
}

fn run_local_validation(
    graph: &FullTypeInferenceGraph<'_>,
    variable_registry: &VariableRegistry,
    validation: fn(&FullTypeInferenceGraph<'_>, &VariableRegistry) -> Result<(), TypeInferenceError>,
) -> Result<(), TypeInferenceError> {
    validation(graph, variable_registry)?;
    graph
        .disjunctions
        .iter()
        .flatten()
        .chain(graph.negations.iter())
        .chain(graph.optionals.iter())
        .try_for_each(|nested| run_local_validation(nested, variable_registry, validation))?;
    Ok(())
}

fn validate_category_alignment(
    graph: &FullTypeInferenceGraph<'_>,
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

fn check_non_type_constraints_satisfiable(
    graph: &FullTypeInferenceGraph<'_>,
    _variable_registry: &VariableRegistry,
) -> Result<(), TypeInferenceError> {
    let not_only_type_constraints = graph.conjunction.constraints().iter().any(|constraint| match constraint {
        Constraint::Is(_)
        | Constraint::Isa(_)
        | Constraint::Iid(_)
        | Constraint::Links(_)
        | Constraint::IndexedRelation(_)
        | Constraint::Has(_)
        | Constraint::ExpressionBinding(_)
        | Constraint::FunctionCallBinding(_)
        | Constraint::LinksDeduplication(_)
        | Constraint::DeleteConcepts(_)
        | Constraint::Comparison(_) => true,

        Constraint::IsSet(_)
        | Constraint::Kind(_)
        | Constraint::Label(_)
        | Constraint::RoleName(_)
        | Constraint::Sub(_)
        | Constraint::Owns(_)
        | Constraint::Relates(_)
        | Constraint::Plays(_)
        | Constraint::Value(_)
        | Constraint::Unsatisfiable(_) => false,
    });

    let any_vertex_empty = graph.vertices.annotations.iter().any(|(_, types)| types.is_empty());
    if any_vertex_empty && not_only_type_constraints {
        return Err(TypeInferenceError::DetectedUnsatisfiablePattern {});
    }
    Ok(())
}

fn check_expressions_were_compiled(
    graph: &FullTypeInferenceGraph<'_>,
    variable_registry: &VariableRegistry,
) -> Result<(), TypeInferenceError> {
    fn join_names(iter: impl Iterator<Item = ValueType>) -> String {
        iter.map(|v| v.category().name()).join(", ")
    }
    fn collect_uncompiled_recursive<'graph, 'conj>(
        graph: &'graph FullTypeInferenceGraph<'conj>,
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
        graph.disjunctions.iter().flatten().for_each(|g| collect_uncompiled_recursive(g, expressions))
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
            return Err(TypeInferenceError::VariableMultipleValueTypesInExpression {
                variable,
                value_types,
                source_span,
            });
        }
    }
    debug_assert!(false && leaf_uncompiled.is_some(), "Unreachable: we've already caught circular-dependencies");
    let assigned_variables =
        uncompiled.keys().map(|v| variable_registry.get_variable_name_or_unnamed(v.as_variable().unwrap())).join(", ");
    Err(TypeInferenceError::InternalUnresolvedExpressions { assigned_variables })
}

fn check_uniqueness_of_value_types(
    graph: &FullTypeInferenceGraph<'_>,
    variable_registry: &VariableRegistry,
) -> Result<(), TypeInferenceError> {
    if let Some((variable, value_types)) = find_multiply_typed_value_vertex(&graph.vertices) {
        let variable = variable_registry.get_variable_name_or_unnamed(variable).to_owned();
        let value_types = join_value_type_names(value_types.iter());
        let source_span = None; // Can't do much with a conjunction
        Err(TypeInferenceError::ValueVariableMultipleValueTypes { variable, value_types, source_span })
    } else {
        Ok(())
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
