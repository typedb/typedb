/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::BTreeMap;

use ir::{
    pattern::{conjunction::Conjunction, constraint::Constraint, nested_pattern::NestedPattern},
    pipeline::VariableRegistry,
};

use crate::{
    annotation::type_annotations::{BlockAnnotations, ConstraintTypeAnnotations, LeftRightAnnotations},
    transformation::StaticOptimiserError,
};

pub fn make_constraint_variables_unique(
    conjunction: &mut Conjunction,
    variable_registry: &mut VariableRegistry,
    block_annotations: &mut BlockAnnotations,
) -> Result<(), StaticOptimiserError> {
    let (new_checks, constraint_mapping, variable_mapping) = conjunction
        .make_constraint_variables_unique(variable_registry)
        .map_err(|typedb_source| StaticOptimiserError::Representation { typedb_source })?;
    let annotations = block_annotations.type_annotations_mut_of(conjunction).expect("Expected annotated conjunction");

    for check in new_checks {
        match check {
            Constraint::Comparison(cmp) => {
                let (new_vertex, old_vertex) = if variable_mapping.contains_key(&cmp.lhs().as_variable().unwrap()) {
                    (cmp.lhs(), cmp.rhs())
                } else {
                    debug_assert!(variable_mapping.contains_key(&cmp.rhs().as_variable().unwrap()));
                    (cmp.rhs(), cmp.lhs())
                };
                let vertex_annotations = annotations.value_type_annotations_of(old_vertex).unwrap().clone();
                annotations
                    .value_type_annotations_mut()
                    .expect("ValueTypeAnnotations should be available by now")
                    .insert(new_vertex.clone(), vertex_annotations);
            }
            Constraint::Is(is) => {
                let (new_vertex, old_vertex) = if variable_mapping.contains_key(&is.lhs().as_variable().unwrap()) {
                    (is.lhs(), is.rhs())
                } else {
                    debug_assert!(variable_mapping.contains_key(&is.rhs().as_variable().unwrap()));
                    (is.rhs(), is.lhs())
                };
                let vertex_annotations = annotations.vertex_annotations_of(old_vertex).unwrap().clone();
                let lr_annotations: BTreeMap<_, _> = vertex_annotations.iter().map(|t| (*t, vec![*t])).collect();
                let rl_annotations = lr_annotations.clone();
                let new_is_annotations =
                    ConstraintTypeAnnotations::LeftRight(LeftRightAnnotations::new(lr_annotations, rl_annotations));
                annotations.vertex_annotations_mut().insert(new_vertex.clone(), vertex_annotations);
                annotations.constraint_annotations_mut().insert(Constraint::Is(is), new_is_annotations);
            }
            _ => {
                unreachable!("Did not expect any other constraint");
            }
        }
    }
    for (new_constraint, old_constraint) in constraint_mapping {
        let old_annos =
            annotations.constraint_annotations_of(old_constraint).cloned().expect("Expected old constraint");
        annotations.constraint_annotations_mut().insert(new_constraint, old_annos);
    }

    // And recurse
    for nested in conjunction.nested_patterns_mut() {
        match nested {
            NestedPattern::Disjunction(disjunction) => {
                for conj in disjunction.conjunctions_mut() {
                    make_constraint_variables_unique(conj, variable_registry, block_annotations)?;
                }
            }
            NestedPattern::Negation(negation) => {
                make_constraint_variables_unique(negation.conjunction_mut(), variable_registry, block_annotations)?;
            }
            NestedPattern::Optional(optional) => {
                make_constraint_variables_unique(optional.conjunction_mut(), variable_registry, block_annotations)?;
            }
        }
    }

    Ok(())
}
