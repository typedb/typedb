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
    let (new_checks, constraint_mapping) = conjunction
        .constraints_mut()
        .make_variables_unique(variable_registry)
        .map_err(|typedb_source| StaticOptimiserError::Representation { typedb_source })?;
    let annotations = block_annotations.type_annotations_mut_of(conjunction).expect("Expected annotated conjunction");

    for check in new_checks {
        match check {
            Constraint::Comparison(cmp) => {
                conjunction.register_variable_copy(cmp.lhs().as_variable().unwrap(), cmp.rhs().as_variable().unwrap());
                let vertex_annotations = annotations.value_type_annotations_of(&cmp.lhs()).unwrap().clone();
                annotations
                    .value_type_annotations_mut()
                    .expect("ValueTypeAnnotations should be available by now")
                    .insert(cmp.rhs().clone(), vertex_annotations);
            }
            Constraint::Is(is) => {
                conjunction.register_variable_copy(is.lhs().as_variable().unwrap(), is.rhs().as_variable().unwrap());
                let vertex_annotations = annotations.vertex_annotations_of(&is.lhs()).unwrap().clone();
                let lr_annotations: BTreeMap<_, _> = vertex_annotations.iter().map(|t| (*t, vec![*t])).collect();
                let rl_annotations = lr_annotations.clone();
                let new_is_annotations =
                    ConstraintTypeAnnotations::LeftRight(LeftRightAnnotations::new(lr_annotations, rl_annotations));
                annotations.vertex_annotations_mut().insert(is.rhs().clone(), vertex_annotations);
                annotations.constraint_annotations_mut().insert(Constraint::Is(is), new_is_annotations);
            }
            _ => {
                unreachable!("Did not expect any other constraint");
            }
        }
    }
    for (old_constraint, new_constraint) in constraint_mapping {
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
