/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::{BTreeMap, BTreeSet};

use ir::{
    pattern::{Vertex, conjunction::Conjunction, constraint::Constraint, nested_pattern::NestedPattern},
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
    let (new_is_constraints, var_mapping, constraint_mapping) = conjunction
        .constraints_mut()
        .make_variables_unique(variable_registry)
        .map_err(|typedb_source| StaticOptimiserError::Representation { typedb_source })?;
    let annotations = block_annotations.type_annotations_mut_of(conjunction).expect("Expected annotated conjunction");

    conjunction.register_copies_of_variables(var_mapping.iter().copied());
    for (old_var, new_var) in var_mapping {
        if let Some(old_annotations) = annotations.vertex_annotations_of(&Vertex::Variable(old_var)).cloned() {
            annotations.vertex_annotations_mut().insert(Vertex::Variable(new_var), old_annotations);
        } else if let Some(old_annotations) = annotations.value_type_annotations_of(&Vertex::Variable(old_var)).cloned()
        {
            annotations
                .value_type_annotations_mut()
                .expect("Should be present at this point")
                .insert(Vertex::Variable(new_var), old_annotations);
        }
    }

    for new_is in new_is_constraints {
        let annos = if let Some(annos) = annotations.vertex_annotations_of(new_is.lhs()) {
            annos
        } else if let Some(annos) = annotations.vertex_annotations_of(new_is.rhs()) {
            annos
        } else {
            unreachable!("Expected atleast one vertex")
        };
        let lr_annotations: BTreeMap<_, _> = annos.iter().map(|t| (*t, vec![*t])).collect();
        let rl_annotations = lr_annotations.clone();
        let new_is_annotations =
            ConstraintTypeAnnotations::LeftRight(LeftRightAnnotations::new(lr_annotations, rl_annotations));
        annotations.constraint_annotations_mut().insert(Constraint::Is(new_is), new_is_annotations);
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
