/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::Arc,
};

use answer::variable::Variable;
use concept::type_::type_manager::TypeManager;
use ir::{
    pattern::constraint::{Constraint, Has, Links},
    pipeline::block::Block,
};
use storage::snapshot::ReadableSnapshot;

use crate::annotation::{
    type_annotations::{
        ConstraintTypeAnnotations, LeftRightAnnotations, LeftRightFilteredAnnotations, TypeAnnotations,
    },
    TypeInferenceError,
};

pub fn check_annotations(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    block: &Block,
    input_annotations_variables: &BTreeMap<Variable, Arc<BTreeSet<answer::Type>>>,
    input_annotations_constraints: &HashMap<Constraint<Variable>, ConstraintTypeAnnotations>,
    insert_annotations: &TypeAnnotations,
) -> Result<(), TypeInferenceError> {
    for constraint in block.conjunction().constraints() {
        match constraint {
            Constraint::Isa(_) => (), // Nothing to do
            Constraint::Has(has) => {
                validate_has_insertable(
                    snapshot,
                    type_manager,
                    has,
                    input_annotations_variables,
                    input_annotations_constraints,
                    insert_annotations.constraint_annotations_of(constraint.clone()).unwrap().as_left_right(),
                )?;
            }
            Constraint::Links(links) => {
                validate_links_insertable(
                    snapshot,
                    type_manager,
                    links,
                    input_annotations_variables,
                    input_annotations_constraints,
                    insert_annotations.constraint_annotations_of(constraint.clone()).unwrap().as_left_right_filtered(),
                )?;
            }
            | Constraint::Kind(_)
            | Constraint::Label(_)
            | Constraint::RoleName(_)
            | Constraint::Sub(_)
            | Constraint::ExpressionBinding(_)
            | Constraint::FunctionCallBinding(_)
            | Constraint::Is(_)
            | Constraint::Comparison(_)
            | Constraint::Owns(_)
            | Constraint::Relates(_)
            | Constraint::Plays(_)
            | Constraint::As(_)
            | Constraint::Value(_) => (),
        }
    }
    Ok(())
}

fn validate_has_insertable(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    has: &Has<Variable>,
    input_annotations_variables: &BTreeMap<Variable, Arc<BTreeSet<answer::Type>>>,
    input_annotations_constraints: &HashMap<Constraint<Variable>, ConstraintTypeAnnotations>, // Future use
    left_right: &LeftRightAnnotations,
) -> Result<(), TypeInferenceError> {
    // TODO: Improve. This is extremely coarse and likely to rule out many valid combinations
    // Esp when doing queries using type variables.
    let input_owner_types = input_annotations_variables.get(&has.owner().as_variable().unwrap()).unwrap();
    let input_attr_types = input_annotations_variables.get(&has.attribute().as_variable().unwrap()).unwrap();

    let mut invalid_iter = input_owner_types.iter().flat_map(|left_type| {
        input_attr_types
            .iter()
            .filter(|right_type| {
                !left_right
                    .left_to_right()
                    .get(left_type)
                    .map(|valid_right_types| valid_right_types.contains(right_type))
                    .unwrap_or(false)
            })
            .map(|right_type| (left_type.clone(), right_type.clone()))
    });
    if let Some((left_type, right_type)) = invalid_iter.find(|_| true) {
        Err(TypeInferenceError::IllegalInsertTypes {
            constraint_name: Constraint::Has(has.clone()).name().to_string(),
            left_type: left_type
                .get_label(snapshot, type_manager)
                .map_err(|err| TypeInferenceError::ConceptRead { source: err })?
                .scoped_name()
                .as_str()
                .to_string(),
            right_type: right_type
                .get_label(snapshot, type_manager)
                .map_err(|err| TypeInferenceError::ConceptRead { source: err })?
                .scoped_name()
                .as_str()
                .to_string(),
        })?;
    }
    Ok(())
}

fn validate_links_insertable(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    links: &Links<Variable>,
    input_annotations_variables: &BTreeMap<Variable, Arc<BTreeSet<answer::Type>>>,
    input_annotations_constraints: &HashMap<Constraint<Variable>, ConstraintTypeAnnotations>, // Future use
    left_right_filtered: &LeftRightFilteredAnnotations,
) -> Result<(), TypeInferenceError> {
    // TODO: Improve. This is extremely coarse and likely to rule out many valid combinations
    // Esp when doing queries using type variables.
    let input_relation_types = input_annotations_variables.get(&links.relation().as_variable().unwrap()).unwrap();
    let input_player_types = input_annotations_variables.get(&links.player().as_variable().unwrap()).unwrap();
    let input_role_types = input_annotations_variables.get(&links.role_type().as_variable().unwrap()).unwrap();

    let invalid_relation_role_iter = input_relation_types.iter().flat_map(|relation_type| {
        input_role_types
            .iter()
            .filter(|role_type| {
                !left_right_filtered
                    .filters_on_left
                    .get(relation_type)
                    .map(|valid_role_types| valid_role_types.contains(role_type))
                    .unwrap_or(false)
            })
            .map(|role_type| (relation_type.clone(), role_type.clone()))
    });
    let invalid_player_role_iter = input_player_types.iter().flat_map(|player_type| {
        input_role_types
            .iter()
            .filter(|role_type| {
                !left_right_filtered
                    .filters_on_right
                    .get(player_type)
                    .map(|valid_role_types| valid_role_types.contains(role_type))
                    .unwrap_or(false)
            })
            .map(|role_type| (player_type.clone(), role_type.clone()))
    });
    if let Some((left_type, right_type)) = invalid_relation_role_iter.chain(invalid_player_role_iter).find(|_| true) {
        Err(TypeInferenceError::IllegalInsertTypes {
            constraint_name: Constraint::Links(links.clone()).name().to_string(),
            left_type: left_type
                .get_label(snapshot, type_manager)
                .map_err(|err| TypeInferenceError::ConceptRead { source: err })?
                .scoped_name()
                .as_str()
                .to_string(),
            right_type: right_type
                .get_label(snapshot, type_manager)
                .map_err(|err| TypeInferenceError::ConceptRead { source: err })?
                .scoped_name()
                .as_str()
                .to_string(),
        })?;
    }
    Ok(())
}
