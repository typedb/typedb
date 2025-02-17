/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::Arc,
};

use answer::{variable::Variable, Type};
use concept::type_::{constraint::Constraint as TypeConstraint, type_manager::TypeManager, OwnerAPI, TypeAPI};
use ir::{
    pattern::constraint::{Constraint, Has, Links},
    pipeline::block::Block,
};
use storage::snapshot::ReadableSnapshot;

use crate::{
    annotation::{
        type_annotations::{ConstraintTypeAnnotations, LeftRightAnnotations, LinksAnnotations, TypeAnnotations},
        TypeInferenceError,
    },
    executable::insert,
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
            Constraint::Has(has) => {
                validate_has_updatable(
                    snapshot,
                    type_manager,
                    has,
                    input_annotations_variables,
                    input_annotations_constraints,
                    insert_annotations.constraint_annotations_of(constraint.clone()).unwrap().as_left_right(),
                )?;
            }
            Constraint::Links(links) => {
                validate_links_updatable(
                    snapshot,
                    type_manager,
                    links,
                    input_annotations_variables,
                    input_annotations_constraints,
                    insert_annotations.constraint_annotations_of(constraint.clone()).unwrap().as_links(),
                )?;
            }

            Constraint::Isa(_)
            | Constraint::Is(_)
            | Constraint::Comparison(_)
            | Constraint::LinksDeduplication(_)
            | Constraint::ExpressionBinding(_)
            | Constraint::FunctionCallBinding(_)
            | Constraint::RoleName(_) => (),

            Constraint::Kind(_)
            | Constraint::Label(_)
            | Constraint::Sub(_)
            | Constraint::Owns(_)
            | Constraint::Relates(_)
            | Constraint::Plays(_)
            | Constraint::Value(_)
            | Constraint::Iid(_) => unreachable!("{constraint:?} in update should have been rejected by now"),
            Constraint::IndexedRelation(_) => unreachable!("Indexed relations can only appear after type inference"),
        }
    }
    Ok(())
}

fn validate_has_updatable(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    has: &Has<Variable>,
    input_annotations_variables: &BTreeMap<Variable, Arc<BTreeSet<answer::Type>>>,
    input_annotations_constraints: &HashMap<Constraint<Variable>, ConstraintTypeAnnotations>, // Future use
    left_right: &LeftRightAnnotations,
) -> Result<(), TypeInferenceError> {
    insert::type_check::validate_has_insertable(
        snapshot,
        type_manager,
        has,
        input_annotations_variables,
        input_annotations_constraints,
        left_right,
    )?;

    let input_owner_types = input_annotations_variables.get(&has.owner().as_variable().unwrap()).ok_or(
        TypeInferenceError::AnnotationsUnavailableForVariableInWrite {
            variable: has.owner().as_variable().unwrap(),
            source_span: has.source_span(),
        },
    )?;
    let input_attr_types = input_annotations_variables.get(&has.attribute().as_variable().unwrap()).ok_or(
        TypeInferenceError::AnnotationsUnavailableForVariableInWrite {
            variable: has.attribute().as_variable().unwrap(),
            source_span: has.source_span(),
        },
    )?;
    for left_type in input_owner_types.iter() {
        for right_type in input_attr_types.iter() {
            validate_has_cardinality(snapshot, type_manager, has, left_type, right_type)?;
        }
    }

    Ok(())
}

fn validate_links_updatable(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    links: &Links<Variable>,
    input_annotations_variables: &BTreeMap<Variable, Arc<BTreeSet<answer::Type>>>,
    input_annotations_constraints: &HashMap<Constraint<Variable>, ConstraintTypeAnnotations>, // Future use
    left_right_filtered: &LinksAnnotations,
) -> Result<(), TypeInferenceError> {
    insert::type_check::validate_links_insertable(
        snapshot,
        type_manager,
        links,
        input_annotations_variables,
        input_annotations_constraints,
        left_right_filtered,
    )?;

    let input_relation_types = input_annotations_variables.get(&links.relation().as_variable().unwrap()).ok_or(
        TypeInferenceError::AnnotationsUnavailableForVariableInWrite {
            variable: links.relation().as_variable().unwrap(),
            source_span: links.source_span(),
        },
    )?;
    let input_role_types = input_annotations_variables.get(&links.role_type().as_variable().unwrap()).ok_or(
        TypeInferenceError::AnnotationsUnavailableForVariableInWrite {
            variable: links.role_type().as_variable().unwrap(),
            source_span: links.source_span(),
        },
    )?;
    for left_type in input_relation_types.iter() {
        for right_type in input_role_types.iter() {
            validate_links_cardinality(snapshot, type_manager, links, left_type, right_type)?;
        }
    }

    Ok(())
}

fn validate_has_cardinality(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    has: &Has<Variable>,
    object_type: &Type,
    interface_type: &Type,
) -> Result<(), TypeInferenceError> {
    let object_type = object_type.as_object_type();
    let attribute_type = interface_type.as_attribute_type();
    let incorrect_cardinality = object_type
        .get_owned_attribute_type_constraints_cardinality(snapshot, type_manager, attribute_type)
        .map_err(|typedb_source| TypeInferenceError::ConceptRead { typedb_source })?
        .into_iter()
        .filter_map(|constraint| {
            constraint
                .source()
                .attribute()
                .eq(&attribute_type)
                .then(|| constraint.description().unwrap_cardinality().expect("Expected cardinality"))
        })
        .find(|cardinality| !cardinality.is_bounded_to_one());

    if incorrect_cardinality.is_some() {
        Err(TypeInferenceError::IllegalUpdatableTypesDueToCardinality {
            constraint_name: Constraint::Has(has.clone()).name().to_string(),
            left_type: object_type
                .get_label(snapshot, type_manager)
                .map_err(|err| TypeInferenceError::ConceptRead { typedb_source: err })?
                .scoped_name()
                .as_str()
                .to_string(),
            right_type: attribute_type
                .get_label(snapshot, type_manager)
                .map_err(|err| TypeInferenceError::ConceptRead { typedb_source: err })?
                .scoped_name()
                .as_str()
                .to_string(),
            source_span: has.source_span(),
        })
    } else {
        Ok(())
    }
}

fn validate_links_cardinality(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    links: &Links<Variable>,
    relation_type: &Type,
    role_type: &Type,
) -> Result<(), TypeInferenceError> {
    let relation_type = relation_type.as_relation_type();
    let role_type = role_type.as_role_type();
    let incorrect_cardinality = relation_type
        .get_related_role_type_constraints_cardinality(snapshot, type_manager, role_type)
        .map_err(|typedb_source| TypeInferenceError::ConceptRead { typedb_source })?
        .into_iter()
        .filter_map(|constraint| {
            constraint
                .source()
                .role()
                .eq(&role_type)
                .then(|| constraint.description().unwrap_cardinality().expect("Expected cardinality"))
        })
        .find(|cardinality| !cardinality.is_bounded_to_one());

    if incorrect_cardinality.is_some() {
        Err(TypeInferenceError::IllegalUpdatableTypesDueToCardinality {
            constraint_name: Constraint::Links(links.clone()).name().to_string(),
            left_type: relation_type
                .get_label(snapshot, type_manager)
                .map_err(|err| TypeInferenceError::ConceptRead { typedb_source: err })?
                .scoped_name()
                .as_str()
                .to_string(),
            right_type: role_type
                .get_label(snapshot, type_manager)
                .map_err(|err| TypeInferenceError::ConceptRead { typedb_source: err })?
                .scoped_name()
                .as_str()
                .to_string(),
            source_span: links.source_span(),
        })
    } else {
        Ok(())
    }
}
