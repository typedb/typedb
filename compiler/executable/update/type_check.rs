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
use concept::type_::{constraint::Constraint as TypeConstraint, OwnerAPI};
use ir::{
    pattern::constraint::{Constraint, Has, Links},
    pipeline::block::Block,
};
use storage::snapshot::ReadableSnapshot;

use crate::annotation::{type_annotations::{BlockAnnotations, ConstraintTypeAnnotations, LeftRightAnnotations, LinksAnnotations}, write_type_check::{validate_has_type_combinations_for_write, validate_links_type_combinations_for_write}, TypeInferenceError};
use crate::annotation::utils::{NameForError, PipelineAnnotationContext};

pub fn check_annotations(
    ctx: &mut PipelineAnnotationContext<'_, impl ReadableSnapshot>,
    block: &Block,
    input_annotations_variables: &BTreeMap<Variable, Arc<BTreeSet<answer::Type>>>,
    input_annotations_constraints: &HashMap<Constraint<Variable>, ConstraintTypeAnnotations>,
    insert_annotations: &BlockAnnotations,
) -> Result<(), TypeInferenceError> {
    for constraint in block.conjunction().constraints() {
        match constraint {
            Constraint::Has(has) => {
                validate_has_updatable(
                    ctx,
                    has,
                    input_annotations_variables,
                    input_annotations_constraints,
                    insert_annotations
                        .type_annotations_of(block.conjunction())
                        .unwrap()
                        .constraint_annotations_of(constraint.clone())
                        .unwrap()
                        .as_left_right(),
                )?;
            }
            Constraint::Links(links) => {
                validate_links_updatable(
                    ctx,
                    links,
                    input_annotations_variables,
                    input_annotations_constraints,
                    insert_annotations
                        .type_annotations_of(block.conjunction())
                        .unwrap()
                        .constraint_annotations_of(constraint.clone())
                        .unwrap()
                        .as_links(),
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
            | Constraint::Iid(_) => unreachable!("{constraint} in update should have been rejected by now"),
            Constraint::IndexedRelation(_) => unreachable!("Indexed relations can only appear after type inference"),
            Constraint::Unsatisfiable(_) => {
                unreachable!("Unsatisfiable can only appear after type inference")
            }
        }
    }
    Ok(())
}

fn validate_has_updatable(
    ctx: &mut PipelineAnnotationContext<'_, impl ReadableSnapshot>,
    has: &Has<Variable>,
    input_annotations_variables: &BTreeMap<Variable, Arc<BTreeSet<answer::Type>>>,
    input_annotations_constraints: &HashMap<Constraint<Variable>, ConstraintTypeAnnotations>, // Future use
    left_right: &LeftRightAnnotations,
) -> Result<(), TypeInferenceError> {
    validate_has_type_combinations_for_write(
        ctx,
        has,
        input_annotations_variables,
        input_annotations_constraints,
        left_right,
    )?;

    let input_owner_types = input_annotations_variables.get(&has.owner().as_variable().unwrap()).ok_or(
        TypeInferenceError::AnnotationsUnavailableForVariableInWrite {
            variable: ctx.variable_registry.get_variable_name_or_unnamed(has.owner().as_variable().unwrap()).to_owned(),
            source_span: has.source_span(),
        },
    )?;
    let input_attr_types = input_annotations_variables.get(&has.attribute().as_variable().unwrap()).ok_or(
        TypeInferenceError::AnnotationsUnavailableForVariableInWrite {
            variable: ctx.variable_registry.get_variable_name_or_unnamed(has.attribute().as_variable().unwrap()).to_owned(),
            source_span: has.source_span(),
        },
    )?;
    for left_type in input_owner_types.iter() {
        for right_type in input_attr_types.iter() {
            validate_has_cardinality(ctx, has, left_type, right_type)?;
        }
    }

    Ok(())
}

fn validate_links_updatable(
    ctx: &mut PipelineAnnotationContext<'_, impl ReadableSnapshot>,
    links: &Links<Variable>,
    input_annotations_variables: &BTreeMap<Variable, Arc<BTreeSet<answer::Type>>>,
    input_annotations_constraints: &HashMap<Constraint<Variable>, ConstraintTypeAnnotations>, // Future use
    left_right_filtered: &LinksAnnotations,
) -> Result<(), TypeInferenceError> {
    validate_links_type_combinations_for_write(
        ctx,
        links,
        input_annotations_variables,
        input_annotations_constraints,
        left_right_filtered,
    )?;

    let input_relation_types = input_annotations_variables.get(&links.relation().as_variable().unwrap()).ok_or(
        TypeInferenceError::AnnotationsUnavailableForVariableInWrite {
            variable: ctx.variable_registry
                .get_variable_name_or_unnamed(links.relation().as_variable().unwrap())
                .to_owned(),
            source_span: links.source_span(),
        },
    )?;
    let input_role_types = input_annotations_variables.get(&links.role_type().as_variable().unwrap()).ok_or(
        TypeInferenceError::AnnotationsUnavailableForVariableInWrite {
            variable: ctx.variable_registry
                .get_variable_name_or_unnamed(links.role_type().as_variable().unwrap())
                .to_owned(),
            source_span: links.source_span(),
        },
    )?;
    for left_type in input_relation_types.iter() {
        for right_type in input_role_types.iter() {
            validate_links_cardinality(ctx, links, left_type, right_type)?;
        }
    }

    Ok(())
}

fn validate_has_cardinality(
    ctx: &mut PipelineAnnotationContext<'_, impl ReadableSnapshot>,
    has: &Has<Variable>,
    object_type: &Type,
    interface_type: &Type,
) -> Result<(), TypeInferenceError> {
    let object_type = object_type.as_object_type();
    let attribute_type = interface_type.as_attribute_type();
    let incorrect_cardinality = object_type
        .get_owned_attribute_type_constraints_cardinality(ctx.snapshot, ctx.type_manager, attribute_type)
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
            left_type: object_type.name_for_error(ctx)?,
            right_type: attribute_type.name_for_error(ctx)?,
            source_span: has.source_span(),
        })
    } else {
        Ok(())
    }
}

fn validate_links_cardinality(
    ctx: &mut PipelineAnnotationContext<'_, impl ReadableSnapshot>,
    links: &Links<Variable>,
    relation_type: &Type,
    role_type: &Type,
) -> Result<(), TypeInferenceError> {
    let relation_type = relation_type.as_relation_type();
    let role_type = role_type.as_role_type();
    let incorrect_cardinality = relation_type
        .get_related_role_type_constraints_cardinality(ctx.snapshot, ctx.type_manager, role_type)
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
            left_type: relation_type.name_for_error(&ctx)?,
            right_type: role_type.name_for_error(&ctx)?,
            source_span: links.source_span(),
        })
    } else {
        Ok(())
    }
}
