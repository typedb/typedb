/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::Arc,
};
use itertools::Itertools;
use answer::Type;

use answer::variable::Variable;
use concept::type_::type_manager::TypeManager;
use ir::{
    pattern::constraint::{Constraint, Has, Links},
    pipeline::block::Block,
};
use storage::snapshot::ReadableSnapshot;

use crate::annotation::{
    type_annotations::{ConstraintTypeAnnotations, LeftRightAnnotations, LinksAnnotations, TypeAnnotations},
    TypeInferenceError,
};

pub fn check_type_combinations_for_write(
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
                validate_has_type_combinations_for_write(
                    snapshot,
                    type_manager,
                    has,
                    input_annotations_variables,
                    input_annotations_constraints,
                    insert_annotations.constraint_annotations_of(constraint.clone()).unwrap().as_left_right(),
                )?;
            }
            Constraint::Links(links) => {
                validate_links_type_combinations_for_write(
                    snapshot,
                    type_manager,
                    links,
                    input_annotations_variables,
                    input_annotations_constraints,
                    insert_annotations.constraint_annotations_of(constraint.clone()).unwrap().as_links(),
                )?;
            }

            Constraint::Isa(_)
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
            | Constraint::Value(_)
            | Constraint::LinksDeduplication(_) => (),
            Constraint::Iid(_) => unreachable!("iid in insert should have been rejected by now"),
            Constraint::IndexedRelation(_) => unreachable!("Indexed relations can only appear after type inference"),
            Constraint::Unsatisfiable(_) => {
                unreachable!("Optimised away can only appear after type inference")
            }
        }
    }
    Ok(())
}

pub(crate) fn validate_has_type_combinations_for_write(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    insert_has: &Has<Variable>,
    input_annotations_variables: &BTreeMap<Variable, Arc<BTreeSet<answer::Type>>>,
    input_annotations_constraints: &HashMap<Constraint<Variable>, ConstraintTypeAnnotations>, // Future use
    left_right: &LeftRightAnnotations,
) -> Result<(), TypeInferenceError> {
    // TODO: Improve. This is extremely coarse and likely to rule out many valid combinations
    // Esp when doing queries using type variables.
    let mut applicable_constraint_annotations = input_annotations_constraints.iter()
        .filter_map(|(constraint,_)| {
            match constraint {
                Constraint::Has(match_has) => Some((match_has.owner(), match_has.attribute(), constraint)),
                Constraint::Owns(match_owns) => Some((match_owns.owner(), match_owns.attribute(), constraint)),
                _ => None
            }
        })
        .filter(|(match_owner, match_attribute, _)| match_owner == &insert_has.owner() && match_attribute == &insert_has.attribute())
        .map(|(_, _, constraint)| input_annotations_constraints.get(constraint).unwrap().as_left_right().left_to_right());
    let mut match_pairs = if let Some(match_type_pairs) = may_intersect_all(applicable_constraint_annotations) {
        match_type_pairs
    } else {
        let input_owner_types = input_annotations_variables.get(&insert_has.owner().as_variable().unwrap()).ok_or(
            TypeInferenceError::AnnotationsUnavailableForVariableInWrite {
                variable: insert_has.owner().as_variable().unwrap(),
                source_span: insert_has.source_span(),
            },
        )?;
        let input_attr_types = input_annotations_variables.get(&insert_has.attribute().as_variable().unwrap()).ok_or(
            TypeInferenceError::AnnotationsUnavailableForVariableInWrite {
                variable: insert_has.attribute().as_variable().unwrap(),
                source_span: insert_has.source_span(),
            },
        )?;
        input_owner_types.iter().cloned().cartesian_product(input_attr_types.iter().cloned()).collect()
    };

    let mut invalid_iter = match_pairs.iter().filter(|(left_type, right_type)| {
        !left_right
            .left_to_right()
            .get(left_type)
            .map(|valid_right_types| valid_right_types.contains(right_type))
            .unwrap_or(false)
    });

    if let Some((left_type, right_type)) = invalid_iter.next() {
        Err(TypeInferenceError::IllegalTypeCombinationForWrite {
            constraint_name: Constraint::Has(insert_has.clone()).name().to_string(),
            left_type: left_type
                .get_label(snapshot, type_manager)
                .map_err(|err| TypeInferenceError::ConceptRead { typedb_source: err })?
                .scoped_name()
                .as_str()
                .to_string(),
            right_type: right_type
                .get_label(snapshot, type_manager)
                .map_err(|err| TypeInferenceError::ConceptRead { typedb_source: err })?
                .scoped_name()
                .as_str()
                .to_string(),
            source_span: insert_has.source_span(),
        })?;
    }
    Ok(())
}

pub(crate) fn validate_links_type_combinations_for_write(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    links: &Links<Variable>,
    input_annotations_variables: &BTreeMap<Variable, Arc<BTreeSet<answer::Type>>>,
    input_annotations_constraints: &HashMap<Constraint<Variable>, ConstraintTypeAnnotations>, // Future use
    left_right_filtered: &LinksAnnotations,
) -> Result<(), TypeInferenceError> {
    // TODO: Should we check uniqueness of inferred role-types here instead of at compilation?
    // TODO: Improve. This is extremely coarse and likely to rule out many valid combinations
    // Esp when doing queries using type variables.

    let input_relation_types = input_annotations_variables.get(&links.relation().as_variable().unwrap()).ok_or(
        TypeInferenceError::AnnotationsUnavailableForVariableInWrite {
            variable: links.relation().as_variable().unwrap(),
            source_span: links.source_span(),
        },
    )?;
    let input_player_types = input_annotations_variables.get(&links.player().as_variable().unwrap()).ok_or(
        TypeInferenceError::AnnotationsUnavailableForVariableInWrite {
            variable: links.player().as_variable().unwrap(),
            source_span: links.source_span(),
        },
    )?;
    let input_role_types = input_annotations_variables.get(&links.role_type().as_variable().unwrap()).ok_or(
        TypeInferenceError::AnnotationsUnavailableForVariableInWrite {
            variable: links.role_type().as_variable().unwrap(),
            source_span: links.source_span(),
        },
    )?;

    let invalid_relation_role_iter = input_relation_types.iter().flat_map(|relation_type| {
        input_role_types
            .iter()
            .filter(|role_type| {
                !left_right_filtered
                    .relation_to_role()
                    .get(relation_type)
                    .map(|valid_role_types| valid_role_types.contains(role_type))
                    .unwrap_or(false)
            })
            .map(|role_type| (*relation_type, *role_type))
    });
    let invalid_player_role_iter = input_player_types.iter().flat_map(|player_type| {
        input_role_types
            .iter()
            .filter(|role_type| {
                !left_right_filtered
                    .player_to_role()
                    .get(player_type)
                    .map(|valid_role_types| valid_role_types.contains(role_type))
                    .unwrap_or(false)
            })
            .map(|role_type| (*player_type, *role_type))
    });
    if let Some((left_type, right_type)) = invalid_relation_role_iter.chain(invalid_player_role_iter).next() {
        Err(TypeInferenceError::IllegalTypeCombinationForWrite {
            constraint_name: Constraint::Links(links.clone()).name().to_string(),
            left_type: left_type
                .get_label(snapshot, type_manager)
                .map_err(|err| TypeInferenceError::ConceptRead { typedb_source: err })?
                .scoped_name()
                .as_str()
                .to_string(),
            right_type: right_type
                .get_label(snapshot, type_manager)
                .map_err(|err| TypeInferenceError::ConceptRead { typedb_source: err })?
                .scoped_name()
                .as_str()
                .to_string(),
            source_span: links.source_span(),
        })?;
    }
    Ok(())
}

fn may_intersect_all<T: ContainsAndIterOnType>(
    mut constraint_annotations: impl Iterator<Item=Arc<BTreeMap<Type, T>>>,
) -> Option<BTreeSet<(Type, Type)>> {
    if let Some(first) = constraint_annotations.next() {
        let mut combinations = first.iter().flat_map(|(left, right_set)| {
            right_set._iter().map(|right| (left.clone(), right.clone()))
        }).collect::<BTreeSet<_>>();
        while let Some(annotations) = constraint_annotations.next() {
            combinations.retain(|(left,right)| annotations.get(left).map(|right_set| right_set._contains(right)).unwrap_or(false));
        }
        Some(combinations)
    } else {
        None
    }
}


trait ContainsAndIterOnType {
    fn _contains(&self, item: &answer::Type) -> bool;
    fn _iter(&self) -> impl Iterator<Item=&answer::Type> + '_;
}

impl ContainsAndIterOnType for Vec<Type> {
    fn _contains(&self, item: &Type) -> bool {
        self.contains(item)
    }

    fn _iter(&self) -> impl Iterator<Item=&Type> + '_ {
        self.iter()
    }
}

impl ContainsAndIterOnType for BTreeSet<Type> {
    fn _contains(&self, item: &Type) -> bool {
        self.contains(item)
    }

    fn _iter(&self) -> impl Iterator<Item=&Type> + '_ {
        self.iter()
    }
}
