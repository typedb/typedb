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
use concept::type_::type_manager::TypeManager;
use ir::{
    pattern::{
        constraint::{Constraint, Has, Links},
        Vertex,
    },
    pipeline::block::Block,
};
use itertools::Itertools;
use storage::snapshot::ReadableSnapshot;
use typeql::common::Span;
use ir::pipeline::VariableRegistry;

use crate::annotation::{
    type_annotations::{ConstraintTypeAnnotations, LeftRightAnnotations, LinksAnnotations, TypeAnnotations},
    TypeInferenceError,
};

pub fn check_type_combinations_for_write(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    variable_registry: &VariableRegistry,
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
                    variable_registry,
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
                    variable_registry,
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
    variable_registry: &VariableRegistry,
    insert_has: &Has<Variable>,
    input_annotations_variables: &BTreeMap<Variable, Arc<BTreeSet<answer::Type>>>,
    input_annotations_constraints: &HashMap<Constraint<Variable>, ConstraintTypeAnnotations>, // Future use
    valid_insert_types: &LeftRightAnnotations,
) -> Result<(), TypeInferenceError> {
    // TODO: Improve. This is still coarse and likely to rule out some valid combinations
    // Esp when doing queries using type variables. See
    let applicable_constraint_annotations = input_annotations_constraints
        .iter()
        .filter_map(|(constraint, annotations)| match constraint {
            Constraint::Has(match_has) => Some((match_has.owner(), match_has.attribute(), annotations)),
            Constraint::Owns(match_owns) => Some((match_owns.owner(), match_owns.attribute(), annotations)),
            _ => None,
        })
        .filter(|(match_owner, match_attribute, _)| {
            match_owner == &insert_has.owner() && match_attribute == &insert_has.attribute()
        })
        .map(|(_, _, annotations)| annotations.as_left_right().left_to_right());
    let match_pairs = match may_intersect_all(applicable_constraint_annotations) {
        Some(pairs) => pairs,
        None => pairs_from_vertex_annotations(
            variable_registry,
            input_annotations_variables,
            insert_has.owner(),
            insert_has.attribute(),
            insert_has.source_span(),
        )?,
    };
    check_insert_types_against_match_types(
        valid_insert_types.left_to_right(),
        match_pairs,
        insert_has,
        snapshot,
        type_manager,
        insert_has.source_span(),
    )
}

pub(crate) fn validate_links_type_combinations_for_write(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    variable_registry: &VariableRegistry,
    insert_links: &Links<Variable>,
    input_annotations_variables: &BTreeMap<Variable, Arc<BTreeSet<answer::Type>>>,
    input_annotations_constraints: &HashMap<Constraint<Variable>, ConstraintTypeAnnotations>, // Future use
    valid_insert_types: &LinksAnnotations,
) -> Result<(), TypeInferenceError> {
    // TODO: Should we check uniqueness of inferred role-types here instead of at compilation?
    // TODO: Improve. This is extremely coarse and likely to rule out many valid combinations
    // Esp when doing queries using type variables.
    {
        // First do it for relation -> role
        let link_annotations = input_annotations_constraints
            .iter()
            .filter_map(|(constraint, annotations)| constraint.as_links().map(|links| (links, annotations)))
            .filter(|(match_links, _)| {
                match_links.relation() == insert_links.relation() && match_links.role_type() == insert_links.role_type()
            })
            .map(|(_, annotations)| annotations.as_links().relation_to_role.clone());
        let relates_annotations = input_annotations_constraints
            .iter()
            .filter_map(|(constraint, annotations)| constraint.as_relates().map(|relates| (relates, annotations)))
            .filter(|(match_relates, _)| {
                match_relates.relation() == insert_links.relation()
                    && match_relates.role_type() == insert_links.role_type()
            })
            .map(|(_, annotations)| annotations.as_left_right().left_to_right());
        let match_pairs = match (may_intersect_all(link_annotations), may_intersect_all(relates_annotations)) {
            (None, None) => pairs_from_vertex_annotations(
                variable_registry,
                input_annotations_variables,
                insert_links.relation(),
                insert_links.role_type(),
                insert_links.source_span(),
            )?,
            (None, Some(types)) | (Some(types), None) => types,
            (Some(a), Some(b)) => a.intersection(&b).cloned().collect(),
        };
        check_insert_types_against_match_types(
            valid_insert_types.relation_to_role(),
            match_pairs,
            insert_links,
            snapshot,
            type_manager,
            insert_links.source_span(),
        )?;
    }

    {
        // Now do it for player -> role
        let link_annotations = input_annotations_constraints
            .iter()
            .filter_map(|(constraint, annotations)| constraint.as_links().map(|links| (links, annotations)))
            .filter(|(match_links, _)| {
                match_links.player() == insert_links.player() && match_links.role_type() == insert_links.role_type()
            })
            .map(|(_, annotations)| annotations.as_links().player_to_role.clone());
        let plays_annotations = input_annotations_constraints
            .iter()
            .filter_map(|(constraint, annotations)| constraint.as_plays().map(|plays| (plays, annotations)))
            .filter(|(match_plays, _)| {
                match_plays.player() == insert_links.player() && match_plays.role_type() == insert_links.role_type()
            })
            .map(|(_, annotations)| annotations.as_left_right().left_to_right());
        let match_pairs = match (may_intersect_all(link_annotations), may_intersect_all(plays_annotations)) {
            (None, None) => pairs_from_vertex_annotations(
                variable_registry,
                input_annotations_variables,
                insert_links.player(),
                insert_links.role_type(),
                insert_links.source_span(),
            )?,
            (None, Some(types)) | (Some(types), None) => types,
            (Some(a), Some(b)) => a.intersection(&b).cloned().collect(),
        };
        check_insert_types_against_match_types(
            valid_insert_types.player_to_role(),
            match_pairs,
            insert_links,
            snapshot,
            type_manager,
            insert_links.source_span(),
        )?;
    }
    Ok(())
}

fn may_intersect_all<T: ContainsAndIterOnType>(
    mut constraint_annotations: impl Iterator<Item = Arc<BTreeMap<Type, T>>>,
) -> Option<BTreeSet<(Type, Type)>> {
    if let Some(first) = constraint_annotations.next() {
        let mut combinations = first
            .iter()
            .flat_map(|(left, right_set)| right_set._iter().map(|right| (left.clone(), right.clone())))
            .collect::<BTreeSet<_>>();
        while let Some(annotations) = constraint_annotations.next() {
            combinations.retain(|(left, right)| {
                annotations.get(left).map(|right_set| right_set._contains(right)).unwrap_or(false)
            });
        }
        Some(combinations)
    } else {
        None
    }
}

fn pairs_from_vertex_annotations(
    variable_registry: &VariableRegistry,
    input_annotations_variables: &BTreeMap<Variable, Arc<BTreeSet<Type>>>,
    left: &Vertex<Variable>,
    right: &Vertex<Variable>,
    source_span: Option<Span>,
) -> Result<BTreeSet<(Type, Type)>, TypeInferenceError> {
    let left_types = input_annotations_variables.get(left.as_variable().as_ref().unwrap()).ok_or(
        TypeInferenceError::AnnotationsUnavailableForVariableInWrite {
            variable: variable_registry.get_variable_name_or_unnamed(right.as_variable().unwrap()).to_owned(),
            source_span,
        },
    )?;
    let right_types = input_annotations_variables.get(right.as_variable().as_ref().unwrap()).ok_or(
        TypeInferenceError::AnnotationsUnavailableForVariableInWrite {
            variable: variable_registry.get_variable_name_or_unnamed(right.as_variable().unwrap()).to_owned(),
            source_span,
        },
    )?;
    Ok(left_types.iter().cloned().cartesian_product(right_types.iter().cloned()).collect())
}

fn check_insert_types_against_match_types<T: ContainsAndIterOnType>(
    valid_insert_pairs: Arc<BTreeMap<Type, T>>,
    match_pairs: BTreeSet<(Type, Type)>,
    constraint: &(impl Into<Constraint<Variable>> + Clone),
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    source_span: Option<Span>,
) -> Result<(), TypeInferenceError> {
    let mut invalid_iter = match_pairs.iter().filter(|(left_type, right_type)| {
        !valid_insert_pairs
            .get(left_type)
            .map(|valid_right_types| valid_right_types._contains(right_type))
            .unwrap_or(false)
    });
    if let Some((left_type, right_type)) = invalid_iter.next() {
        Err(TypeInferenceError::IllegalTypeCombinationForWrite {
            constraint_name: Into::<Constraint<Variable>>::into(constraint.clone()).name().to_string(),
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
            source_span,
        })
    } else {
        Ok(())
    }
}

trait ContainsAndIterOnType {
    fn _contains(&self, item: &answer::Type) -> bool;
    fn _iter(&self) -> impl Iterator<Item = &answer::Type> + '_;
}

impl ContainsAndIterOnType for Vec<Type> {
    fn _contains(&self, item: &Type) -> bool {
        self.contains(item)
    }

    fn _iter(&self) -> impl Iterator<Item = &Type> + '_ {
        self.iter()
    }
}

impl ContainsAndIterOnType for BTreeSet<Type> {
    fn _contains(&self, item: &Type) -> bool {
        self.contains(item)
    }

    fn _iter(&self) -> impl Iterator<Item = &Type> + '_ {
        self.iter()
    }
}
