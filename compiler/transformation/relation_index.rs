/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;

use answer::variable::Variable;
use concept::type_::type_manager::TypeManager;
use ir::pattern::{
    conjunction::Conjunction,
    constraint::{Comparator, Constraint, IndexedRelation, Links},
    nested_pattern::NestedPattern,
    Vertex,
};
use storage::snapshot::ReadableSnapshot;

use crate::{
    annotation::type_annotations::{ConstraintTypeAnnotations, IndexedRelationAnnotations, TypeAnnotations},
    transformation::StaticOptimiserError,
};

/// Precondition:
///   1) $r links $x (role: $role1)
///   2) $r links $y (role: $role2)
/// and all types in $r have a relation index
/// and $r does not have an attribute with an equality comparator (either constant, or an attribute/value variable)
///     (heuristically, this will often produce worse plans since we can't find the relation by attribute value, then intersect on the relation)
/// and there are exactly 2 query player variables in the relation $r
///
/// TODO: we should just add the relation index when available and make it mutually exclusive to the 2 links constraints, rather than replacing them
///
/// Then
///   replace 1) and 2) with
///   3) $x indexed_relation $y via $r ($role1, $role2)
pub fn relation_index_transformation(
    conjunction: &mut Conjunction,
    type_annotations: &mut TypeAnnotations,
    type_manager: &TypeManager,
    snapshot: &impl ReadableSnapshot,
) -> Result<(), StaticOptimiserError> {
    let mut candidates: HashMap<Vertex<Variable>, (usize, Vec<usize>)> = HashMap::new();
    for (index, constraint) in conjunction.constraints().iter().enumerate() {
        if let Constraint::Links(links) = constraint {
            let relation_variable = links.relation();
            candidates
                .entry(relation_variable.clone())
                .and_modify(|(_first_index, other_indices)| other_indices.push(index))
                .or_insert((index, Vec::new()));
        }
    }

    while let Some(relation) = candidates.keys().next() {
        let relation = relation.clone(); // release borrow on candidates
        let (links_index, other_links_indices) = candidates.remove(&relation).unwrap();
        // we will for now only apply the optimisation when it's exactly a binary edge query involving 2 role player variables
        if other_links_indices.len() == 1
            && index_available(type_manager, snapshot, &relation, type_annotations)?
            && !with_iid_or_constant_attribute(&relation, conjunction)
        {
            let other_links_index = other_links_indices[0];
            replace_links(conjunction, links_index, other_links_index, type_annotations);

            // update the other indexes so they remain accurate
            for (_, (index, other_indices)) in candidates.iter_mut() {
                *index -= index_decrement_if_removing(*index, links_index, other_links_index);
                for other_index in other_indices {
                    *other_index -= index_decrement_if_removing(*other_index, links_index, other_links_index)
                }
            }
        }
    }

    for nested in conjunction.nested_patterns_mut().iter_mut() {
        match nested {
            NestedPattern::Disjunction(disjunction) => {
                for branch_conjunction in disjunction.conjunctions_mut() {
                    relation_index_transformation(branch_conjunction, type_annotations, type_manager, snapshot)?;
                }
            }
            NestedPattern::Negation(negation) => {
                relation_index_transformation(negation.conjunction_mut(), type_annotations, type_manager, snapshot)?;
            }
            NestedPattern::Optional(optional) => {
                relation_index_transformation(optional.conjunction_mut(), type_annotations, type_manager, snapshot)?;
            }
        }
    }
    Ok(())
}

fn index_available(
    type_manager: &TypeManager,
    snapshot: &impl ReadableSnapshot,
    relation: &Vertex<Variable>,
    type_annotations: &TypeAnnotations,
) -> Result<bool, StaticOptimiserError> {
    let relation_types = type_annotations.vertex_annotations_of(&relation).unwrap();
    for type_ in relation_types.iter() {
        let index_available = type_manager
            .relation_index_available(snapshot, type_.as_relation_type())
            .map_err(|err| StaticOptimiserError::ConceptRead { source: err })?;
        if !index_available {
            return Ok(false);
        }
    }
    return Ok(true);
}

fn with_iid_or_constant_attribute(relation: &Vertex<Variable>, conjunction: &Conjunction) -> bool {
    for constraint in conjunction.constraints() {
        if let Some(iid_constraint) = constraint.as_iid() {
            if relation == iid_constraint.var() {
                return true;
            }
        } else if let Some(has_constraint) = constraint.as_has() {
            if relation == has_constraint.owner() && attribute_has_value(has_constraint.attribute(), conjunction) {
                return true;
            }
        }
    }
    return false;
}

fn attribute_has_value(attribute: &Vertex<Variable>, conjunction: &Conjunction) -> bool {
    conjunction.constraints().iter().filter_map(|constraint| constraint.as_comparison()).any(|comparison| {
        (comparison.lhs() == attribute || comparison.rhs() == attribute) && comparison.comparator() == Comparator::Equal
    })
}

// TODO: add indexed-relation with mutual exclusivity
fn replace_links(
    conjunction: &mut Conjunction,
    index_rp_1: usize,
    index_rp_2: usize,
    annotations: &mut TypeAnnotations,
) {
    debug_assert!(index_rp_1 != index_rp_2);
    let (remove_first, remove_second) =
        if index_rp_1 > index_rp_2 { (index_rp_1, index_rp_2) } else { (index_rp_2, index_rp_1) };

    let removed_1 = conjunction.constraints_mut().constraints_mut().remove(remove_first);
    let removed_2 = conjunction.constraints_mut().constraints_mut().remove(remove_second);

    let Constraint::Links(links_1) = removed_1 else {
        unreachable!("Unexpectedly received non-links constraint");
    };
    let Constraint::Links(links_2) = removed_2 else {
        unreachable!("Unexpectedly received non-links constraint");
    };
    debug_assert_eq!(links_1.relation(), links_2.relation());

    let indexed_relation = IndexedRelation::new(
        links_1.player().clone().as_variable().unwrap(),
        links_2.player().clone().as_variable().unwrap(),
        links_1.relation().clone().as_variable().unwrap(),
        links_1.role_type().clone().as_variable().unwrap(),
        links_2.role_type().clone().as_variable().unwrap(),
        // TODO: improve arbitrarily picking 1 links as the source span
        links_1.source_span(),
    );
    add_type_annotations(&links_1, &links_2, &indexed_relation, annotations);
    conjunction.constraints_mut().constraints_mut().push(Constraint::IndexedRelation(indexed_relation));
}

fn add_type_annotations(
    links_1: &Links<Variable>,
    links_2: &Links<Variable>,
    indexed_relation: &IndexedRelation<Variable>,
    type_annotations: &mut TypeAnnotations,
) {
    let links_1_annotations = type_annotations.constraint_annotations_of(Constraint::Links(links_1.clone())).unwrap();
    let links_2_annotations = type_annotations.constraint_annotations_of(Constraint::Links(links_2.clone())).unwrap();
    let indexed_annotations = IndexedRelationAnnotations::new(
        links_1_annotations.as_links().player_to_relation(),
        links_2_annotations.as_links().relation_to_player(),
        links_2_annotations.as_links().player_to_relation(),
        links_1_annotations.as_links().relation_to_player(),
        links_1_annotations.as_links().player_to_role(),
        links_2_annotations.as_links().player_to_role(),
        links_1_annotations.as_links().relation_to_role(),
        links_2_annotations.as_links().relation_to_role(),
    );
    type_annotations.constraint_annotations_mut().insert(
        Constraint::IndexedRelation(indexed_relation.clone()),
        ConstraintTypeAnnotations::IndexedRelation(indexed_annotations),
    );
}

fn index_decrement_if_removing(index: usize, removed_1: usize, removed_2: usize) -> usize {
    let mut decrement_by = 0;
    if index > removed_1 {
        decrement_by += 1;
    }
    if index > removed_2 {
        decrement_by += 1;
    }
    decrement_by
}
