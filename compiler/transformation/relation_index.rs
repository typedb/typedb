/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

/*
Optimisation pass to convert simple binary relation traversal into rp-index lookups:

Given:
$r roleplayer $x (role filter: $role1)
$r roleplayer $y (role filter: $role2)

Where $r, $role1, and $role2 are NOT returned
Then we can collapse these into:

$x relation-rp-indexed $y (rel filter: $r, role-left filter: $role1, role-right filter: $role2)
 */

use std::collections::HashMap;
use answer::variable::Variable;
use concept::type_::type_manager::TypeManager;
use ir::pattern::conjunction::Conjunction;
use ir::pattern::constraint::{Constraint, IndexedRelation};
use ir::pattern::Vertex;
use storage::snapshot::ReadableSnapshot;
use crate::annotation::type_annotations::TypeAnnotations;
use crate::optimisation::StaticOptimiserError;

/// Precondition:
///   1) $r links $x (role: $role1)
///   2) $r links $y (role: $role2)
/// and all types in $r have a relation index
///
/// Then
///   replace 1) and 2) with
///   3) $x indexed_relation $y via $r ($role1, $role2)
pub(crate) fn relation_index_transformation(
    conjunction: &mut Conjunction,
    type_annotations: &TypeAnnotations,
    type_manager: &TypeManager,
    snapshot: &impl ReadableSnapshot,
) -> Result<(), StaticOptimiserError> {
    let mut candidates: HashMap<Vertex<Variable>, (usize, Vec<usize>)> = HashMap::new();
    for (index, constraint) in conjunction.constraints().iter().enumerate() {
        if let Constraint::Links(links) = constraint {
            let relation_variable = links.relation();
            candidates.entry(relation_variable.clone()).and_modify(|(first_index, other_indices)| {
                other_indices.push(index)
            }).or_insert((index, Vec::new()));
        }
    }

    while let Some(&relation) = candidates.keys().next() {
        let (links_index, other_links_indices) = candidates.remove(&relation).unwrap();
        // we will for now only apply the optimisation when it's exactly a binary edge query involving 2 role player variables
        if other_links_indices.len() == 1 && index_available(type_manager, snapshot, &relation, type_annotations)? {
            let other_links_index = other_links_indices[0];
            replace_links(conjunction, links_index, other_links_index);
            
            // update the other indexes so they remain accurate
            for (_, (index, other_indices)) in candidates.iter_mut() {
                *index -= index_decrement_if_removing(*index, links_index, other_links_index);
                for other_index in other_indices {
                    *other_index -= index_decrement_if_removing(*other_index, links_index, other_links_index)
                }
            }
        }
    }
    Ok(())
}

fn index_available(
    type_manager: &TypeManager,
    snapshot: &impl ReadableSnapshot,
    relation: &Vertex<Variable>,
    type_annotations: &TypeAnnotations
) -> Result<bool, StaticOptimiserError> {
    let relation_types = type_annotations.vertex_annotations_of(&relation).unwrap();
    for type_ in relation_types.iter() {
        let index_available = type_manager.relation_index_available(snapshot, type_.as_relation_type())
            .map_err(|err| StaticOptimiserError::ConceptRead { source: err })?;
        if !index_available {
            return Ok(false);
        }
    }
    return Ok(true);
}

fn replace_links(conjunction: &mut Conjunction, index_rp_1: usize, index_rp_2: usize) {
    debug_assert!(index_rp_1 != index_rp_2);
    let (remove_first, remove_second) = if index_rp_1 > index_rp_2 {
        (index_rp_1, index_rp_2)
    } else {
        (index_rp_2, index_rp_2)
    };

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
        links_2.role_type().clone().as_variable().unwrap()
    );
    conjunction.constraints_mut().constraints_mut().push(Constraint::IndexedRelation(indexed_relation));
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
