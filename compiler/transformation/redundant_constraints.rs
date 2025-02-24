/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

/*
Optimisation to remove redundant constraints.

Due to type inference, we should never need to check:

$x type <label>;

since $x will only be allowed take types indicated by the label.

We can also eliminate `isa` constraints, where the type is not named or is named but has no further constraints on it: TODO: double check these preconditions

$x isa $t

Since this will have been taken into account by type inference.

 */

use ir::pattern::{conjunction::Conjunction, constraint::Constraint, nested_pattern::NestedPattern, Scope};

use crate::annotation::type_annotations::{ConstraintTypeAnnotations, TypeAnnotations};

pub(super) fn prune_redundant_roleplayer_deduplication(
    conjunction: &mut Conjunction,
    block_annotations: &mut TypeAnnotations,
) {
    conjunction.constraints_mut().constraints_mut().retain(|constraint| {
        if let Constraint::LinksDeduplication(dedup) = constraint {
            let first = block_annotations
                .constraint_annotations_of(Constraint::Links(dedup.links1().clone()))
                .unwrap()
                .as_links()
                .player_to_role();
            let second = block_annotations
                .constraint_annotations_of(Constraint::Links(dedup.links2().clone()))
                .unwrap()
                .as_links()
                .player_to_role();
            first.iter().any(|(player, role_types)| {
                return second
                    .get(&player)
                    .map(|type_set| role_types.iter().any(|role_type| type_set.contains(role_type)))
                    .unwrap_or(false);
            })
        } else {
            true
        }
    });
}

pub fn optimize_away_statically_unsatisfiable_conjunctions(
    conjunction: &mut Conjunction,
    block_annotations: &TypeAnnotations,
) {
    let mut must_optimise_away = false;
    for nested in conjunction.nested_patterns_mut() {
        match nested {
            NestedPattern::Disjunction(disjunction) => {
                let mut optimised_unsatisfiable_branch_ids = Vec::new();
                for branch in disjunction.conjunctions_mut().iter_mut() {
                    optimize_away_statically_unsatisfiable_conjunctions(branch, block_annotations);
                    if branch.is_set_to_unsatisfiable() {
                        optimised_unsatisfiable_branch_ids.push(branch.scope_id())
                    }
                }
                disjunction.optimise_away_failing_branches(optimised_unsatisfiable_branch_ids);
                must_optimise_away = must_optimise_away || disjunction.conjunctions().is_empty();
            }
            NestedPattern::Negation(negation) => {
                optimize_away_statically_unsatisfiable_conjunctions(negation.conjunction_mut(), block_annotations);
            }
            NestedPattern::Optional(optional) => {
                optimize_away_statically_unsatisfiable_conjunctions(optional.conjunction_mut(), block_annotations);
            }
        }
    }
    let must_optimise_away = must_optimise_away
        || conjunction.constraints().iter().any(|constraint| {
            if let Some(constraint_annotation) = block_annotations.constraint_annotations_of(constraint.clone()) {
                match constraint_annotation {
                    ConstraintTypeAnnotations::LeftRight(lr) => {
                        debug_assert!(lr.left_to_right().is_empty() == lr.right_to_left().is_empty());
                        lr.left_to_right().is_empty()
                    }
                    ConstraintTypeAnnotations::Links(links) => {
                        links.player_to_role.is_empty() || links.relation_to_role.is_empty()
                    }
                    ConstraintTypeAnnotations::IndexedRelation(_) => {
                        unreachable!("This is called before IndexedRelations are inserted")
                    }
                }
            } else {
                false
            }
        });
    if must_optimise_away {
        conjunction.set_unsatisfiable();
    }
}
