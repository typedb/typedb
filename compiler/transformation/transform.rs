/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use concept::type_::type_manager::TypeManager;
use ir::pattern::{conjunction::Conjunction, constraint::Constraint, nested_pattern::NestedPattern, Scope};
use storage::snapshot::ReadableSnapshot;

use crate::{
    annotation::{
        pipeline::{AnnotatedPipeline, AnnotatedStage},
        type_annotations::{ConstraintTypeAnnotations, TypeAnnotations},
    },
    transformation::{relation_index::relation_index_transformation, StaticOptimiserError},
};

pub fn apply_transformations(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    pipeline: &mut AnnotatedPipeline,
) -> Result<(), StaticOptimiserError> {
    for stage in &mut pipeline.annotated_stages {
        if let AnnotatedStage::Match { block, block_annotations, .. } = stage {
            optimize_away_statically_unsatisfiable_conjunctions(block.conjunction_mut(), block_annotations);
            prune_redundant_roleplayer_deduplication(block.conjunction_mut(), block_annotations);
            relation_index_transformation(block.conjunction_mut(), block_annotations, type_manager, snapshot)?;
        }
    }
    Ok(())

    // Ideas:
    // - we should move subtrees/graphs of a query that have no returned variables into a new pattern: "Check", which are only checked for a single answer
    // - we should push constraints, like comparisons, that apply to variables passed into functions, into the function itself
    // - function inlining v1: if a function does not have recursion or sort/offset/limit, we could inline the function into the query
    // - function inlining v2: we could try to inline/lift some constraints from recursive calls into the parent query to dramatically cut the search space
    // - function inlining v3: we could introduce new sub-patterns that include sort/offset/limit that let us more generally inline functions?
}

fn prune_redundant_roleplayer_deduplication(conjunction: &mut Conjunction, block_annotations: &mut TypeAnnotations) {
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
    optimize_away_statically_unsatisfiable_conjunctions_impl(conjunction, block_annotations);
}

fn optimize_away_statically_unsatisfiable_conjunctions_impl(
    conjunction: &mut Conjunction,
    block_annotations: &TypeAnnotations,
) -> bool {
    let mut must_optimise_away = false;
    for nested in conjunction.nested_patterns_mut() {
        match nested {
            NestedPattern::Disjunction(disjunction) => {
                let mut optimised_unsatisfiable_branch_ids = Vec::new();
                for branch in disjunction.conjunctions_mut().iter_mut() {
                    if optimize_away_statically_unsatisfiable_conjunctions_impl(branch, block_annotations) {
                        optimised_unsatisfiable_branch_ids.push(branch.scope_id())
                    }
                }
                disjunction.optimise_away_failing_branches(optimised_unsatisfiable_branch_ids);
                must_optimise_away = must_optimise_away || disjunction.conjunctions().is_empty();
            }
            NestedPattern::Negation(negation) => {
                optimize_away_statically_unsatisfiable_conjunctions_impl(negation.conjunction_mut(), block_annotations);
            }
            NestedPattern::Optional(optional) => {
                optimize_away_statically_unsatisfiable_conjunctions_impl(optional.conjunction_mut(), block_annotations);
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
        conjunction.optimise_away();
    }
    must_optimise_away
}
