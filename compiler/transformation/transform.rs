/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;
use concept::type_::type_manager::TypeManager;
use ir::pattern::conjunction::Conjunction;
use ir::pattern::constraint::{Constraint, Different};
use storage::snapshot::ReadableSnapshot;

use crate::{
    annotation::pipeline::{AnnotatedPipeline, AnnotatedStage},
    transformation::{relation_index::relation_index_transformation, StaticOptimiserError},
};
use crate::annotation::type_annotations::TypeAnnotations;

pub fn apply_transformations(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    pipeline: &mut AnnotatedPipeline,
) -> Result<(), StaticOptimiserError> {
    for stage in &mut pipeline.annotated_stages {
        if let AnnotatedStage::Match { block, block_annotations, .. } = stage {
            relation_index_transformation(block.conjunction_mut(), block_annotations, type_manager, snapshot)?;
            insert_role_player_deduplication(block.conjunction_mut(), block_annotations)?;
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


fn insert_role_player_deduplication(conjunction: &mut Conjunction, block_annotations: &TypeAnnotations) -> Result<(), StaticOptimiserError> {
    let mut role_player_deduplication = HashMap::new();
    conjunction.constraints().iter().for_each(|constraint| {
        if let Constraint::Links(links) = constraint {
             let relation = links.relation().as_variable().unwrap();
            if !role_player_deduplication.contains_key(&relation) {
                role_player_deduplication.insert(relation.clone(), Vec::new());
            }
            role_player_deduplication.get_mut(&relation).unwrap().push(
                (links.player().as_variable().unwrap(), links.role_type().as_variable().unwrap(), block_annotations.constraint_annotations_of(constraint.clone()))
            );
        }
    });
    for (k, v) in role_player_deduplication {
        for i in 0..v.len() {
            for j in (i+1)..v.len() {
                let annotations_have_some_intersection = true;
                if annotations_have_some_intersection {
                    conjunction.constraints_mut().constraints_mut().push(Different::new(v[i].0, v[i].1, v[j].0, v[j].1).into());
                }
            }
        }
    }
    Ok(())
}
