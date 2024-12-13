/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use concept::type_::type_manager::TypeManager;
use storage::snapshot::ReadableSnapshot;

use crate::{
    annotation::pipeline::{AnnotatedPipeline, AnnotatedStage},
    transformation::{relation_index::relation_index_transformation, StaticOptimiserError},
};

pub fn apply_transformations(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    pipeline: &mut AnnotatedPipeline,
) -> Result<(), StaticOptimiserError> {
    for stage in &mut pipeline.annotated_stages {
        if let AnnotatedStage::Match { block, block_annotations, .. } = stage {
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
