/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use concept::error::ConceptReadError;
use error::typedb_error;
use ir::pattern::conjunction::Conjunction;
use crate::annotation::pipeline::AnnotatedPipeline;

pub mod relation_index;
mod redundant_constraints;
pub mod transform;

pub(crate) trait ConjunctionTransformation {
    fn apply(conjunction: &mut Conjunction);
}

pub(crate) trait PipelineTransformation {
    fn apply(pipeline: &mut AnnotatedPipeline);
}

typedb_error!(
    pub StaticOptimiserError(component = "Static optimiser", prefix = "SOP") {
        ConceptRead(1, "Error reading concept", ( source : Box<ConceptReadError> )),
    }
);
