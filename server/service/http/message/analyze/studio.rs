/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::{BTreeMap, HashMap};

use compiler::query_structure::{PipelineStructure, StructureVariableId};
use concept::{error::ConceptReadError, type_::type_manager::TypeManager};
use serde::{Deserialize, Serialize};
use storage::snapshot::ReadableSnapshot;

use crate::service::http::message::{
    analyze::structure::{
        encode_analyzed_pipeline, AnalyzedPipelineResponse, StructureConstraint, StructureConstraintSpan,
        StructureConstraintWithSpan, StructureVariableInfo, StructureVertex,
    },
    query::concept::RoleTypeResponse,
};

// Backwards compatibility
pub fn encode_analyzed_pipeline_for_studio(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    structure: &PipelineStructure,
) -> Result<PipelineStructureResponseForStudio, Box<ConceptReadError>> {
    let dummy_annotations = vec![BTreeMap::new(); structure.parametrised_structure.conjunctions.len()];
    encode_analyzed_pipeline(snapshot, type_manager, structure, &dummy_annotations)
        .map(|analyzed| PipelineStructureResponseForStudio::from(analyzed))
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PipelineStructureResponseForStudio {
    blocks: Vec<StructureBlockForStudio>,
    variables: HashMap<StructureVariableId, StructureVariableInfo>,
    outputs: Vec<StructureVariableId>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct StructureBlockForStudio {
    constraints: Vec<StructureConstraintWithSpanForStudio>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct StructureConstraintWithSpanForStudio {
    text_span: Option<StructureConstraintSpan>,
    #[serde(flatten)]
    pub constraint: StructureConstraintForStudio,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", untagged)]
pub enum StructureConstraintForStudio {
    Normal(StructureConstraint),
    StudioOnly(StudioOnlyConstraint),
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "tag")]
pub enum StudioOnlyConstraint {
    Expression {
        text: String,
        assigned: Vec<StructureVertex>, // In just StructureVertex in analyze
        arguments: Vec<StructureVertex>,
    },
    Links {
        relation: StructureVertex,
        player: StructureVertex,
        role: StructureVertex, // Is a NamedRole in analyze, but Label here
    },
}

impl From<AnalyzedPipelineResponse> for PipelineStructureResponseForStudio {
    fn from(value: AnalyzedPipelineResponse) -> Self {
        let AnalyzedPipelineResponse { variables, outputs, conjunctions, .. } = value;
        let blocks = conjunctions
            .into_iter()
            .map(|conjunction| conjunction.constraints.into_iter().filter_map(|c| c.try_into().ok()).collect())
            .map(|constraints| StructureBlockForStudio { constraints })
            .collect();
        PipelineStructureResponseForStudio { variables, outputs, blocks }
    }
}

impl TryFrom<StructureConstraintWithSpan> for StructureConstraintWithSpanForStudio {
    type Error = ();

    fn try_from(value: StructureConstraintWithSpan) -> Result<Self, Self::Error> {
        Ok(Self { text_span: value.text_span, constraint: value.constraint.try_into()? })
    }
}

impl TryFrom<StructureConstraint> for StructureConstraintForStudio {
    type Error = ();
    fn try_from(value: StructureConstraint) -> Result<Self, Self::Error> {
        match value {
            StructureConstraint::Links { relation, player, role } => {
                let role = if let StructureVertex::NamedRole { name, .. } = role {
                    StructureVertex::Label { r#type: serde_json::json!(RoleTypeResponse { label: name.to_owned() }) }
                } else {
                    role
                };
                Ok(StructureConstraintForStudio::StudioOnly(StudioOnlyConstraint::Links { relation, player, role }))
            }
            StructureConstraint::Expression { text, assigned, arguments } => {
                let assigned = vec![assigned];
                Ok(StructureConstraintForStudio::StudioOnly(StudioOnlyConstraint::Expression {
                    text,
                    assigned,
                    arguments,
                }))
            }
            StructureConstraint::Or { .. } | StructureConstraint::Not { .. } | StructureConstraint::Try { .. } => {
                Err(())
            }
            c => Ok(StructureConstraintForStudio::Normal(c)),
        }
    }
}
