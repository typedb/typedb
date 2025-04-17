/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{BTreeSet, HashMap, HashSet},
    sync::Arc,
};

use answer::{variable::Variable, Type};
use encoding::value::label::Label;
use error::unimplemented_feature;
use ir::{
    pattern::{conjunction::Conjunction, constraint::Constraint, nested_pattern::NestedPattern, BranchID, Vertex},
    pipeline::{ParameterRegistry, VariableRegistry},
};
use itertools::Itertools;

use crate::annotation::pipeline::AnnotatedStage;
#[derive(Debug, Clone)]
pub struct ParametrisedQueryStructure {
    pub branches: [Option<Vec<Constraint<Variable>>>; 64],
    pub resolved_labels: HashMap<Label, answer::Type>,
    pub function_texts: HashMap<Constraint<Variable>, String>,
}

impl ParametrisedQueryStructure {
    pub fn with_parameters(
        self: Arc<Self>,
        parameters: Arc<ParameterRegistry>,
        variable_names: HashMap<Variable, String>,
        available_variables: HashSet<Variable>,
    ) -> QueryStructure {
        QueryStructure { parametrised_structure: self, parameters, variable_names, available_variables }
    }
}

#[derive(Debug, Clone)]
pub struct QueryStructure {
    pub parametrised_structure: Arc<ParametrisedQueryStructure>,
    pub variable_names: HashMap<Variable, String>,
    pub available_variables: HashSet<Variable>,
    pub parameters: Arc<ParameterRegistry>,
}

pub(crate) fn extract_query_structure_from(
    variable_registry: &VariableRegistry,
    annotated_stages: Vec<AnnotatedStage>,
    source_query: &str,
) -> Option<ParametrisedQueryStructure> {
    if variable_registry.highest_branch_id_allocated() > 63 {
        return None;
    }
    let mut branches: [Option<_>; 64] = [(); 64].map(|_| None);
    let mut resolved_labels = HashMap::new();
    let mut function_texts = HashMap::new();
    annotated_stages.into_iter().for_each(|stage| {
        match stage {
            AnnotatedStage::Match { block, block_annotations, .. } => {
                extract_query_structure_from_branch(&mut branches, BranchID(0), block.conjunction());
                let block_label_annotations = block_annotations
                    .type_annotations()
                    .values()
                    .flat_map(|annotations| annotations.vertex_annotations().iter());
                extend_labels_from(&mut resolved_labels, block_label_annotations);
                extend_function_text_from(block.conjunction(), &mut function_texts, source_query);
            }
            AnnotatedStage::Insert { block, annotations, .. }
            | AnnotatedStage::Put { block, insert_annotations: annotations, .. }
            | AnnotatedStage::Update { block, annotations, .. } => {
                // May change with try-insert
                debug_assert!(block.conjunction().nested_patterns().is_empty());
                extract_query_structure_from_branch(&mut branches, BranchID(0), block.conjunction());
                extend_labels_from(&mut resolved_labels, annotations.vertex_annotations().iter());
            }
            AnnotatedStage::Delete { .. }
            | AnnotatedStage::Select(_)
            | AnnotatedStage::Sort(_)
            | AnnotatedStage::Offset(_)
            | AnnotatedStage::Limit(_)
            | AnnotatedStage::Require(_)
            | AnnotatedStage::Distinct(_)
            | AnnotatedStage::Reduce(_, _) => {}
        }
    });
    Some(ParametrisedQueryStructure { branches, resolved_labels, function_texts })
}

fn extract_query_structure_from_branch(
    branches: &mut [Option<Vec<Constraint<Variable>>>; 64],
    branch_id: BranchID,
    conjunction: &Conjunction,
) {
    if branches[branch_id.0 as usize].is_none() {
        branches[branch_id.0 as usize] = Some(Vec::new());
    }
    branches[branch_id.0 as usize].as_mut().unwrap().extend_from_slice(conjunction.constraints());
    conjunction.nested_patterns().iter().for_each(|nested| match nested {
        NestedPattern::Disjunction(disjunction) => {
            disjunction.branch_ids().iter().zip(disjunction.conjunctions().iter()).for_each(|(id, branch)| {
                extract_query_structure_from_branch(branches, *id, branch);
            })
        }
        NestedPattern::Negation(_) => {}
        NestedPattern::Optional(_) => {
            unimplemented_feature!(Optionals);
        }
    })
}

fn extend_labels_from<'a>(
    resolved_labels: &mut HashMap<Label, Type>,
    vertex_annotations: impl Iterator<Item = (&'a Vertex<Variable>, &'a Arc<BTreeSet<answer::Type>>)>,
) {
    resolved_labels.extend(vertex_annotations.filter_map(|(vertex, type_)| {
        match (vertex.as_label(), type_.iter().exactly_one()) {
            (Some(label), Ok(type_)) => Some((label.clone(), type_.clone())),
            _ => None,
        }
    }));
}

fn extend_function_text_from(
    conjunction: &Conjunction,
    function_texts: &mut HashMap<Constraint<Variable>, String>,
    source_query: &str,
) {
    conjunction.constraints().iter().for_each(|constraint| {
        if matches!(constraint, Constraint::ExpressionBinding(_) | Constraint::FunctionCallBinding(_)) {
            if let Some(span) = constraint.source_span() {
                function_texts.insert(constraint.clone(), source_query[span.begin_offset..span.end_offset].to_owned());
            }
        }
    });
    conjunction.nested_patterns().iter().for_each(|nested| match nested {
        NestedPattern::Disjunction(disj) => {
            disj.conjunctions().iter().for_each(|conj| extend_function_text_from(conj, function_texts, source_query))
        }
        NestedPattern::Negation(inner) => extend_function_text_from(inner.conjunction(), function_texts, source_query),
        NestedPattern::Optional(inner) => extend_function_text_from(inner.conjunction(), function_texts, source_query),
    })
}
