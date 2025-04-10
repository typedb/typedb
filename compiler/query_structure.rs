use std::{
    collections::{BTreeSet, HashMap},
    sync::Arc,
};

use answer::{variable::Variable, Type};
use encoding::value::{label::Label, value::Value};
use error::unimplemented_feature;
use ir::{
    pattern::{
        conjunction::Conjunction, constraint::Constraint, nested_pattern::NestedPattern, BranchID, ParameterID, Vertex,
    },
    pipeline::{ParameterRegistry, VariableRegistry},
};
use itertools::Itertools;

use crate::{annotation::pipeline::AnnotatedStage, VariablePosition};

#[derive(Debug, Clone)]
pub struct ParametrisedQueryStructure {
    pub branches: [Option<Vec<Constraint<Variable>>>; 64],
    pub variable_positions: HashMap<Variable, VariablePosition>,
    pub resolved_labels: HashMap<Label, answer::Type>,
}

impl ParametrisedQueryStructure {
    pub fn with_parameters(self: Arc<Self>, parameters: Arc<ParameterRegistry>) -> QueryStructure {
        QueryStructure { parametrised_structure: self, parameters }
    }
}

#[derive(Debug, Clone)]
pub struct QueryStructure {
    pub parametrised_structure: Arc<ParametrisedQueryStructure>,
    pub parameters: Arc<ParameterRegistry>,
}

impl QueryStructure {
    pub fn get_parameter_value(&self, param: &ParameterID) -> Option<Value<'static>> {
        debug_assert!(matches!(param, ParameterID::Value(_, _)));
        self.parameters.value(*param).cloned()
    }

    pub fn get_variable_position(&self, variable: &Variable) -> Option<VariablePosition> {
        self.parametrised_structure.variable_positions.get(&variable).copied()
    }

    pub fn get_type(&self, label: &Label) -> Option<answer::Type> {
        self.parametrised_structure.resolved_labels.get(label).cloned()
    }
}

pub(crate) fn extract_query_structure_from(
    variable_registry: &VariableRegistry,
    annotated_stages: Vec<AnnotatedStage>,
    variable_positions: HashMap<Variable, VariablePosition>,
) -> Option<ParametrisedQueryStructure> {
    if variable_registry.highest_branch_id_allocated() > 63 {
        return None;
    }
    let mut branches: [Option<_>; 64] = [(); 64].map(|_| None);
    let mut resolved_labels = HashMap::new();

    annotated_stages.into_iter().for_each(|stage| {
        match stage {
            AnnotatedStage::Match { block, block_annotations, .. } => {
                extract_query_structure_from_branch(&mut branches, BranchID(0), block.conjunction());
                let block_label_annotations = block_annotations
                    .type_annotations()
                    .values()
                    .flat_map(|annotations| annotations.vertex_annotations().iter());
                extend_labels_from(&mut resolved_labels, block_label_annotations);
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
    Some(ParametrisedQueryStructure { branches, variable_positions, resolved_labels })
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
