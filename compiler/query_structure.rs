/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{BTreeMap, HashMap},
    fmt::Formatter,
    marker::PhantomData,
    str::FromStr,
    sync::Arc,
};

use answer::variable::Variable;
use encoding::value::label::Label;
use ir::{
    pattern::{
        conjunction::Conjunction, constraint::Constraint, nested_pattern::NestedPattern, BranchID, IrID, Scope, ScopeId,
    },
    pipeline::{
        modifier::SortVariable,
        reduce::{AssignedReduction, Reducer},
        ParameterRegistry, VariableRegistry,
    },
};
use itertools::Itertools;
use serde::{Deserialize, Serialize, Serializer};

use crate::{
    annotation::{
        function::{AnnotatedFunction, FunctionParameterAnnotation},
        pipeline::{AnnotatedPipeline, AnnotatedStage},
        type_annotations::{BlockAnnotations, TypeAnnotations},
    },
    VariablePosition,
};

#[derive(Debug, Clone)]
pub struct QueryStructure {
    pub preamble: Vec<FunctionStructure>,
    pub pipeline: Option<PipelineStructure>,
    // fetch: Option<FetchStructure>, // TODO?
}

#[derive(Debug, Clone)]
pub struct FunctionStructure {
    pub pipeline: Option<PipelineStructure>,
    // TODO: arguments, returned
}

#[derive(Debug, Clone)]
pub struct PipelineStructure {
    pub parametrised_structure: Arc<ParametrisedPipelineStructure>,
    pub variable_names: HashMap<StructureVariableId, String>,
    pub available_variables: Vec<StructureVariableId>,
    pub parameters: Arc<ParameterRegistry>,
}

pub fn extract_query_structure_from(
    variable_registry: &VariableRegistry,
    parameters: Arc<ParameterRegistry>,
    annotated_pipeline: &AnnotatedPipeline,
    source_query: &str,
) -> QueryStructure {
    let parametrised_pipeline = extract_pipeline_structure_from(
        variable_registry,
        annotated_pipeline.annotated_stages.as_slice(),
        source_query,
    );
    // We don't compile, so positions don't exist. Assign arbitrarily
    let output_positions = annotated_pipeline
        .annotated_stages
        .last()
        .unwrap()
        .named_referenced_variables(variable_registry)
        .enumerate()
        .map(|(i, var)| (var, VariablePosition::new(i as u32)))
        .collect();

    let pipeline = parametrised_pipeline
        .map(|pp| Arc::new(pp).with_parameters(parameters, variable_registry.variable_names(), &output_positions));
    let preamble = annotated_pipeline
        .annotated_preamble
        .iter()
        .map(|function| extract_function_structure_from(function, source_query))
        .collect();
    QueryStructure { pipeline, preamble }
}

pub fn extract_function_structure_from(function: &AnnotatedFunction, source_query: &str) -> FunctionStructure {
    let parametrised_pipeline =
        extract_pipeline_structure_from(&function.variable_registry, function.stages.as_slice(), source_query);
    let pipeline = parametrised_pipeline.map(|pp| {
        Arc::new(pp).with_parameters(
            Arc::new(function.parameter_registry.clone()),
            function.variable_registry.variable_names(),
            &function
                .return_
                .referenced_variables()
                .iter()
                .enumerate()
                .map(|(i, var)| (*var, VariablePosition::new(i as u32)))
                .collect(),
        )
    });
    FunctionStructure { pipeline }
}

pub fn extract_pipeline_structure_from(
    variable_registry: &VariableRegistry,
    annotated_stages: &[AnnotatedStage],
    source_query: &str,
) -> Option<ParametrisedPipelineStructure> {
    let branch_ids_allocated = variable_registry.branch_ids_allocated();
    if branch_ids_allocated < 64 {
        let mut builder = ParametrisedQueryStructureBuilder::new(source_query, branch_ids_allocated);
        annotated_stages.into_iter().for_each(|stage| builder.add_stage(stage));
        Some(builder.pipeline_structure)
    } else {
        return None;
    }
}

pub type PipelineStructureAnnotations =
    BTreeMap<QueryStructureBlockID, BTreeMap<StructureVariableId, FunctionParameterAnnotation>>;
#[derive(Debug, Clone)]
pub struct ParametrisedPipelineStructure {
    pub stages: Vec<QueryStructureStage>,
    pub blocks: Vec<QueryStructureBlock>,
    pub resolved_labels: HashMap<Label, answer::Type>,
    pub calls_syntax: HashMap<Constraint<Variable>, String>,
    pub scope_to_block: HashMap<ScopeId, QueryStructureBlockID>,
}

impl ParametrisedPipelineStructure {
    pub fn with_parameters(
        self: Arc<Self>,
        parameters: Arc<ParameterRegistry>,
        variable_names: &HashMap<Variable, String>,
        output_variable_positions: &HashMap<Variable, VariablePosition>,
    ) -> PipelineStructure {
        let mut available_variables = output_variable_positions
            .keys()
            .filter(|v| !v.is_anonymous())
            .map(|v| StructureVariableId::from(v))
            .collect::<Vec<_>>();
        available_variables.sort();
        let variable_names = variable_names.iter().map(|(var, name)| (var.into(), name.clone())).collect();
        PipelineStructure { parametrised_structure: self, parameters, variable_names, available_variables }
    }

    pub fn always_taken_blocks(&self) -> Vec<QueryStructureBlockID> {
        self.stages
            .iter()
            .filter_map(|stage| match stage {
                QueryStructureStage::Match { block: QueryStructureConjunction { conjunction } } => match conjunction.first() {
                    Some(QueryStructurePattern::Block { index }) => Some(index),
                    Some(_) | None => {
                        debug_assert!(!conjunction.iter().any(|c| matches!(c, QueryStructurePattern::Block { .. })));
                        None
                    }
                },
                QueryStructureStage::Insert { block }
                | QueryStructureStage::Put { block }
                | QueryStructureStage::Update { block } => Some(block),

                QueryStructureStage::Select { .. }
                | QueryStructureStage::Delete { .. } // Deleted edges are deleted.
                | QueryStructureStage::Sort { .. }
                | QueryStructureStage::Offset { .. }
                | QueryStructureStage::Limit { .. }
                | QueryStructureStage::Require { .. }
                | QueryStructureStage::Distinct
                | QueryStructureStage::Reduce { .. } => None,
            })
            .cloned()
            .collect()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum QueryStructureStage {
    Match { block: QueryStructureConjunction },
    Insert { block: QueryStructureBlockID },
    Delete { block: QueryStructureBlockID, deleted_variables: Vec<StructureVariableId> },
    Put { block: QueryStructureBlockID },
    Update { block: QueryStructureBlockID },

    Select { variables: Vec<StructureVariableId> },
    Sort { variables: Vec<StructureSortVariable> },
    Offset { offset: u64 },
    Limit { limit: u64 },

    Require { variables: Vec<StructureVariableId> },
    Distinct,
    Reduce { reducers: Vec<StructureReducer>, groupby: Vec<StructureVariableId> },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryStructureConjunction {
    conjunction: Vec<QueryStructurePattern>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "tag")]
pub enum QueryStructurePattern {
    Block { index: QueryStructureBlockID },
    Or { branches: Vec<QueryStructureConjunction> },
    Not(QueryStructureConjunction),
    Try(QueryStructureConjunction),
}

#[derive(Debug, Clone)]
pub struct QueryStructureBlock {
    pub constraints: Vec<Constraint<Variable>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct QueryStructureBlockID(pub u16);

#[derive(Debug, Clone)]
pub struct ParametrisedQueryStructureBuilder<'a> {
    pipeline_structure: ParametrisedPipelineStructure,
    source_query: &'a str,
}

impl<'a> ParametrisedQueryStructureBuilder<'a> {
    fn new(source_query: &'a str, branch_ids_allocated: u16) -> Self {
        // Pre-allocated for query branches that have already been allocated branch ids
        let blocks = vec![QueryStructureBlock { constraints: Vec::new() }; branch_ids_allocated as usize];
        Self {
            source_query,
            pipeline_structure: ParametrisedPipelineStructure {
                stages: Vec::new(),
                blocks,
                resolved_labels: HashMap::new(),
                calls_syntax: HashMap::new(),
                scope_to_block: HashMap::new(),
            },
        }
    }

    pub fn add_stage(&mut self, stage: &AnnotatedStage) {
        match stage {
            AnnotatedStage::Match { block, block_annotations, .. } => {
                let conjunction = self.add_block(None, block.conjunction(), &block_annotations);
                self.pipeline_structure.stages.push(QueryStructureStage::Match { block: conjunction });
            }
            AnnotatedStage::Insert { block, annotations, .. } => {
                debug_assert!(block.conjunction().nested_patterns().is_empty());
                let block = self.add_block_impl(
                    block.conjunction().scope_id(),
                    None,
                    block.conjunction().constraints(),
                    &annotations,
                );
                self.pipeline_structure.stages.push(QueryStructureStage::Insert { block });
            }
            AnnotatedStage::Put { block, insert_annotations: annotations, .. } => {
                debug_assert!(block.conjunction().nested_patterns().is_empty());
                let block = self.add_block_impl(
                    block.conjunction().scope_id(),
                    None,
                    block.conjunction().constraints(),
                    &annotations,
                );
                self.pipeline_structure.stages.push(QueryStructureStage::Put { block });
            }
            AnnotatedStage::Update { block, annotations, .. } => {
                debug_assert!(block.conjunction().nested_patterns().is_empty());
                let block = self.add_block_impl(
                    block.conjunction().scope_id(),
                    None,
                    block.conjunction().constraints(),
                    &annotations,
                );
                self.pipeline_structure.stages.push(QueryStructureStage::Update { block });
            }
            AnnotatedStage::Delete { block, deleted_variables, annotations, .. } => {
                debug_assert!(block.conjunction().nested_patterns().is_empty());
                let block = self.add_block_impl(
                    block.conjunction().scope_id(),
                    None,
                    block.conjunction().constraints(),
                    &annotations,
                );
                self.pipeline_structure
                    .stages
                    .push(QueryStructureStage::Delete { block, deleted_variables: vec_from(deleted_variables.iter()) });
            }
            AnnotatedStage::Select(select) => self
                .pipeline_structure
                .stages
                .push(QueryStructureStage::Select { variables: vec_from(select.variables.iter()) }),
            AnnotatedStage::Sort(sort) => self
                .pipeline_structure
                .stages
                .push(QueryStructureStage::Sort { variables: vec_from(sort.variables.iter()) }),
            AnnotatedStage::Offset(offset) => {
                self.pipeline_structure.stages.push(QueryStructureStage::Offset { offset: offset.offset() })
            }
            AnnotatedStage::Limit(limit) => {
                self.pipeline_structure.stages.push(QueryStructureStage::Limit { limit: limit.limit() })
            }
            AnnotatedStage::Require(require) => self
                .pipeline_structure
                .stages
                .push(QueryStructureStage::Require { variables: vec_from(require.variables.iter()) }),
            AnnotatedStage::Distinct(_) => self.pipeline_structure.stages.push(QueryStructureStage::Distinct),
            AnnotatedStage::Reduce(reduce, _) => self.pipeline_structure.stages.push(QueryStructureStage::Reduce {
                reducers: vec_from(reduce.assigned_reductions.iter()),
                groupby: vec_from(reduce.groupby.iter()),
            }),
        }
    }

    fn add_block(
        &mut self,
        existing_branch_id: Option<BranchID>,
        conjunction: &Conjunction,
        block_annotations: &BlockAnnotations,
    ) -> QueryStructureConjunction {
        let mut conjuncts = Vec::new();
        let block_id = self.add_block_impl(
            conjunction.scope_id(),
            existing_branch_id,
            conjunction.constraints(),
            block_annotations.type_annotations_of(conjunction).unwrap(),
        );
        conjuncts.push(QueryStructurePattern::Block { index: block_id });
        conjunction.nested_patterns().iter().for_each(|nested| match nested {
            NestedPattern::Disjunction(disjunction) => {
                let branches = disjunction
                    .conjunctions_by_branch_id()
                    .map(|(id, branch)| self.add_block(Some(*id), branch, block_annotations))
                    .collect::<Vec<_>>();
                conjuncts.push(QueryStructurePattern::Or { branches });
            }
            NestedPattern::Negation(negation) => {
                let inner = self.add_block(None, negation.conjunction(), block_annotations);
                conjuncts.push(QueryStructurePattern::Not(inner));
            }
            NestedPattern::Optional(optional) => {
                let inner = self.add_block(Some(optional.branch_id()), optional.conjunction(), block_annotations);
                conjuncts.push(QueryStructurePattern::Try(inner));
            }
        });
        QueryStructureConjunction { conjunction: conjuncts }
    }

    fn add_block_impl(
        &mut self,
        scope_id: ScopeId,
        existing_branch_id: Option<BranchID>,
        constraints: &[Constraint<Variable>],
        annotations: &TypeAnnotations,
    ) -> QueryStructureBlockID {
        self.extend_labels_from(annotations);
        self.extend_function_calls_syntax_from(constraints);
        let block_id = if let Some(BranchID(id)) = existing_branch_id {
            debug_assert!((id as usize) < self.pipeline_structure.blocks.len());
            self.pipeline_structure.blocks[id as usize] = QueryStructureBlock { constraints: Vec::from(constraints) };
            QueryStructureBlockID(id)
        } else {
            self.pipeline_structure.blocks.push(QueryStructureBlock { constraints: Vec::from(constraints) });
            QueryStructureBlockID(self.pipeline_structure.blocks.len() as u16 - 1)
        };
        self.pipeline_structure.scope_to_block.insert(scope_id, block_id.clone());
        block_id
    }

    fn extend_function_calls_syntax_from(&mut self, constraints: &[Constraint<Variable>]) {
        constraints
            .iter()
            .filter(|constraint| {
                matches!(constraint, Constraint::ExpressionBinding(_) | Constraint::FunctionCallBinding(_))
            })
            .for_each(|constraint| {
                if let Some(span) = constraint.source_span() {
                    let syntax = self.source_query[span.begin_offset..span.end_offset].to_owned();
                    self.pipeline_structure.calls_syntax.insert(constraint.clone(), syntax);
                }
            });
    }

    fn extend_labels_from(&mut self, type_annotations: &TypeAnnotations) {
        self.pipeline_structure.resolved_labels.extend(type_annotations.vertex_annotations().iter().filter_map(
            |(vertex, type_)| match (vertex.as_label(), type_.iter().exactly_one()) {
                (Some(label), Ok(type_)) => Some((label.clone(), type_.clone())),
                _ => None,
            },
        ));
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Eq, Hash, PartialEq, Ord, PartialOrd)]
pub struct StructureVariableId(
    #[serde(serialize_with = "serialize_using_to_string")]
    #[serde(deserialize_with = "deserialize_using_from_string")]
    u16,
);

impl std::fmt::Display for StructureVariableId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "StructureVariable({})", self.0)
    }
}

impl IrID for StructureVariableId {}

impl From<&Variable> for StructureVariableId {
    fn from(value: &Variable) -> Self {
        Self(value.id().as_u16())
    }
}

impl From<Variable> for StructureVariableId {
    fn from(value: Variable) -> Self {
        (&value).into()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StructureSortVariable {
    variable: StructureVariableId,
    ascending: bool,
}

impl From<&SortVariable> for StructureSortVariable {
    fn from(value: &SortVariable) -> Self {
        let ascending = match value {
            SortVariable::Ascending(_) => true,
            SortVariable::Descending(_) => false,
        };
        Self { variable: value.variable().into(), ascending }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StructureReducer {
    pub assigned: StructureVariableId,
    pub reducer: String,
    pub arguments: Vec<StructureVariableId>,
}

impl From<&AssignedReduction> for StructureReducer {
    fn from(value: &AssignedReduction) -> Self {
        let arguments = match value.reduction {
            Reducer::Count => vec![],
            Reducer::CountVar(var)
            | Reducer::Sum(var)
            | Reducer::Max(var)
            | Reducer::Mean(var)
            | Reducer::Median(var)
            | Reducer::Min(var)
            | Reducer::Std(var) => vec![var.into()],
        };
        StructureReducer { assigned: value.assigned.into(), reducer: value.reduction.name(), arguments }
    }
}

// utils
fn vec_from<'a, T: 'a, U: for<'b> From<&'b T>>(from: impl Iterator<Item = &'a T>) -> Vec<U> {
    from.into_iter().map(|v| v.into()).collect()
}

fn serialize_using_to_string<S: Serializer, T: ToString>(value: &T, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(&value.to_string())
}

fn deserialize_using_from_string<'de, D: serde::de::Deserializer<'de>, T: FromStr>(
    deserializer: D,
) -> Result<T, D::Error> {
    // define a visitor that deserializes
    // `ActualData` encoded as json within a string
    struct Visitor<T> {
        phantom: PhantomData<T>,
    }

    impl<'de, T1: FromStr> serde::de::Visitor<'de> for Visitor<T1> {
        type Value = T1;

        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "A string that can be converted to {} via FromStr", std::any::type_name::<Self::Value>())
        }

        fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Self::Value::from_str(v).map_err(|_err| {
                E::custom(format!("Could not deserialize {} from {}", std::any::type_name::<Self::Value>(), v))
            })
        }
    }

    deserializer.deserialize_any(Visitor { phantom: PhantomData })
}
