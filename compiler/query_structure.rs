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
use encoding::value::{label::Label, value_type::ValueType};
use ir::{
    pattern::{
        conjunction::Conjunction, constraint::Constraint, nested_pattern::NestedPattern, BranchID, IrID, Pattern,
        Scope, ScopeId,
    },
    pipeline::{
        modifier::SortVariable,
        reduce::{AssignedReduction, Reducer},
        ParameterRegistry, VariableRegistry,
    },
};
use itertools::Itertools;
use serde::{Deserialize, Serialize, Serializer};

use crate::annotation::{
    function::{AnnotatedFunction, AnnotatedFunctionReturn},
    pipeline::{AnnotatedPipeline, AnnotatedStage},
    type_annotations::{BlockAnnotations, TypeAnnotations},
};

#[derive(Debug, Clone)]
pub struct QueryStructure {
    pub preamble: Vec<FunctionStructure>,
    pub query: PipelineStructure,
}

#[derive(Debug, Clone)]
pub struct PipelineStructure {
    pub parametrised_structure: Arc<ParametrisedPipelineStructure>,
    pub variable_names: HashMap<StructureVariableId, String>,
    pub parameters: Arc<ParameterRegistry>,
}

#[derive(Debug, Clone)]
pub struct FunctionStructure {
    pub body: PipelineStructure,
    pub arguments: Vec<StructureVariableId>,
    pub return_: FunctionReturnStructure,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "tag")]
pub enum FunctionReturnStructure {
    Stream { variables: Vec<StructureVariableId> },
    Single { selector: String, variables: Vec<StructureVariableId> },
    Check {},
    Reduce { reducers: Vec<StructureReducer> },
}

impl From<&AnnotatedFunctionReturn> for FunctionReturnStructure {
    fn from(value: &AnnotatedFunctionReturn) -> Self {
        match value {
            AnnotatedFunctionReturn::Stream { variables } => {
                Self::Stream { variables: variables.iter().map_into().collect() }
            }
            AnnotatedFunctionReturn::Single { variables, selector } => Self::Single {
                selector: selector.to_string().to_owned(),
                variables: variables.iter().map_into().collect(),
            },
            AnnotatedFunctionReturn::ReduceCheck {} => Self::Check {},
            AnnotatedFunctionReturn::ReduceReducer { instructions } => {
                let reducers = instructions
                    .iter()
                    .map(|reducer| StructureReducer {
                        reducer: reducer.unresolved().name(),
                        arguments: reducer.id().iter().map(|var| var.into()).collect(),
                    })
                    .collect();
                Self::Reduce { reducers }
            }
        }
    }
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
    let pipeline = Arc::new(parametrised_pipeline).with_parameters(parameters, variable_registry.variable_names());
    let preamble = annotated_pipeline
        .annotated_preamble
        .iter()
        .map(|function| extract_function_structure_from(function, source_query))
        .collect();
    QueryStructure { query: pipeline, preamble }
}

pub fn extract_function_structure_from(function: &AnnotatedFunction, source_query: &str) -> FunctionStructure {
    let parametrised_pipeline =
        extract_pipeline_structure_from(&function.variable_registry, function.stages.as_slice(), source_query);
    let body = Arc::new(parametrised_pipeline)
        .with_parameters(Arc::new(function.parameter_registry.clone()), function.variable_registry.variable_names());
    let arguments = function.arguments.iter().map_into().collect();
    let return_ = FunctionReturnStructure::from(&function.return_);
    FunctionStructure { body, arguments, return_ }
}

pub fn extract_pipeline_structure_from(
    variable_registry: &VariableRegistry,
    annotated_stages: &[AnnotatedStage],
    source_query: &str,
) -> ParametrisedPipelineStructure {
    let branch_ids_allocated = variable_registry.branch_ids_allocated();
    let mut builder = ParametrisedQueryStructureBuilder::new(source_query, branch_ids_allocated);
    annotated_stages.into_iter().enumerate().for_each(|(index, stage)| builder.add_stage(stage, StageIndex(index)));
    let output_variables = annotated_stages
        .iter()
        .rev()
        .filter_map(|stage| match stage {
            AnnotatedStage::Match { block, .. } => Some(
                block
                    .conjunction()
                    .variable_binding_modes()
                    .iter()
                    .filter_map(|(v, mode)| (!mode.is_locally_binding_in_child()).then_some(v.clone()))
                    .collect::<Vec<_>>(),
            ),
            | AnnotatedStage::Insert { block, .. }
            | AnnotatedStage::Update { block, .. }
            | AnnotatedStage::Put { block, .. } => {
                Some(block.conjunction().variable_binding_modes().keys().cloned().collect::<Vec<_>>())
            }
            AnnotatedStage::Delete { block, deleted_variables, .. } => Some(
                block
                    .conjunction()
                    .variable_binding_modes()
                    .keys()
                    .filter(|v| !deleted_variables.contains(v))
                    .cloned()
                    .collect::<Vec<_>>(),
            ),
            AnnotatedStage::Select(select) => Some(select.variables.iter().cloned().collect::<Vec<_>>()),
            AnnotatedStage::Reduce(reduce, _) => Some(reduce.variables().collect::<Vec<_>>()),
            AnnotatedStage::Sort(_) => None,
            | AnnotatedStage::Offset(_) => None,
            | AnnotatedStage::Limit(_) => None,
            | AnnotatedStage::Require(_) => None,
            | AnnotatedStage::Distinct(_) => None,
        })
        .next()
        .unwrap_or_default();
    builder.finish(output_variables.iter().map(|v| StructureVariableId::from(v)).collect())
}

#[derive(Debug, Clone)]
pub enum PipelineVariableAnnotation {
    Instance(Vec<answer::Type>),
    Type(Vec<answer::Type>),
    Value(ValueType),
}

#[derive(Debug, Clone, Copy, Eq, Hash, PartialEq)]
pub struct StageIndex(pub usize);

#[derive(Debug, Clone)]
pub struct PipelineVariableAnnotationAndModifier {
    pub is_optional: bool,
    pub annotations: PipelineVariableAnnotation,
}

pub type ConjunctionAnnotations = BTreeMap<StructureVariableId, PipelineVariableAnnotationAndModifier>;
pub type PipelineStructureAnnotations = Vec<ConjunctionAnnotations>;

#[derive(Debug, Clone)]
pub struct ParametrisedPipelineStructure {
    pub stages: Vec<QueryStructureStage>,
    pub conjunctions: Vec<QueryStructureConjunction>,
    pub output_variables: Vec<StructureVariableId>,
    pub resolved_labels: HashMap<Label, answer::Type>,
    pub calls_syntax: HashMap<Constraint<Variable>, String>,
    pub scope_to_conjunction_id: HashMap<(StageIndex, ScopeId), QueryStructureConjunctionID>,
}

impl ParametrisedPipelineStructure {
    pub fn with_parameters(
        self: Arc<Self>,
        parameters: Arc<ParameterRegistry>,
        variable_names: &HashMap<Variable, String>,
    ) -> PipelineStructure {
        let variable_names = variable_names.iter().map(|(var, name)| (var.into(), name.clone())).collect();
        PipelineStructure { parametrised_structure: self, parameters, variable_names }
    }

    pub fn always_involved_blocks(&self) -> Vec<QueryStructureConjunctionID> {
        self.stages
            .iter()
            .filter_map(|stage| match stage {
                QueryStructureStage::Match { block }
                | QueryStructureStage::Insert { block }
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

    pub fn resolve_conjunction_id(&self, stage_index: StageIndex, scope_id: ScopeId) -> QueryStructureConjunctionID {
        self.scope_to_conjunction_id.get(&(stage_index, scope_id)).unwrap().clone()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "tag")]
pub enum QueryStructureStage {
    Match {
        block: QueryStructureConjunctionID,
    },
    Insert {
        block: QueryStructureConjunctionID,
    },
    #[serde(rename_all = "camelCase")]
    Delete {
        block: QueryStructureConjunctionID,
        deleted_variables: Vec<StructureVariableId>,
    },
    Put {
        block: QueryStructureConjunctionID,
    },
    Update {
        block: QueryStructureConjunctionID,
    },

    Select {
        variables: Vec<StructureVariableId>,
    },
    Sort {
        variables: Vec<StructureSortVariable>,
    },
    Offset {
        offset: u64,
    },
    Limit {
        limit: u64,
    },

    Require {
        variables: Vec<StructureVariableId>,
    },
    Distinct,
    Reduce {
        reducers: Vec<StructureReduceAssign>,
        groupby: Vec<StructureVariableId>,
    },
}

#[derive(Debug, Clone)]
pub struct QueryStructureConjunction {
    pub constraints: Vec<Constraint<Variable>>,
    pub nested: Vec<QueryStructureNestedPattern>,
}

#[derive(Debug, Clone)]
pub enum QueryStructureNestedPattern {
    Or { branches: Vec<QueryStructureConjunctionID> },
    Not { conjunction: QueryStructureConjunctionID },
    Try { conjunction: QueryStructureConjunctionID },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct QueryStructureConjunctionID(pub u16);

impl QueryStructureConjunctionID {
    pub fn as_u32(&self) -> u32 {
        self.0 as u32
    }
}

#[derive(Debug, Clone)]
pub struct ParametrisedQueryStructureBuilder<'a> {
    pipeline_structure: ParametrisedPipelineStructure,
    source_query: &'a str,
}

impl<'a> ParametrisedQueryStructureBuilder<'a> {
    fn new(source_query: &'a str, branch_ids_allocated: u16) -> Self {
        // Pre-allocated for query branches that have already been allocated branch ids
        let conjunctions = vec![
            QueryStructureConjunction { constraints: Vec::new(), nested: Vec::new() };
            branch_ids_allocated as usize
        ];
        Self {
            source_query,
            pipeline_structure: ParametrisedPipelineStructure {
                stages: Vec::new(),
                conjunctions,
                output_variables: Vec::new(),
                resolved_labels: HashMap::new(),
                calls_syntax: HashMap::new(),
                scope_to_conjunction_id: HashMap::new(),
            },
        }
    }

    pub fn add_stage(&mut self, stage: &AnnotatedStage, stage_index: StageIndex) {
        match stage {
            AnnotatedStage::Match { block, block_annotations, .. } => {
                let conjunction = self.add_conjunction(stage_index, None, block.conjunction(), &block_annotations);
                self.pipeline_structure.stages.push(QueryStructureStage::Match { block: conjunction });
            }
            AnnotatedStage::Insert { block, annotations, .. } => {
                let block = self.add_conjunction(stage_index, None, block.conjunction(), &annotations);
                self.pipeline_structure.stages.push(QueryStructureStage::Insert { block });
            }
            AnnotatedStage::Put { block, insert_annotations: annotations, .. } => {
                let block = self.add_conjunction(stage_index, None, block.conjunction(), &annotations);
                self.pipeline_structure.stages.push(QueryStructureStage::Put { block });
            }
            AnnotatedStage::Update { block, annotations, .. } => {
                let block = self.add_conjunction(stage_index, None, block.conjunction(), &annotations);
                self.pipeline_structure.stages.push(QueryStructureStage::Update { block });
            }
            AnnotatedStage::Delete { block, deleted_variables, annotations, .. } => {
                let block = self.add_conjunction(stage_index, None, block.conjunction(), &annotations);
                self.pipeline_structure
                    .stages
                    .push(QueryStructureStage::Delete { block, deleted_variables: vec_from(deleted_variables.iter()) });
            }
            AnnotatedStage::Select(select) => self
                .pipeline_structure
                .stages
                .push(QueryStructureStage::Select { variables: vec_from(select.variables.iter().sorted()) }),
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

    fn add_conjunction(
        &mut self,
        stage_index: StageIndex,
        existing_branch_id: Option<BranchID>,
        conjunction: &Conjunction,
        block_annotations: &BlockAnnotations,
    ) -> QueryStructureConjunctionID {
        let mut nested_patterns = Vec::new();
        conjunction.nested_patterns().iter().for_each(|nested| match nested {
            NestedPattern::Disjunction(disjunction) => {
                let branches = disjunction
                    .conjunctions_by_branch_id()
                    .map(|(id, branch)| self.add_conjunction(stage_index, Some(*id), branch, block_annotations))
                    .collect::<Vec<_>>();
                nested_patterns.push(QueryStructureNestedPattern::Or { branches });
            }
            NestedPattern::Negation(negation) => {
                let conj = self.add_conjunction(stage_index, None, negation.conjunction(), block_annotations);
                nested_patterns.push(QueryStructureNestedPattern::Not { conjunction: conj });
            }
            NestedPattern::Optional(optional) => {
                let conj = self.add_conjunction(
                    stage_index,
                    Some(optional.branch_id()),
                    optional.conjunction(),
                    block_annotations,
                );
                nested_patterns.push(QueryStructureNestedPattern::Try { conjunction: conj });
            }
        });
        self.add_conjunction_to_structure(
            stage_index,
            conjunction.scope_id(),
            existing_branch_id,
            Vec::from(conjunction.constraints()),
            nested_patterns,
            block_annotations.type_annotations_of(conjunction).unwrap(),
        )
    }

    fn add_conjunction_to_structure(
        &mut self,
        stage_index: StageIndex,
        scope_id: ScopeId,
        existing_branch_id: Option<BranchID>,
        constraints: Vec<Constraint<Variable>>,
        nested: Vec<QueryStructureNestedPattern>,
        annotations: &TypeAnnotations,
    ) -> QueryStructureConjunctionID {
        self.extend_labels_from(annotations);
        self.extend_function_calls_syntax_from(&constraints);
        let conj_id = if let Some(BranchID(id)) = existing_branch_id {
            debug_assert!((id as usize) < self.pipeline_structure.conjunctions.len());
            self.pipeline_structure.conjunctions[id as usize] = QueryStructureConjunction { constraints, nested };
            QueryStructureConjunctionID(id)
        } else {
            self.pipeline_structure.conjunctions.push(QueryStructureConjunction { constraints, nested });
            QueryStructureConjunctionID(self.pipeline_structure.conjunctions.len() as u16 - 1)
        };
        self.pipeline_structure.scope_to_conjunction_id.insert((stage_index, scope_id), conj_id.clone());
        conj_id
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

    pub fn finish(self, mut output_variables: Vec<StructureVariableId>) -> ParametrisedPipelineStructure {
        let Self { mut pipeline_structure, .. } = self;
        output_variables.sort();
        pipeline_structure.output_variables = output_variables;
        pipeline_structure
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

impl StructureVariableId {
    pub fn as_u32(&self) -> u32 {
        self.0 as u32
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
#[serde(rename_all = "camelCase", tag = "tag")]
pub enum StructureSortVariable {
    Ascending { variable: StructureVariableId },
    Descending { variable: StructureVariableId },
}

impl From<&SortVariable> for StructureSortVariable {
    fn from(value: &SortVariable) -> Self {
        let variable = value.variable().into();
        match value {
            SortVariable::Ascending(_) => Self::Ascending { variable },
            SortVariable::Descending(_) => Self::Descending { variable },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StructureReducer {
    pub reducer: String,
    pub arguments: Vec<StructureVariableId>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StructureReduceAssign {
    pub assigned: StructureVariableId,
    pub reducer: StructureReducer,
}

impl From<&AssignedReduction> for StructureReduceAssign {
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
        let reducer = StructureReducer { reducer: value.reduction.name(), arguments };
        StructureReduceAssign { assigned: value.assigned.into(), reducer }
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
