/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::{BTreeMap, HashMap};

use answer::variable::Variable;
use bytes::util::HexBytesFormatter;
use compiler::{
    annotation::type_inference::get_type_annotation_from_label,
    query_structure::{
        ConjunctionAnnotations, FunctionReturnStructure, ParametrisedPipelineStructure, PipelineStructure,
        PipelineStructureAnnotations, QueryStructureConjunction, QueryStructureConjunctionID,
        QueryStructureNestedPattern, QueryStructureStage, StructureVariableId,
    },
};
use concept::{error::ConceptReadError, type_::type_manager::TypeManager};
use encoding::value::{label::Label, value::Value};
use ir::pattern::{
    constraint::{Constraint, IsaKind, SubKind},
    ParameterID, Vertex,
};
use serde::{Deserialize, Serialize};
use storage::snapshot::ReadableSnapshot;
use typeql::common::Span;

use super::annotations::{
    encode_variable_type_annotations_and_modifiers, ConjunctionAnnotationsResponse, FunctionReturnAnnotationsResponse,
    VariableAnnotationsResponse,
};
use crate::service::http::message::query::concept::{
    encode_type_concept, encode_value, EntityTypeResponse, ValueResponse,
};

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AnalyzedFunctionResponse {
    pub body: AnalyzedPipelineResponse,
    pub arguments: Vec<StructureVariableId>,
    pub returns: FunctionReturnStructure,
    pub argument_annotations: Vec<VariableAnnotationsResponse>,
    pub return_annotations: FunctionReturnAnnotationsResponse,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AnalyzedPipelineResponse {
    pub(super) conjunctions: Vec<AnalyzedConjunctionResponse>,
    pub(super) stages: Vec<QueryStructureStage>,
    pub(super) variables: HashMap<StructureVariableId, StructureVariableInfo>,
    pub(super) outputs: Vec<StructureVariableId>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AnalyzedConjunctionResponse {
    pub constraints: Vec<StructureConstraintWithSpan>,
    pub annotations: ConjunctionAnnotationsResponse,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StructureVariableInfo {
    name: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "tag")]
pub enum StructureConstraint {
    Isa {
        instance: StructureVertex,
        r#type: StructureVertex,
    },

    #[serde(rename = "isa!")]
    IsaExact {
        instance: StructureVertex,
        r#type: StructureVertex,
    },
    Has {
        owner: StructureVertex,
        attribute: StructureVertex,
    },
    Links {
        relation: StructureVertex,
        player: StructureVertex,
        role: StructureVertex,
    },

    Sub {
        subtype: StructureVertex,
        supertype: StructureVertex,
    },
    #[serde(rename = "sub!")]
    SubExact {
        subtype: StructureVertex,
        supertype: StructureVertex,
    },
    Owns {
        owner: StructureVertex,
        attribute: StructureVertex,
    },
    Relates {
        relation: StructureVertex,
        role: StructureVertex,
    },
    Plays {
        player: StructureVertex,
        role: StructureVertex,
    },

    FunctionCall {
        name: String,
        assigned: Vec<StructureVertex>,
        arguments: Vec<StructureVertex>,
    },
    Expression {
        text: String,
        assigned: StructureVertex,
        arguments: Vec<StructureVertex>,
    },
    Is {
        lhs: StructureVertex,
        rhs: StructureVertex,
    },
    Iid {
        concept: StructureVertex,
        iid: String,
    },
    Comparison {
        lhs: StructureVertex,
        rhs: StructureVertex,
        comparator: String,
    },
    Kind {
        kind: String,
        r#type: StructureVertex,
    },
    Label {
        r#type: StructureVertex,
        label: String,
    },
    Value {
        #[serde(rename = "attributeType")]
        attribute_type: StructureVertex,
        #[serde(rename = "valueType")]
        value_type: String,
    },

    // Nested patterns are now constraints too
    Or {
        branches: Vec<QueryStructureConjunctionID>,
    },
    Not {
        conjunction: QueryStructureConjunctionID,
    },
    Try {
        conjunction: QueryStructureConjunctionID,
    },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct StructureConstraintSpan {
    begin: usize,
    end: usize,
}

impl From<typeql::common::Span> for StructureConstraintSpan {
    fn from(value: Span) -> Self {
        StructureConstraintSpan { begin: value.begin_offset, end: value.end_offset }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StructureConstraintWithSpan {
    pub(super) text_span: Option<StructureConstraintSpan>,
    #[serde(flatten)]
    pub(super) constraint: StructureConstraint,
}

impl StructureConstraint {
    pub fn is_subpattern(&self) -> bool {
        matches!(self, Self::Or { .. } | Self::Not { .. } | Self::Try { .. })
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase", tag = "tag")]
pub(super) enum StructureVertex {
    Variable { id: StructureVariableId },
    Label { r#type: serde_json::Value },
    Value(ValueResponse),
    NamedRole { variable: StructureVariableId, name: String },
}

struct PipelineStructureContext<'a, Snapshot: ReadableSnapshot> {
    structure: &'a PipelineStructure,
    snapshot: &'a Snapshot,
    type_manager: &'a TypeManager,
    role_names: HashMap<Variable, String>,
    variables: &'a mut HashMap<StructureVariableId, StructureVariableInfo>,
}

impl<'a, Snapshot: ReadableSnapshot> PipelineStructureContext<'a, Snapshot> {
    pub fn get_parameter_value(&self, param: &ParameterID) -> Option<Value<'static>> {
        debug_assert!(matches!(param, ParameterID::Value { .. }));
        self.structure.parameters.value(param).cloned()
    }

    pub fn get_parameter_iid(&self, param: &ParameterID) -> Option<&[u8]> {
        self.structure.parameters.iid(param).map(|iid| iid.as_ref())
    }

    pub fn get_variable_name(&self, variable: &StructureVariableId) -> Option<String> {
        self.structure.variable_names.get(&variable).cloned()
    }

    pub fn get_type(&self, label: &Label) -> Option<answer::Type> {
        self.structure.parametrised_structure.resolved_labels.get(label).cloned()
    }

    fn get_call_syntax(&self, constraint: &Constraint<Variable>) -> Option<&String> {
        self.structure.parametrised_structure.calls_syntax.get(constraint)
    }

    fn get_role_type(&self, variable: &Variable) -> Option<&str> {
        self.role_names.get(variable).map(|name| name.as_str())
    }

    fn record_variable(&mut self, variable: StructureVariableId) {
        if !self.variables.contains_key(&variable) {
            let info = StructureVariableInfo { name: self.get_variable_name(&variable) };
            self.variables.insert(variable, info);
        }
    }
}

pub fn encode_analyzed_pipeline_for_studio(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    structure: &PipelineStructure,
) -> Result<AnalyzedPipelineResponse, Box<ConceptReadError>> {
    let dummy_annotations = vec![BTreeMap::new(); structure.parametrised_structure.conjunctions.len()];
    encode_analyzed_pipeline(snapshot, type_manager, structure, &dummy_annotations)
}

pub fn encode_analyzed_pipeline(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    structure: &PipelineStructure,
    annotations: &PipelineStructureAnnotations,
) -> Result<AnalyzedPipelineResponse, Box<ConceptReadError>> {
    let mut variables = HashMap::new();
    let ParametrisedPipelineStructure { stages, conjunctions, .. } = &*structure.parametrised_structure;
    let role_names = conjunctions
        .iter()
        .flat_map(|c| c.constraints.iter())
        .filter_map(|constraint| constraint.as_role_name())
        .map(|rolename| (rolename.type_().as_variable().unwrap(), rolename.name().to_owned()))
        .collect();
    let mut context =
        PipelineStructureContext { structure, snapshot, type_manager, role_names, variables: &mut variables };
    let encoded_conjunctions = conjunctions
        .iter()
        .zip(annotations.iter())
        .map(|(conj_structure, conj_annotations)| {
            encode_analyzed_conjunction(&mut context, conj_structure, conj_annotations)
        })
        .collect::<Result<Vec<_>, _>>()?;
    // Ensure reduced variables are added to variables
    record_reducer_variables(&mut context);
    let outputs = structure.parametrised_structure.output_variables.clone();
    Ok(AnalyzedPipelineResponse { conjunctions: encoded_conjunctions, outputs, variables, stages: stages.clone() })
}

fn record_reducer_variables<'a>(context: &mut PipelineStructureContext<'a, impl ReadableSnapshot>) {
    context.structure.parametrised_structure.stages.iter().for_each(|stage| {
        if let QueryStructureStage::Reduce { reducers, .. } = stage {
            reducers.iter().for_each(|reducer| context.record_variable(reducer.assigned));
        }
    });
}

fn encode_analyzed_conjunction<'a>(
    context: &mut PipelineStructureContext<'a, impl ReadableSnapshot>,
    structure: &QueryStructureConjunction,
    annotations: &ConjunctionAnnotations,
) -> Result<AnalyzedConjunctionResponse, Box<ConceptReadError>> {
    let mut encoded_constraints = Vec::new();
    structure.constraints.iter().enumerate().try_for_each(|(index, constraint)| {
        encode_structure_constraint(context, constraint, &mut encoded_constraints, index)
    })?;
    structure.nested.iter().try_for_each(|nested| encode_structure_nested_pattern(nested, &mut encoded_constraints))?;
    let variable_annotations = annotations
        .into_iter()
        .map(|(var_id, annotations)| {
            Ok((
                var_id.clone(),
                encode_variable_type_annotations_and_modifiers(context.snapshot, context.type_manager, annotations)?,
            ))
        })
        .collect::<Result<HashMap<_, _>, Box<ConceptReadError>>>()?;
    let encoded_annotations = ConjunctionAnnotationsResponse { variable_annotations };
    Ok(AnalyzedConjunctionResponse { constraints: encoded_constraints, annotations: encoded_annotations })
}

fn encode_structure_constraint(
    context: &mut PipelineStructureContext<'_, impl ReadableSnapshot>,
    constraint: &Constraint<Variable>,
    constraints: &mut Vec<StructureConstraintWithSpan>,
    index: usize,
) -> Result<(), Box<ConceptReadError>> {
    let text_span = constraint.source_span().map(Into::into);
    let mut push = |constraint: StructureConstraint| {
        constraints.push(StructureConstraintWithSpan { text_span, constraint });
    };
    match constraint {
        Constraint::Links(links) => push(StructureConstraint::Links {
            relation: encode_structure_vertex(context, links.relation())?,
            player: encode_structure_vertex(context, links.player())?,
            role: encode_role_type_as_vertex(context, links.role_type())?,
        }),
        Constraint::Has(has) => push(StructureConstraint::Has {
            owner: encode_structure_vertex(context, has.owner())?,
            attribute: encode_structure_vertex(context, has.attribute())?,
        }),
        Constraint::Isa(isa) => {
            let instance = encode_structure_vertex(context, isa.thing())?;
            let r#type = encode_structure_vertex(context, isa.type_())?;
            push(match isa.isa_kind() {
                IsaKind::Exact => StructureConstraint::IsaExact { instance, r#type },
                IsaKind::Subtype => StructureConstraint::Isa { instance, r#type },
            })
        }
        Constraint::Sub(sub) => {
            let subtype = encode_structure_vertex(context, sub.subtype())?;
            let supertype = encode_structure_vertex(context, sub.supertype())?;
            push(match sub.sub_kind() {
                SubKind::Exact => StructureConstraint::SubExact { subtype, supertype },
                SubKind::Subtype => StructureConstraint::Sub { subtype, supertype },
            })
        }
        Constraint::Owns(owns) => push(StructureConstraint::Owns {
            owner: encode_structure_vertex(context, owns.owner())?,
            attribute: encode_structure_vertex(context, owns.attribute())?,
        }),
        Constraint::Relates(relates) => push(StructureConstraint::Relates {
            relation: encode_structure_vertex(context, relates.relation())?,
            role: encode_role_type_as_vertex(context, relates.role_type())?,
        }),
        Constraint::Plays(plays) => {
            push(StructureConstraint::Plays {
                player: encode_structure_vertex(context, plays.player())?,
                role: encode_structure_vertex(context, plays.role_type())?, // Doesn't have to be encode_role_type
            })
        }
        Constraint::ExpressionBinding(expr) => push({
            let text =
                context.get_call_syntax(constraint).map_or_else(|| format!("Expression#{index}"), |text| text.clone());
            let assigned = encode_structure_vertex(context, expr.left())?;
            let arguments = encode_structure_vertices(context, expr.expression_ids())?;
            StructureConstraint::Expression { text, assigned, arguments }
        }),
        Constraint::FunctionCallBinding(function_call) => push({
            let text =
                context.get_call_syntax(constraint).map_or_else(|| format!("Function#{index}"), |text| text.clone());
            let assigned = encode_structure_vertices(context, function_call.ids_assigned())?;
            let arguments = encode_structure_vertices(context, function_call.function_call().argument_ids())?;

            StructureConstraint::FunctionCall { name: text, assigned, arguments }
        }),
        Constraint::Is(is) => push({
            StructureConstraint::Is {
                lhs: encode_structure_vertex(context, is.lhs())?,
                rhs: encode_structure_vertex(context, is.rhs())?,
            }
        }),
        Constraint::Iid(iid) => push({
            let concept = encode_structure_vertex(context, iid.var())?;
            let iid_bytes = context.get_parameter_iid(iid.iid().as_parameter().as_ref().unwrap()).unwrap();
            let iid = HexBytesFormatter::borrowed(iid_bytes).format_iid();
            StructureConstraint::Iid { concept, iid }
        }),
        Constraint::Comparison(comparison) => push(StructureConstraint::Comparison {
            lhs: encode_structure_vertex(context, comparison.lhs())?,
            rhs: encode_structure_vertex(context, comparison.rhs())?,
            comparator: comparison.comparator().name().to_owned(),
        }),
        Constraint::Kind(kind) => push(StructureConstraint::Kind {
            kind: kind.kind().name().to_owned(),
            r#type: encode_structure_vertex(context, kind.type_())?,
        }),
        Constraint::Label(label) => push(StructureConstraint::Label {
            r#type: encode_structure_vertex(context, label.type_())?,
            label: label
                .type_label()
                .as_label()
                .expect("Expected constant label in label constraint")
                .scoped_name()
                .as_str()
                .to_owned(),
        }),
        Constraint::Value(value) => push(StructureConstraint::Value {
            attribute_type: encode_structure_vertex(context, value.attribute_type())?,
            value_type: value.value_type().to_string(),
        }),
        Constraint::IndexedRelation(indexed) => {
            constraints.push(StructureConstraintWithSpan {
                text_span: indexed.source_span_1().map(Into::into),
                constraint: StructureConstraint::Links {
                    relation: encode_structure_vertex(context, indexed.relation())?,
                    player: encode_structure_vertex(context, indexed.player_1())?,
                    role: encode_role_type_as_vertex(context, indexed.role_type_1())?,
                },
            });
            constraints.push(StructureConstraintWithSpan {
                text_span: indexed.source_span_2().map(Into::into),
                constraint: StructureConstraint::Links {
                    relation: encode_structure_vertex(context, indexed.relation())?,
                    player: encode_structure_vertex(context, indexed.player_2())?,
                    role: encode_role_type_as_vertex(context, indexed.role_type_2())?,
                },
            });
        }
        // Constraints that probably don't need to be handled
        Constraint::RoleName(_) => {} // Handled separately via resolved_role_names
        // Optimisations don't represent the structure
        Constraint::LinksDeduplication(_) | Constraint::Unsatisfiable(_) => {}
    };
    Ok(())
}

fn encode_structure_nested_pattern(
    nested: &QueryStructureNestedPattern,
    constraints: &mut Vec<StructureConstraintWithSpan>,
) -> Result<(), Box<ConceptReadError>> {
    let constraint = match nested.clone() {
        QueryStructureNestedPattern::Or { branches } => StructureConstraint::Or { branches },
        QueryStructureNestedPattern::Not { conjunction } => StructureConstraint::Not { conjunction },
        QueryStructureNestedPattern::Try { conjunction } => StructureConstraint::Try { conjunction },
    };
    constraints.push(StructureConstraintWithSpan { constraint, text_span: None });
    Ok(())
}

fn encode_structure_vertices(
    context: &mut PipelineStructureContext<'_, impl ReadableSnapshot>,
    vertices: impl Iterator<Item = impl Into<Vertex<Variable>>>,
) -> Result<Vec<StructureVertex>, Box<ConceptReadError>> {
    vertices.map(|v| encode_structure_vertex(context, &v.into())).collect()
}

fn encode_structure_vertex(
    context: &mut PipelineStructureContext<'_, impl ReadableSnapshot>,
    vertex: &Vertex<Variable>,
) -> Result<StructureVertex, Box<ConceptReadError>> {
    let vertex = match vertex {
        Vertex::Variable(variable) => {
            context.record_variable(variable.into());
            StructureVertex::Variable { id: variable.into() }
        }
        Vertex::Label(label) => {
            let r#type = if let Some(type_) = context.get_type(label) {
                encode_type_concept(&type_, context.snapshot, context.type_manager)?
            } else if let Some(type_) = get_type_annotation_from_label(context.snapshot, context.type_manager, label)? {
                encode_type_concept(&type_, context.snapshot, context.type_manager)?
            } else {
                debug_assert!(false, "This should be unreachable, but we don't want crashes");
                let label = format!("ERROR_UNRESOLVED:{}", label.scoped_name.as_str());
                serde_json::json!(EntityTypeResponse { label })
            };
            StructureVertex::Label { r#type }
        }
        Vertex::Parameter(param) => {
            let value = context.get_parameter_value(param).unwrap();
            StructureVertex::Value(encode_value(value))
        }
    };
    Ok(vertex)
}

fn encode_role_type_as_vertex(
    context: &mut PipelineStructureContext<'_, impl ReadableSnapshot>,
    role_type: &Vertex<Variable>,
) -> Result<StructureVertex, Box<ConceptReadError>> {
    if let Some(name) = context.get_role_type(&role_type.as_variable().unwrap()) {
        Ok(StructureVertex::NamedRole {
            variable: StructureVariableId::from(role_type.as_variable().unwrap()),
            name: name.to_owned(),
        })
    } else {
        encode_structure_vertex(context, role_type)
    }
}

#[cfg(debug_assertions)]
pub mod bdd {
    use compiler::query_structure::{
        FunctionReturnStructure, QueryStructureConjunctionID, QueryStructureStage, StructureReduceAssign,
        StructureReducer, StructureSortVariable, StructureVariableId,
    };
    use itertools::Itertools;
    use serde_json::Value;

    use crate::service::http::message::analyze::{
        bdd::{
            functor_macros,
            functor_macros::{encode_functor_impl, impl_functor_for, impl_functor_for_impl, impl_functor_for_multi},
            FunctorContext, FunctorEncoded,
        },
        structure::{
            AnalyzedFunctionResponse, AnalyzedPipelineResponse, StructureConstraint, StructureConstraintWithSpan,
            StructureVertex,
        },
    };

    pub fn encode_pipeline_structure_as_functor(pipeline: &AnalyzedPipelineResponse) -> String {
        pipeline.encode_as_functor(&FunctorContext { pipeline })
    }

    pub fn encode_function_structure_as_functor(function: &AnalyzedFunctionResponse) -> String {
        function.encode_as_functor(&FunctorContext { pipeline: &function.body })
    }

    impl_functor_for!(struct AnalyzedPipelineResponse { stages, } named Pipeline);
    impl_functor_for!(struct AnalyzedFunctionResponse { arguments, returns, body, } named Function);
    impl_functor_for!(struct StructureReduceAssign { assigned, reducer,  } named ReduceAssign);
    impl_functor_for!(struct StructureReducer { reducer, arguments, } named Reducer);

    impl_functor_for!(enum QueryStructureStage [
        Match { block, } |
        Insert { block, } |
        Delete { deleted_variables, block, } |
        Put { block, } |
        Update { block, } |
        Select { variables, } |
        Sort { variables, } |
        Offset { offset, } |
        Limit { limit, } |
        Require { variables, } |
        Distinct { } |
        Reduce { reducers, groupby, } |
    ]);

    impl_functor_for!(enum StructureConstraint [
        Isa { instance, r#type, } |
        IsaExact { instance, r#type, } |
        Has { owner, attribute, } |
        Links { relation, player, role, } |
        Sub { subtype, supertype, } |
        SubExact { subtype, supertype, } |
        Owns { owner, attribute, } |
        Relates { relation, role, } |
        Plays { player, role, } |
        FunctionCall { name, assigned, arguments, } |
        Expression { text, assigned, arguments, } |
        Is { lhs, rhs, } |
        Iid { concept, iid, } |
        Comparison { lhs, rhs, comparator, } |
        Kind { kind, r#type, } |
        Label { r#type, label, } |
        Value { attribute_type, value_type, } |
        Or { branches, } |
        Not { conjunction, } |
        Try { conjunction, } |
    ]);

    impl_functor_for_impl!(StructureVertex => |self, context| {
        match self {
            StructureVertex::Variable { id } => { id.encode_as_functor(context) }
            StructureVertex::Label { r#type } => { r#type.as_object().unwrap()["label"].as_str().unwrap().to_owned() }
            StructureVertex::NamedRole{ name, .. } => { name.to_owned() },
            StructureVertex::Value(v) => {
                match &v.value {
                    Value::String(s) => std::format!("\"{}\"", s.to_string()),
                    other => other.to_string(),
                }
            }
        }
    });

    impl_functor_for_multi!(|self, context| [
        StructureVariableId =>  {
            format!("${}", context.pipeline.variables.get(self).and_then(|v| v.name.as_ref()).map_or("_", String::as_str))
        }
        QueryStructureConjunctionID => { context.pipeline.conjunctions[self.0 as usize].constraints.encode_as_functor(context) }
        StructureConstraintWithSpan => { self.constraint.encode_as_functor(context) }
        StructureSortVariable => {
            match self {
                Self::Ascending{ variable } => encode_functor_impl!(context, Asc { variable, }),
                Self::Descending{ variable } => encode_functor_impl!(context, Desc { variable, }),
            }
        }
    ]);

    impl_functor_for!(enum FunctionReturnStructure [
        Stream { variables, } |
        Single { selector, variables, }  |
        Check { }  |
        Reduce { reducers, } |
    ]);
}
