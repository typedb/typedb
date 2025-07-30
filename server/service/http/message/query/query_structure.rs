/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, str::FromStr};

use answer::variable::Variable;
use bytes::util::HexBytesFormatter;
use compiler::query_structure::{
    ParametrisedPipelineStructure, PipelineStructure, QueryStructure, QueryStructureStage, StructureVariableId,
};
use concept::{error::ConceptReadError, type_::type_manager::TypeManager};
use encoding::value::{label::Label, value::Value};
use ir::pattern::{
    constraint::{Constraint, IsaKind, SubKind},
    ParameterID, Vertex,
};
use serde::{Deserialize, Serialize, Serializer};
use storage::snapshot::ReadableSnapshot;

use crate::service::http::message::query::concept::{
    encode_type_concept, encode_value, RoleTypeResponse, ValueResponse,
};

struct PipelineStructureContext<'a, Snapshot: ReadableSnapshot> {
    pipeline_structure: &'a PipelineStructure,
    snapshot: &'a Snapshot,
    type_manager: &'a TypeManager,
    role_names: HashMap<Variable, String>,
    variables: &'a mut HashMap<StructureVariableId, StructureVariableInfo>,
}

impl<'a, Snapshot: ReadableSnapshot> PipelineStructureContext<'a, Snapshot> {
    pub fn get_parameter_value(&self, param: &ParameterID) -> Option<Value<'static>> {
        debug_assert!(matches!(param, ParameterID::Value(_, _)));
        self.pipeline_structure.parameters.value(*param).cloned()
    }

    pub fn get_parameter_iid(&self, param: &ParameterID) -> Option<&[u8]> {
        self.pipeline_structure.parameters.iid(*param).map(|iid| iid.as_ref())
    }

    pub fn get_variable_name(&self, variable: &StructureVariableId) -> Option<String> {
        self.pipeline_structure.variable_names.get(&variable).cloned()
    }

    pub fn get_type(&self, label: &Label) -> Option<answer::Type> {
        self.pipeline_structure.parametrised_structure.resolved_labels.get(label).cloned()
    }

    fn get_call_syntax(&self, constraint: &Constraint<Variable>) -> Option<&String> {
        self.pipeline_structure.parametrised_structure.calls_syntax.get(constraint)
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

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct FunctionStructureResponse {
    pipeline: Option<PipelineStructureResponse>,
    // TODO: arguments, returned,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct QueryStructureResponse {
    pipeline: Option<PipelineStructureResponse>,
    preamble: Vec<FunctionStructureResponse>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PipelineStructureResponse {
    blocks: Vec<StructureBlock>,
    stages: Vec<QueryStructureStage>,
    variables: HashMap<StructureVariableId, StructureVariableInfo>,
    outputs: Vec<StructureVariableId>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StructureVariableInfo {
    name: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct StructureBlock {
    constraints: Vec<StructureConstraintWithSpan>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "tag")]
enum StructureConstraint {
    Isa(StructureConstraintIsaBase),
    #[serde(rename = "isa!")]
    IsaExact(StructureConstraintIsaBase),
    Has {
        owner: StructureVertex,
        attribute: StructureVertex,
    },
    Links {
        relation: StructureVertex,
        player: StructureVertex,
        role: StructureVertex,
    },

    Sub(StructureConstraintSubBase),
    #[serde(rename = "sub!")]
    SubExact(StructureConstraintSubBase),
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
        assigned: Vec<StructureVertex>,
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
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct StructureConstraintIsaBase {
    instance: StructureVertex,
    r#type: StructureVertex,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct StructureConstraintSubBase {
    subtype: StructureVertex,
    supertype: StructureVertex,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct StructureConstraintSpan {
    begin: usize,
    end: usize,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct StructureConstraintWithSpan {
    text_span: Option<StructureConstraintSpan>,
    #[serde(flatten)]
    constraint: StructureConstraint,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase", tag = "tag")]
enum StructureVertex {
    Variable { id: StructureVariableId },
    Label { r#type: serde_json::Value },
    Value(ValueResponse),
}

pub(crate) fn encode_query_structure(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    query_structure: QueryStructure,
) -> Result<QueryStructureResponse, Box<ConceptReadError>> {
    let QueryStructure { preamble, pipeline } = query_structure;
    let pipeline =
        pipeline.as_ref().map(|pipeline| encode_pipeline_structure(snapshot, type_manager, &pipeline)).transpose()?;
    let preamble = preamble
        .into_iter()
        .map(|function| {
            let pipeline = function
                .pipeline
                .as_ref()
                .map(|pipeline| encode_pipeline_structure(snapshot, type_manager, pipeline))
                .transpose()?;
            Ok::<_, Box<ConceptReadError>>(FunctionStructureResponse { pipeline })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(QueryStructureResponse { pipeline, preamble })
}

pub(crate) fn encode_pipeline_structure(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    pipeline_structure: &PipelineStructure,
) -> Result<PipelineStructureResponse, Box<ConceptReadError>> {
    let mut variables = HashMap::new();
    let ParametrisedPipelineStructure { stages, blocks, .. } = &*pipeline_structure.parametrised_structure;
    let blocks = blocks
        .iter()
        .map(|block| {
            encode_structure_block(
                snapshot,
                type_manager,
                &pipeline_structure,
                &mut variables,
                block.constraints.as_slice(),
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    // Ensure reduced variables are added to variables
    record_reducer_variables(snapshot, type_manager, pipeline_structure, &mut variables);
    let outputs = pipeline_structure.available_variables.clone();
    Ok(PipelineStructureResponse { blocks, outputs, variables, stages: stages.clone() })
}

fn record_reducer_variables(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    pipeline_structure: &PipelineStructure,
    variables: &mut HashMap<StructureVariableId, StructureVariableInfo>,
) {
    let mut context =
        PipelineStructureContext { pipeline_structure, snapshot, type_manager, role_names: HashMap::new(), variables };
    pipeline_structure
        .parametrised_structure
        .stages
        .iter()
        .filter_map(|stage| match stage {
            QueryStructureStage::Reduce { reducers, .. } => Some(reducers),
            _ => None,
        })
        .for_each(|reducers| {
            reducers.iter().for_each(|reducer| context.record_variable(reducer.assigned));
        });
}

fn encode_structure_block(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    pipeline_structure: &PipelineStructure,
    variables: &mut HashMap<StructureVariableId, StructureVariableInfo>,
    block: &[Constraint<Variable>],
) -> Result<StructureBlock, Box<ConceptReadError>> {
    let mut constraints = Vec::new();
    let role_names = block
        .iter()
        .filter_map(|constraint| constraint.as_role_name())
        .map(|rolename| (rolename.type_().as_variable().unwrap(), rolename.name().to_owned()))
        .collect();
    let mut context = PipelineStructureContext { pipeline_structure, snapshot, type_manager, role_names, variables };
    block.iter().enumerate().try_for_each(|(index, constraint)| {
        encode_structure_constraint(&mut context, constraint, &mut constraints, index)
    })?;
    Ok(StructureBlock { constraints })
}

fn encode_structure_constraint(
    context: &mut PipelineStructureContext<'_, impl ReadableSnapshot>,
    constraint: &Constraint<Variable>,
    constraints: &mut Vec<StructureConstraintWithSpan>,
    index: usize,
) -> Result<(), Box<ConceptReadError>> {
    let span =
        constraint.source_span().map(|span| StructureConstraintSpan { begin: span.begin_offset, end: span.end_offset });
    match constraint {
        Constraint::Links(links) => {
            constraints.push(StructureConstraintWithSpan {
                text_span: span,
                constraint: StructureConstraint::Links {
                    relation: encode_structure_vertex(context, links.relation())?,
                    player: encode_structure_vertex(context, links.player())?,
                    role: encode_role_type_as_vertex(context, links.role_type())?,
                },
            });
        }
        Constraint::Has(has) => {
            constraints.push(StructureConstraintWithSpan {
                text_span: span,
                constraint: StructureConstraint::Has {
                    owner: encode_structure_vertex(context, has.owner())?,
                    attribute: encode_structure_vertex(context, has.attribute())?,
                },
            });
        }

        Constraint::Isa(isa) => {
            let constraint = StructureConstraintIsaBase {
                instance: encode_structure_vertex(context, isa.thing())?,
                r#type: encode_structure_vertex(context, isa.type_())?,
            };
            constraints.push(StructureConstraintWithSpan {
                text_span: span,
                constraint: match isa.isa_kind() {
                    IsaKind::Exact => StructureConstraint::IsaExact(constraint),
                    IsaKind::Subtype => StructureConstraint::Isa(constraint),
                },
            })
        }
        Constraint::Sub(sub) => {
            let constraint = StructureConstraintSubBase {
                subtype: encode_structure_vertex(context, sub.subtype())?,
                supertype: encode_structure_vertex(context, sub.supertype())?,
            };
            constraints.push(StructureConstraintWithSpan {
                text_span: span,
                constraint: match sub.sub_kind() {
                    SubKind::Exact => StructureConstraint::SubExact(constraint),
                    SubKind::Subtype => StructureConstraint::Sub(constraint),
                },
            })
        }
        Constraint::Owns(owns) => constraints.push(StructureConstraintWithSpan {
            text_span: span,
            constraint: StructureConstraint::Owns {
                owner: encode_structure_vertex(context, owns.owner())?,
                attribute: encode_structure_vertex(context, owns.attribute())?,
            },
        }),
        Constraint::Relates(relates) => {
            constraints.push(StructureConstraintWithSpan {
                text_span: span,
                constraint: StructureConstraint::Relates {
                    relation: encode_structure_vertex(context, relates.relation())?,
                    role: encode_structure_vertex(context, relates.role_type())?,
                },
            });
        }
        Constraint::Plays(plays) => {
            constraints.push(StructureConstraintWithSpan {
                text_span: span,
                constraint: StructureConstraint::Plays {
                    player: encode_structure_vertex(context, plays.player())?,
                    role: encode_structure_vertex(context, plays.role_type())?,
                },
            });
        }
        Constraint::IndexedRelation(indexed) => {
            let span_1 = indexed
                .source_span_1()
                .map(|span| StructureConstraintSpan { begin: span.begin_offset, end: span.end_offset });
            let span_2 = indexed
                .source_span_2()
                .map(|span| StructureConstraintSpan { begin: span.begin_offset, end: span.end_offset });
            constraints.push(StructureConstraintWithSpan {
                text_span: span_1,
                constraint: StructureConstraint::Links {
                    relation: encode_structure_vertex(context, indexed.relation())?,
                    player: encode_structure_vertex(context, indexed.player_1())?,
                    role: encode_role_type_as_vertex(context, indexed.role_type_1())?,
                },
            });
            constraints.push(StructureConstraintWithSpan {
                text_span: span_2,
                constraint: StructureConstraint::Links {
                    relation: encode_structure_vertex(context, indexed.relation())?,
                    player: encode_structure_vertex(context, indexed.player_2())?,
                    role: encode_role_type_as_vertex(context, indexed.role_type_2())?,
                },
            });
        }
        Constraint::ExpressionBinding(expr) => {
            let text =
                context.get_call_syntax(constraint).map_or_else(|| format!("Expression#{index}"), |text| text.clone());
            let assigned = expr
                .ids_assigned()
                .map(|variable| encode_structure_vertex(context, &Vertex::Variable(variable)))
                .collect::<Result<Vec<_>, _>>()?;
            let arguments = expr
                .expression_ids()
                .map(|variable| encode_structure_vertex(context, &Vertex::Variable(variable)))
                .collect::<Result<Vec<_>, _>>()?;
            constraints.push(StructureConstraintWithSpan {
                text_span: span,
                constraint: StructureConstraint::Expression { text, assigned, arguments },
            });
        }
        Constraint::FunctionCallBinding(function_call) => {
            let text =
                context.get_call_syntax(constraint).map_or_else(|| format!("Function#{index}"), |text| text.clone());
            let assigned = function_call
                .ids_assigned()
                .map(|variable| encode_structure_vertex(context, &Vertex::Variable(variable)))
                .collect::<Result<Vec<_>, _>>()?;
            let arguments = function_call
                .function_call()
                .argument_ids()
                .map(|variable| encode_structure_vertex(context, &Vertex::Variable(variable)))
                .collect::<Result<Vec<_>, _>>()?;
            constraints.push(StructureConstraintWithSpan {
                text_span: span,
                constraint: StructureConstraint::FunctionCall { name: text, assigned, arguments },
            });
        }
        Constraint::Is(is) => {
            constraints.push(StructureConstraintWithSpan {
                text_span: span,
                constraint: StructureConstraint::Is {
                    lhs: encode_structure_vertex(context, is.lhs())?,
                    rhs: encode_structure_vertex(context, is.rhs())?,
                },
            });
        }
        Constraint::Iid(iid) => {
            let iid_bytes = context.get_parameter_iid(iid.iid().as_parameter().as_ref().unwrap()).unwrap();
            let iid_hex = HexBytesFormatter::borrowed(iid_bytes).format_iid();
            constraints.push(StructureConstraintWithSpan {
                text_span: span,
                constraint: StructureConstraint::Iid {
                    concept: encode_structure_vertex(context, iid.var())?,
                    iid: iid_hex,
                },
            });
        }
        Constraint::Comparison(comparison) => constraints.push(StructureConstraintWithSpan {
            text_span: span,
            constraint: StructureConstraint::Comparison {
                lhs: encode_structure_vertex(context, comparison.lhs())?,
                rhs: encode_structure_vertex(context, comparison.lhs())?,
                comparator: comparison.comparator().name().to_owned(),
            },
        }),
        Constraint::Kind(kind) => constraints.push(StructureConstraintWithSpan {
            text_span: span,
            constraint: StructureConstraint::Kind {
                kind: kind.kind().to_string(),
                r#type: encode_structure_vertex(context, kind.type_())?,
            },
        }),
        Constraint::Label(label) => constraints.push(StructureConstraintWithSpan {
            text_span: span,
            constraint: StructureConstraint::Label {
                r#type: encode_structure_vertex(context, label.type_())?,
                label: label
                    .type_label()
                    .as_label()
                    .expect("Expected constant label in label constraint")
                    .scoped_name()
                    .as_str()
                    .to_owned(),
            },
        }),
        Constraint::Value(value) => constraints.push(StructureConstraintWithSpan {
            text_span: span,
            constraint: StructureConstraint::Value {
                attribute_type: encode_structure_vertex(context, value.attribute_type())?,
                value_type: value.value_type().to_string(),
            },
        }),
        // Constraints that probably don't need to be handled
        Constraint::RoleName(_) => {} // Handled separately via resolved_role_names
        // Optimisations don't represent the structure
        Constraint::LinksDeduplication(_) | Constraint::Unsatisfiable(_) => {}
    };
    Ok(())
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
            let type_ = context.get_type(label).unwrap();
            StructureVertex::Label {
                r#type: serde_json::json!(encode_type_concept(&type_, context.snapshot, context.type_manager)?),
            }
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
    if let Some(label) = context.get_role_type(&role_type.as_variable().unwrap()) {
        // At present rolename could resolve to multiple types - Manually encode.
        Ok(StructureVertex::Label { r#type: serde_json::json!(RoleTypeResponse { label: label.to_owned() }) })
    } else {
        encode_structure_vertex(context, role_type)
    }
}
