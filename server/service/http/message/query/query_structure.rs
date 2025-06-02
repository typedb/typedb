/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, marker::PhantomData, str::FromStr};

use answer::variable::Variable;
use bytes::util::HexBytesFormatter;
use compiler::query_structure::QueryStructure;
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

struct QueryStructureContext<'a, Snapshot: ReadableSnapshot> {
    query_structure: &'a QueryStructure,
    snapshot: &'a Snapshot,
    type_manager: &'a TypeManager,
    role_names: HashMap<Variable, String>,
    variables: &'a mut HashMap<QueryVariableId, QueryVariableInfo>,
}

impl<'a, Snapshot: ReadableSnapshot> QueryStructureContext<'a, Snapshot> {
    pub fn get_parameter_value(&self, param: &ParameterID) -> Option<Value<'static>> {
        debug_assert!(matches!(param, ParameterID::Value(_, _)));
        self.query_structure.parameters.value(*param).cloned()
    }

    pub fn get_parameter_iid(&self, param: &ParameterID) -> Option<&[u8]> {
        self.query_structure.parameters.iid(*param).map(|iid| iid.as_ref())
    }

    pub fn get_variable_name(&self, variable: &Variable) -> Option<String> {
        self.query_structure.variable_names.get(&variable).cloned()
    }

    pub fn get_type(&self, label: &Label) -> Option<answer::Type> {
        self.query_structure.parametrised_structure.resolved_labels.get(label).cloned()
    }

    fn get_call_syntax(&self, constraint: &Constraint<Variable>) -> Option<&String> {
        self.query_structure.parametrised_structure.calls_syntax.get(constraint)
    }

    fn get_role_type(&self, variable: &Variable) -> Option<&str> {
        self.role_names.get(variable).map(|name| name.as_str())
    }

    fn record_variable(&mut self, variable: &Variable) {
        let id = variable.into();
        if !self.variables.contains_key(&id) {
            self.variables.insert(id, QueryVariableInfo { name: self.get_variable_name(&variable) });
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryStructureResponse {
    blocks: Vec<QueryStructureBlockResponse>,
    variables: HashMap<QueryVariableId, QueryVariableInfo>,
    outputs: Vec<QueryVariableId>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, Hash, PartialEq)]
struct QueryVariableId(
    #[serde(serialize_with = "serialize_using_to_string")]
    #[serde(deserialize_with = "deserialize_using_from_string")]
    u16,
);

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryVariableInfo {
    name: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryStructureBlockResponse {
    constraints: Vec<QueryStructureConstraintResponse>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "tag")]
pub enum QueryStructureConstraint {
    Isa(QueryConstraintIsaBase),
    #[serde(rename = "isa!")]
    IsaExact(QueryConstraintIsaBase),
    Has {
        owner: QueryStructureVertexResponse,
        attribute: QueryStructureVertexResponse,
    },
    Links {
        relation: QueryStructureVertexResponse,
        player: QueryStructureVertexResponse,
        role: QueryStructureVertexResponse,
    },

    Sub(QueryConstraintSubBase),
    #[serde(rename = "sub!")]
    SubExact(QueryConstraintSubBase),
    Owns {
        owner: QueryStructureVertexResponse,
        attribute: QueryStructureVertexResponse,
    },
    Relates {
        relation: QueryStructureVertexResponse,
        role: QueryStructureVertexResponse,
    },
    Plays {
        player: QueryStructureVertexResponse,
        role: QueryStructureVertexResponse,
    },

    FunctionCall {
        name: String,
        assigned: Vec<QueryStructureVertexResponse>,
        arguments: Vec<QueryStructureVertexResponse>,
    },
    Expression {
        text: String,
        assigned: Vec<QueryStructureVertexResponse>,
        arguments: Vec<QueryStructureVertexResponse>,
    },
    Is {
        lhs: QueryStructureVertexResponse,
        rhs: QueryStructureVertexResponse,
    },
    Iid {
        concept: QueryStructureVertexResponse,
        iid: String,
    },
    Comparison {
        lhs: QueryStructureVertexResponse,
        rhs: QueryStructureVertexResponse,
        comparator: String,
    },
    Kind {
        kind: String,
        r#type: QueryStructureVertexResponse,
    },
    Label {
        r#type: QueryStructureVertexResponse,
        label: String,
    },
    Value {
        #[serde(rename = "attributeType")]
        attribute_type: QueryStructureVertexResponse,
        #[serde(rename = "valueType")]
        value_type: String,
    },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct QueryConstraintIsaBase {
    instance: QueryStructureVertexResponse,
    r#type: QueryStructureVertexResponse,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct QueryConstraintSubBase {
    subtype: QueryStructureVertexResponse,
    supertype: QueryStructureVertexResponse,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct QueryStructureConstraintSpan {
    begin: usize,
    end: usize,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryStructureConstraintResponse {
    text_span: Option<QueryStructureConstraintSpan>,
    #[serde(flatten)]
    constraint: QueryStructureConstraint,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase", tag = "tag")]
pub enum QueryStructureVertexResponse {
    Variable { id: QueryVariableId },
    Label { r#type: serde_json::Value },
    Value(ValueResponse),
}

pub(crate) fn encode_query_structure(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    query_structure: &QueryStructure,
) -> Result<QueryStructureResponse, Box<ConceptReadError>> {
    let mut variables = HashMap::new();
    let blocks = query_structure
        .parametrised_structure
        .branches
        .iter()
        .filter_map(|branch_opt| {
            branch_opt.as_ref().map(|branch| {
                encode_query_structure_block(snapshot, type_manager, &query_structure, &mut variables, branch)
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let output_variables = query_structure.available_variables.iter().map(|v| v.into()).collect();
    Ok(QueryStructureResponse { blocks, variables: variables, outputs: output_variables })
}

fn encode_query_structure_block(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    query_structure: &QueryStructure,
    variables: &mut HashMap<QueryVariableId, QueryVariableInfo>,
    block: &[Constraint<Variable>],
) -> Result<QueryStructureBlockResponse, Box<ConceptReadError>> {
    let mut constraints = Vec::new();
    let role_names = block
        .iter()
        .filter_map(|constraint| constraint.as_role_name())
        .map(|rolename| (rolename.type_().as_variable().unwrap(), rolename.name().to_owned()))
        .collect();
    let mut context = QueryStructureContext { query_structure, snapshot, type_manager, role_names, variables };
    block.iter().enumerate().try_for_each(|(index, constraint)| {
        query_structure_constraint(&mut context, constraint, &mut constraints, index)
    })?;
    Ok(QueryStructureBlockResponse { constraints })
}

fn query_structure_constraint(
    context: &mut QueryStructureContext<'_, impl ReadableSnapshot>,
    constraint: &Constraint<Variable>,
    constraints: &mut Vec<QueryStructureConstraintResponse>,
    index: usize,
) -> Result<(), Box<ConceptReadError>> {
    let span = constraint
        .source_span()
        .map(|span| QueryStructureConstraintSpan { begin: span.begin_offset, end: span.end_offset });
    match constraint {
        Constraint::Links(links) => {
            constraints.push(QueryStructureConstraintResponse {
                text_span: span,
                constraint: QueryStructureConstraint::Links {
                    relation: query_structure_vertex(context, links.relation())?,
                    player: query_structure_vertex(context, links.player())?,
                    role: query_structure_role_type_as_vertex(context, links.role_type())?,
                },
            });
        }
        Constraint::Has(has) => {
            constraints.push(QueryStructureConstraintResponse {
                text_span: span,
                constraint: QueryStructureConstraint::Has {
                    owner: query_structure_vertex(context, has.owner())?,
                    attribute: query_structure_vertex(context, has.attribute())?,
                },
            });
        }

        Constraint::Isa(isa) => {
            let constraint = QueryConstraintIsaBase {
                instance: query_structure_vertex(context, isa.thing())?,
                r#type: query_structure_vertex(context, isa.type_())?,
            };
            constraints.push(QueryStructureConstraintResponse {
                text_span: span,
                constraint: match isa.isa_kind() {
                    IsaKind::Exact => QueryStructureConstraint::IsaExact(constraint),
                    IsaKind::Subtype => QueryStructureConstraint::Isa(constraint),
                },
            })
        }
        Constraint::Sub(sub) => {
            let constraint = QueryConstraintSubBase {
                subtype: query_structure_vertex(context, sub.subtype())?,
                supertype: query_structure_vertex(context, sub.supertype())?,
            };
            constraints.push(QueryStructureConstraintResponse {
                text_span: span,
                constraint: match sub.sub_kind() {
                    SubKind::Exact => QueryStructureConstraint::SubExact(constraint),
                    SubKind::Subtype => QueryStructureConstraint::Sub(constraint),
                },
            })
        }
        Constraint::Owns(owns) => constraints.push(QueryStructureConstraintResponse {
            text_span: span,
            constraint: QueryStructureConstraint::Owns {
                owner: query_structure_vertex(context, owns.owner())?,
                attribute: query_structure_vertex(context, owns.attribute())?,
            },
        }),
        Constraint::Relates(relates) => {
            constraints.push(QueryStructureConstraintResponse {
                text_span: span,
                constraint: QueryStructureConstraint::Relates {
                    relation: query_structure_vertex(context, relates.relation())?,
                    role: query_structure_vertex(context, relates.role_type())?,
                },
            });
        }
        Constraint::Plays(plays) => {
            constraints.push(QueryStructureConstraintResponse {
                text_span: span,
                constraint: QueryStructureConstraint::Plays {
                    player: query_structure_vertex(context, plays.player())?,
                    role: query_structure_vertex(context, plays.role_type())?,
                },
            });
        }
        Constraint::IndexedRelation(indexed) => {
            let span_1 = indexed
                .source_span_1()
                .map(|span| QueryStructureConstraintSpan { begin: span.begin_offset, end: span.end_offset });
            let span_2 = indexed
                .source_span_2()
                .map(|span| QueryStructureConstraintSpan { begin: span.begin_offset, end: span.end_offset });
            constraints.push(QueryStructureConstraintResponse {
                text_span: span_1,
                constraint: QueryStructureConstraint::Links {
                    relation: query_structure_vertex(context, indexed.relation())?,
                    player: query_structure_vertex(context, indexed.player_1())?,
                    role: query_structure_role_type_as_vertex(context, indexed.role_type_1())?,
                },
            });
            constraints.push(QueryStructureConstraintResponse {
                text_span: span_2,
                constraint: QueryStructureConstraint::Links {
                    relation: query_structure_vertex(context, indexed.relation())?,
                    player: query_structure_vertex(context, indexed.player_2())?,
                    role: query_structure_role_type_as_vertex(context, indexed.role_type_2())?,
                },
            });
        }
        Constraint::ExpressionBinding(expr) => {
            let text =
                context.get_call_syntax(constraint).map_or_else(|| format!("Expression#{index}"), |text| text.clone());
            let assigned = expr
                .ids_assigned()
                .map(|variable| query_structure_vertex(context, &Vertex::Variable(variable)))
                .collect::<Result<Vec<_>, _>>()?;
            let arguments = expr
                .required_ids()
                .map(|variable| query_structure_vertex(context, &Vertex::Variable(variable)))
                .collect::<Result<Vec<_>, _>>()?;
            constraints.push(QueryStructureConstraintResponse {
                text_span: span,
                constraint: QueryStructureConstraint::Expression { text, assigned, arguments },
            });
        }
        Constraint::FunctionCallBinding(function_call) => {
            let text =
                context.get_call_syntax(constraint).map_or_else(|| format!("Function#{index}"), |text| text.clone());
            let assigned = function_call
                .ids_assigned()
                .map(|variable| query_structure_vertex(context, &Vertex::Variable(variable)))
                .collect::<Result<Vec<_>, _>>()?;
            let arguments = function_call
                .function_call()
                .argument_ids()
                .map(|variable| query_structure_vertex(context, &Vertex::Variable(variable)))
                .collect::<Result<Vec<_>, _>>()?;
            constraints.push(QueryStructureConstraintResponse {
                text_span: span,
                constraint: QueryStructureConstraint::FunctionCall { name: text, assigned, arguments },
            });
        }
        Constraint::Is(is) => {
            constraints.push(QueryStructureConstraintResponse {
                text_span: span,
                constraint: QueryStructureConstraint::Is {
                    lhs: query_structure_vertex(context, is.lhs())?,
                    rhs: query_structure_vertex(context, is.rhs())?,
                },
            });
        }
        Constraint::Iid(iid) => {
            let iid_bytes = context.get_parameter_iid(iid.iid().as_parameter().as_ref().unwrap()).unwrap();
            let iid_hex = HexBytesFormatter::borrowed(iid_bytes).format_iid();
            constraints.push(QueryStructureConstraintResponse {
                text_span: span,
                constraint: QueryStructureConstraint::Iid {
                    concept: query_structure_vertex(context, iid.var())?,
                    iid: iid_hex,
                },
            });
        }
        Constraint::Comparison(comparison) => constraints.push(QueryStructureConstraintResponse {
            text_span: span,
            constraint: QueryStructureConstraint::Comparison {
                lhs: query_structure_vertex(context, comparison.lhs())?,
                rhs: query_structure_vertex(context, comparison.lhs())?,
                comparator: comparison.comparator().name().to_owned(),
            },
        }),
        Constraint::Kind(kind) => constraints.push(QueryStructureConstraintResponse {
            text_span: span,
            constraint: QueryStructureConstraint::Kind {
                kind: kind.kind().to_string(),
                r#type: query_structure_vertex(context, kind.type_())?,
            },
        }),
        Constraint::Label(label) => constraints.push(QueryStructureConstraintResponse {
            text_span: span,
            constraint: QueryStructureConstraint::Label {
                r#type: query_structure_vertex(context, label.type_())?,
                label: label
                    .type_label()
                    .as_label()
                    .expect("Expected constant label in label constraint")
                    .scoped_name()
                    .as_str()
                    .to_owned(),
            },
        }),
        Constraint::Value(value) => constraints.push(QueryStructureConstraintResponse {
            text_span: span,
            constraint: QueryStructureConstraint::Value {
                attribute_type: query_structure_vertex(context, value.attribute_type())?,
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

fn query_structure_vertex(
    context: &mut QueryStructureContext<'_, impl ReadableSnapshot>,
    vertex: &Vertex<Variable>,
) -> Result<QueryStructureVertexResponse, Box<ConceptReadError>> {
    let vertex = match vertex {
        Vertex::Variable(variable) => {
            context.record_variable(variable);
            QueryStructureVertexResponse::Variable { id: variable.into() }
        }
        Vertex::Label(label) => {
            let type_ = context.get_type(label).unwrap();
            QueryStructureVertexResponse::Label {
                r#type: serde_json::json!(encode_type_concept(&type_, context.snapshot, context.type_manager)?),
            }
        }
        Vertex::Parameter(param) => {
            let value = context.get_parameter_value(param).unwrap();
            QueryStructureVertexResponse::Value(encode_value(value))
        }
    };
    Ok(vertex)
}

fn query_structure_role_type_as_vertex(
    context: &mut QueryStructureContext<'_, impl ReadableSnapshot>,
    role_type: &Vertex<Variable>,
) -> Result<QueryStructureVertexResponse, Box<ConceptReadError>> {
    if let Some(label) = context.get_role_type(&role_type.as_variable().unwrap()) {
        // At present rolename could resolve to multiple types - Manually encode.
        Ok(QueryStructureVertexResponse::Label {
            r#type: serde_json::json!(RoleTypeResponse { label: label.to_owned() }),
        })
    } else {
        query_structure_vertex(context, role_type)
    }
}

impl From<&Variable> for QueryVariableId {
    fn from(value: &Variable) -> Self {
        Self(value.id().as_u16())
    }
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
