/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;

use answer::variable::Variable;
use compiler::query_structure::QueryStructure;
use concept::{error::ConceptReadError, type_::type_manager::TypeManager};
use encoding::value::{label::Label, value::Value};
use ir::pattern::{
    constraint::{Constraint, IsaKind, SubKind},
    ParameterID, Vertex,
};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use storage::snapshot::ReadableSnapshot;

use crate::service::http::message::query::concept::{
    encode_type_concept, encode_value, RoleTypeResponse, ValueResponse,
};

struct QueryStructureContext<'a, Snapshot: ReadableSnapshot> {
    query_structure: &'a QueryStructure,
    snapshot: &'a Snapshot,
    type_manager: &'a TypeManager,
    role_names: HashMap<Variable, String>,
}

impl<'a, Snapshot: ReadableSnapshot> QueryStructureContext<'a, Snapshot> {
    pub fn get_parameter_value(&self, param: &ParameterID) -> Option<Value<'static>> {
        debug_assert!(matches!(param, ParameterID::Value(_, _)));
        self.query_structure.parameters.value(*param).cloned()
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
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryStructureResponse {
    branches: Vec<QueryStructureBranchResponse>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryStructureBranchResponse {
    constraints: Vec<QueryStructureConstraintResponse>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "tag")]
pub enum QueryStructureConstraint {
    Isa {
        instance: QueryStructureVertexResponse,
        r#type: QueryStructureVertexResponse,
        exactness: QueryStructureConstraintExactness,
    },
    Has {
        owner: QueryStructureVertexResponse,
        attribute: QueryStructureVertexResponse,
        exactness: QueryStructureConstraintExactness,
    },
    Links {
        relation: QueryStructureVertexResponse,
        player: QueryStructureVertexResponse,
        role: QueryStructureVertexResponse,
        exactness: QueryStructureConstraintExactness,
    },

    Sub {
        subtype: QueryStructureVertexResponse,
        supertype: QueryStructureVertexResponse,
        exactness: QueryStructureConstraintExactness,
    },
    Owns {
        owner: QueryStructureVertexResponse,
        attribute: QueryStructureVertexResponse,
        exactness: QueryStructureConstraintExactness,
    },
    Relates {
        relation: QueryStructureVertexResponse,
        role: QueryStructureVertexResponse,
        exactness: QueryStructureConstraintExactness,
    },
    Plays {
        player: QueryStructureVertexResponse,
        role: QueryStructureVertexResponse,
        exactness: QueryStructureConstraintExactness,
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
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum QueryStructureConstraintExactness {
    Exact,
    Subtypes,
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
    span: Option<QueryStructureConstraintSpan>,
    #[serde(flatten)]
    constraint: QueryStructureConstraint,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase", tag = "tag")]
pub enum QueryStructureVertexResponse {
    Variable { variable: String },
    Label { r#type: serde_json::Value },
    Value(ValueResponse),
    Expression { repr: String },
    FunctionCall { repr: String },
    UnavailableVariable { variable: String },
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct QueryStructureRolePlayerResponse {
    player: QueryStructureVertexResponse,
    role: QueryStructureVertexResponse,
    span: Option<QueryStructureConstraintSpan>,
}

pub(crate) fn encode_query_structure(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    query_structure: &QueryStructure,
) -> Result<QueryStructureResponse, Box<ConceptReadError>> {
    let branches = query_structure
        .parametrised_structure
        .branches
        .iter()
        .filter_map(|branch_opt| {
            branch_opt
                .as_ref()
                .map(|branch| encode_query_structure_branch(snapshot, type_manager, &query_structure, branch))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(QueryStructureResponse { branches })
}

fn encode_query_structure_branch(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    query_structure: &QueryStructure,
    branch: &[Constraint<Variable>],
) -> Result<QueryStructureBranchResponse, Box<ConceptReadError>> {
    let mut constraints = Vec::new();
    let role_names = branch
        .iter()
        .filter_map(|constraint| constraint.as_role_name())
        .map(|rolename| (rolename.type_().as_variable().unwrap(), rolename.name().to_owned()))
        .collect();
    let context = QueryStructureContext { query_structure, snapshot, type_manager, role_names };
    branch.iter().enumerate().try_for_each(|(index, constraint)| {
        query_structure_constraint(&context, constraint, &mut constraints, index)
    })?;
    Ok(QueryStructureBranchResponse { constraints })
}

fn query_structure_constraint(
    context: &QueryStructureContext<'_, impl ReadableSnapshot>,
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
                span,
                constraint: QueryStructureConstraint::Links {
                    relation: query_structure_vertex(context, links.relation())?,
                    player: query_structure_vertex(context, links.player())?,
                    role: query_structure_role_type_as_vertex(context, links.role_type())?,
                    exactness: QueryStructureConstraintExactness::Subtypes,
                },
            });
        }
        Constraint::Has(has) => {
            constraints.push(QueryStructureConstraintResponse {
                span,
                constraint: QueryStructureConstraint::Has {
                    owner: query_structure_vertex(context, has.owner())?,
                    attribute: query_structure_vertex(context, has.attribute())?,
                    exactness: QueryStructureConstraintExactness::Subtypes,
                },
            });
        }
        Constraint::Isa(isa) => {
            let exactness = match isa.isa_kind() {
                IsaKind::Exact => QueryStructureConstraintExactness::Exact,
                IsaKind::Subtype => QueryStructureConstraintExactness::Subtypes,
            };
            constraints.push(QueryStructureConstraintResponse {
                span,
                constraint: QueryStructureConstraint::Isa {
                    instance: query_structure_vertex(context, isa.thing())?,
                    r#type: query_structure_vertex(context, isa.type_())?,
                    exactness,
                },
            })
        }
        Constraint::Sub(sub) => {
            let exactness = match sub.sub_kind() {
                SubKind::Exact => QueryStructureConstraintExactness::Exact,
                SubKind::Subtype => QueryStructureConstraintExactness::Subtypes,
            };
            constraints.push(QueryStructureConstraintResponse {
                span,
                constraint: QueryStructureConstraint::Sub {
                    subtype: query_structure_vertex(context, sub.subtype())?,
                    supertype: query_structure_vertex(context, sub.supertype())?,
                    exactness,
                },
            })
        }
        Constraint::Owns(owns) => constraints.push(QueryStructureConstraintResponse {
            span,
            constraint: QueryStructureConstraint::Owns {
                owner: query_structure_vertex(context, owns.owner())?,
                attribute: query_structure_vertex(context, owns.attribute())?,
                exactness: QueryStructureConstraintExactness::Subtypes,
            },
        }),
        Constraint::Relates(relates) => {
            constraints.push(QueryStructureConstraintResponse {
                span,
                constraint: QueryStructureConstraint::Relates {
                    relation: query_structure_vertex(context, relates.relation())?,
                    role: query_structure_vertex(context, relates.role_type())?,
                    exactness: QueryStructureConstraintExactness::Subtypes,
                },
            });
        }
        Constraint::Plays(plays) => {
            constraints.push(QueryStructureConstraintResponse {
                span,
                constraint: QueryStructureConstraint::Plays {
                    player: query_structure_vertex(context, plays.player())?,
                    role: query_structure_vertex(context, plays.role_type())?,
                    exactness: QueryStructureConstraintExactness::Subtypes,
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
                span: span_1,
                constraint: QueryStructureConstraint::Links {
                    relation: query_structure_vertex(context, indexed.relation())?,
                    player: query_structure_vertex(context, indexed.player_1())?,
                    role: query_structure_role_type_as_vertex(context, indexed.role_type_1())?,
                    exactness: QueryStructureConstraintExactness::Subtypes,
                },
            });
            constraints.push(QueryStructureConstraintResponse {
                span: span_2,
                constraint: QueryStructureConstraint::Links {
                    relation: query_structure_vertex(context, indexed.relation())?,
                    player: query_structure_vertex(context, indexed.player_2())?,
                    role: query_structure_role_type_as_vertex(context, indexed.role_type_2())?,
                    exactness: QueryStructureConstraintExactness::Subtypes,
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
                span,
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
                span,
                constraint: QueryStructureConstraint::FunctionCall { name: text, assigned, arguments },
            });
        }
        | Constraint::Comparison(_) => {}
        Constraint::RoleName(_) => {} // Handled separately via resolved_role_names

        // Constraints that probably don't need to be handled
        | Constraint::Kind(_)
        | Constraint::Label(_)
        | Constraint::Value(_)
        | Constraint::Is(_)
        | Constraint::Iid(_) => {}
        // Optimisations don't represent the structure
        Constraint::LinksDeduplication(_) | Constraint::Unsatisfiable(_) => {}
    };
    Ok(())
}

fn query_structure_vertex(
    context: &QueryStructureContext<'_, impl ReadableSnapshot>,
    vertex: &Vertex<Variable>,
) -> Result<QueryStructureVertexResponse, Box<ConceptReadError>> {
    let vertex = match vertex {
        Vertex::Variable(variable) => {
            let name = context.get_variable_name(variable).unwrap_or_else(|| variable.to_string());
            if context.query_structure.available_variables.contains(variable) {
                QueryStructureVertexResponse::Variable { variable: name }
            } else {
                QueryStructureVertexResponse::UnavailableVariable { variable: name }
            }
        }
        Vertex::Label(label) => {
            let type_ = context.get_type(label).unwrap();
            QueryStructureVertexResponse::Label {
                r#type: serde_json::json!(encode_type_concept(
                    &type_,
                    context.snapshot,
                    context.type_manager
                )?)
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
    context: &QueryStructureContext<'_, impl ReadableSnapshot>,
    role_type: &Vertex<Variable>,
) -> Result<QueryStructureVertexResponse, Box<ConceptReadError>> {
    if let Some(label) = context.get_role_type(&role_type.as_variable().unwrap()) {
        // At present rolename could resolve to multiple types - Manually encode.
        Ok(QueryStructureVertexResponse::Label { r#type: serde_json::json!(RoleTypeResponse { label: label.to_owned() }) })
    } else {
        query_structure_vertex(context, role_type)
    }
}
