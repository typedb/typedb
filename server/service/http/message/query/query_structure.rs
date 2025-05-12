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
    edges: Vec<QueryStructureEdgeResponse>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "kind", content = "param")]
pub enum QueryStructureEdgeTypeResponse {
    Isa,
    Has,
    Links(QueryStructureVertexResponse),

    Sub,
    Owns,
    Relates,
    Plays,

    IsaExact,
    SubExact,

    Assigned(String),
    Argument(String),
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct QueryStructureEdgeSpan {
    begin: usize,
    end: usize,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryStructureEdgeResponse {
    r#type: QueryStructureEdgeTypeResponse,
    from: QueryStructureVertexResponse,
    to: QueryStructureVertexResponse,
    span: Option<QueryStructureEdgeSpan>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase", tag = "kind", content = "value")]
pub enum QueryStructureVertexResponse {
    Variable { variable: String },
    Label(serde_json::Value),
    Value(ValueResponse),
    Expression { repr: String },
    FunctionCall { repr: String },
    UnavailableVariable { variable: String },
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
    let mut edges = Vec::new();
    let role_names = branch
        .iter()
        .filter_map(|constraint| constraint.as_role_name())
        .map(|rolename| (rolename.type_().as_variable().unwrap(), rolename.name().to_owned()))
        .collect();
    let context = QueryStructureContext { query_structure, snapshot, type_manager, role_names };
    branch
        .iter()
        .enumerate()
        .try_for_each(|(index, constraint)| query_structure_edge(&context, constraint, &mut edges, index))?;
    Ok(QueryStructureBranchResponse { edges })
}

macro_rules! push_edge {
    ($edges:ident, $query_structure:expr, $span:expr, $from:expr, $to:expr, $variant:ident) => {{
        let from = query_structure_vertex($query_structure, $from)?;
        let to = query_structure_vertex($query_structure, $to)?;
        $edges.push(QueryStructureEdgeResponse {
            r#type: QueryStructureEdgeTypeResponse::$variant,
            from,
            to,
            span: $span,
        });
    }};
    ($edges:ident, $query_structure:expr, $span:expr, $from:expr, $to:expr, $variant:ident($param:expr)) => {{
        let from = query_structure_vertex($query_structure, $from)?;
        let to = query_structure_vertex($query_structure, $to)?;
        $edges.push(QueryStructureEdgeResponse {
            r#type: QueryStructureEdgeTypeResponse::$variant($param),
            from,
            to,
            span: $span,
        });
    }};
}

fn query_structure_edge(
    context: &QueryStructureContext<'_, impl ReadableSnapshot>,
    constraint: &Constraint<Variable>,
    edges: &mut Vec<QueryStructureEdgeResponse>,
    index: usize,
) -> Result<(), Box<ConceptReadError>> {
    let span =
        constraint.source_span().map(|span| QueryStructureEdgeSpan { begin: span.begin_offset, end: span.end_offset });
    match constraint {
        Constraint::Links(links) => {
            let role_type = query_structure_role_type_as_vertex(context, links.role_type())?;
            push_edge!(edges, context, span, links.relation(), links.player(), Links(role_type))
        }
        Constraint::Has(has) => push_edge!(edges, context, span, has.owner(), has.attribute(), Has),
        Constraint::Isa(isa) => match isa.isa_kind() {
            IsaKind::Exact => push_edge!(edges, context, span, isa.thing(), isa.type_(), IsaExact),
            IsaKind::Subtype => push_edge!(edges, context, span, isa.thing(), isa.type_(), Isa),
        },
        Constraint::Sub(sub) => match sub.sub_kind() {
            SubKind::Exact => push_edge!(edges, context, span, sub.subtype(), sub.supertype(), SubExact),
            SubKind::Subtype => push_edge!(edges, context, span, sub.subtype(), sub.supertype(), Sub),
        },
        Constraint::Owns(owns) => push_edge!(edges, context, span, owns.owner(), owns.attribute(), Owns),
        Constraint::Relates(relates) => {
            push_edge!(edges, context, span, relates.relation(), relates.role_type(), Relates)
        }
        Constraint::Plays(plays) => push_edge!(edges, context, span, plays.player(), plays.role_type(), Plays),
        Constraint::ExpressionBinding(expr) => {
            let repr =
                context.get_call_syntax(constraint).map_or_else(|| format!("Expression#{index}"), |text| text.clone());
            let expr_vertex = QueryStructureVertexResponse::Expression { repr };
            expr.ids_assigned().try_for_each(|variable| {
                let assigned = query_structure_vertex(context, &Vertex::Variable(variable))?;
                let assigned_name = context.get_variable_name(&variable).unwrap_or_else(|| variable.to_string());
                let edge_type = QueryStructureEdgeTypeResponse::Assigned(assigned_name);
                edges.push(QueryStructureEdgeResponse {
                    r#type: edge_type,
                    from: expr_vertex.clone(),
                    to: assigned,
                    span,
                });
                Ok::<_, Box<ConceptReadError>>(())
            })?;
            expr.required_ids().try_for_each(|variable| {
                let argument = query_structure_vertex(context, &Vertex::Variable(variable))?;
                let arg_name = context.get_variable_name(&variable).unwrap_or_else(|| variable.to_string());
                let edge_type = QueryStructureEdgeTypeResponse::Argument(arg_name);
                edges.push(QueryStructureEdgeResponse {
                    r#type: edge_type,
                    from: argument,
                    to: expr_vertex.clone(),
                    span,
                });
                Ok::<_, Box<ConceptReadError>>(())
            })?;
        }
        Constraint::FunctionCallBinding(function_call) => {
            let repr =
                context.get_call_syntax(constraint).map_or_else(|| format!("Function#{index}"), |text| text.clone());
            let func_vertex = QueryStructureVertexResponse::FunctionCall { repr };
            function_call.ids_assigned().try_for_each(|variable| {
                let assigned = query_structure_vertex(context, &Vertex::Variable(variable))?;
                let assigned_name = context.get_variable_name(&variable).unwrap_or_else(|| variable.to_string());
                let edge_type = QueryStructureEdgeTypeResponse::Assigned(assigned_name);
                edges.push(QueryStructureEdgeResponse {
                    r#type: edge_type,
                    from: func_vertex.clone(),
                    to: assigned,
                    span,
                });
                Ok::<_, Box<ConceptReadError>>(())
            })?;
            function_call.required_ids().try_for_each(|variable| {
                let argument = query_structure_vertex(context, &Vertex::Variable(variable))?;
                let arg_name = context.get_variable_name(&variable).unwrap_or_else(|| variable.to_string());
                let edge_type = QueryStructureEdgeTypeResponse::Argument(arg_name);
                edges.push(QueryStructureEdgeResponse {
                    r#type: edge_type,
                    from: argument,
                    to: func_vertex.clone(),
                    span,
                });
                Ok::<_, Box<ConceptReadError>>(())
            })?;
        }
        | Constraint::Comparison(_) => {}
        Constraint::RoleName(_) => {} // Handled separately via resolved_role_names

        // Constraints that probably don't need to be handled
        | Constraint::Kind(_)
        | Constraint::Label(_)
        | Constraint::Value(_)
        | Constraint::Is(_)
        | Constraint::Iid(_) => {}
        Constraint::IndexedRelation(indexed) => {
            unreachable!("This is unreachable since we extract query-structure before we apply transformations");
        }
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
            QueryStructureVertexResponse::Label(serde_json::json!(encode_type_concept(
                &type_,
                context.snapshot,
                context.type_manager
            )?))
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
        Ok(QueryStructureVertexResponse::Label(serde_json::json!(RoleTypeResponse { label: label.to_owned() })))
    } else {
        query_structure_vertex(context, role_type)
    }
}
