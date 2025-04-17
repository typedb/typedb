/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;
use answer::variable::Variable;
use compiler::query_structure::QueryStructure;
use itertools::Itertools;
use ir::pattern::{
    constraint::{Constraint, IsaKind, SubKind},
    Vertex,
};
use serde::{Deserialize, Serialize};
use concept::error::ConceptReadError;
use concept::type_::type_manager::TypeManager;
use storage::snapshot::ReadableSnapshot;
use crate::service::http::message::query::concept::{encode_type_concept, encode_value, RoleTypeResponse, ValueResponse};

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EncodedQueryStructure {
    branches: Vec<EncodedQueryStructureBranch>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EncodedQueryStructureBranch {
    edges: Vec<EncodedQueryStructureEdge>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "kind", content = "param")]
pub enum EncodedQueryStructureEdgeType {
    Isa,
    Has,
    Links(EncodedQueryStructureVertex),

    Sub,
    Owns,
    Relates,
    Plays,

    IsaExact,
    SubExact,

    Assigned(String),
    Argument(String),
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EncodedQueryStructureEdge {
    r#type: EncodedQueryStructureEdgeType,
    from: EncodedQueryStructureVertex,
    to: EncodedQueryStructureVertex,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase", tag = "kind", content = "value")]
pub enum EncodedQueryStructureVertex {
    Variable { variable: String },
    Label(serde_json::Value),
    Value(ValueResponse),
    Expression { repr: String },
    FunctionCall { repr: String },
    UnavailableVariable { variable: String },
}

struct QueryStructureContext<'a, Snapshot: ReadableSnapshot> {
    query_structure: &'a QueryStructure,
    snapshot: &'a Snapshot,
    type_manager: &'a TypeManager,
    role_names: HashMap<Variable, String>,
}

impl<'a, Snapshot: ReadableSnapshot> QueryStructureContext<'a, Snapshot> {
    pub(crate) fn get_role_type(&self, variable: &Variable) -> Option<&str> {
        self.role_names.get(variable).map(|name| name.as_str())
    }
}

pub(crate) fn encode_query_structure(snapshot: &impl ReadableSnapshot, type_manager: &TypeManager, query_structure: &QueryStructure) -> Result<EncodedQueryStructure, Box<ConceptReadError>> {
    let branches = query_structure
        .parametrised_structure
        .branches
        .iter()
        .filter_map(|branch_opt| {
            branch_opt.as_ref().map(|branch| encode_query_structure_branch(snapshot, type_manager, &query_structure, branch))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(EncodedQueryStructure { branches })
}

fn encode_query_structure_branch(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    query_structure: &QueryStructure,
    branch: &[Constraint<Variable>],
) -> Result<EncodedQueryStructureBranch, Box<ConceptReadError>> {
    let mut edges = Vec::new();
    let role_names = branch.iter().filter_map(|constraint| constraint.as_role_name()).map(|rolename| (rolename.type_().as_variable().unwrap(), rolename.name().to_owned())).collect();
    let context = QueryStructureContext { query_structure, snapshot, type_manager, role_names };
    branch
        .iter()
        .enumerate()
        .try_for_each(|(index, constraint)| query_structure_edge(&context, constraint, &mut edges, index))?;
    Ok(EncodedQueryStructureBranch { edges })
}

macro_rules! push_edge {
    ($edges:ident, $query_structure:expr, $from:expr, $to:expr, $variant:ident) => {{
        let from = query_structure_vertex($query_structure, $from)?;
        let to = query_structure_vertex($query_structure, $to)?;
        $edges.push(EncodedQueryStructureEdge { r#type: EncodedQueryStructureEdgeType::$variant, from, to });
    }};
    ($edges:ident, $query_structure:expr, $from:expr, $to:expr, $variant:ident($param:expr)) => {{
        let from = query_structure_vertex($query_structure, $from)?;
        let to = query_structure_vertex($query_structure, $to)?;
        $edges.push(EncodedQueryStructureEdge { r#type: EncodedQueryStructureEdgeType::$variant($param), from, to });
    }};
}

fn query_structure_edge(
    context: &QueryStructureContext<'_, impl ReadableSnapshot>,
    constraint: &Constraint<Variable>,
    edges: &mut Vec<EncodedQueryStructureEdge>,
    index: usize,
) -> Result<(), Box<ConceptReadError>> {
    match constraint {
        Constraint::Links(links) => {
            let role_type = query_structure_role_type_as_vertex(context, links.role_type())?;
            push_edge!(edges, context, links.relation(), links.player(), Links(role_type))
        }
        Constraint::Has(has) => push_edge!(edges, context, has.owner(), has.attribute(), Has),
        Constraint::Isa(isa) => match isa.isa_kind() {
            IsaKind::Exact => push_edge!(edges, context, isa.thing(), isa.type_(), IsaExact),
            IsaKind::Subtype => push_edge!(edges, context, isa.thing(), isa.type_(), Isa),
        },
        Constraint::Sub(sub) => match sub.sub_kind() {
            SubKind::Exact => push_edge!(edges, context, sub.subtype(), sub.supertype(), SubExact),
            SubKind::Subtype => push_edge!(edges, context, sub.subtype(), sub.supertype(), Sub),
        },
        Constraint::Owns(owns) => push_edge!(edges, context, owns.owner(), owns.attribute(), Owns),
        Constraint::Relates(relates) => {
            push_edge!(edges, context, relates.relation(), relates.role_type(), Relates)
        }
        Constraint::Plays(plays) => push_edge!(edges, context, plays.player(), plays.role_type(), Plays),

        Constraint::IndexedRelation(indexed) => {
            let role_type_1 = query_structure_role_type_as_vertex(context, indexed.role_type_1())?;
            let role_type_2 = query_structure_role_type_as_vertex(context, indexed.role_type_2())?;
            push_edge!(edges, context, indexed.relation(), indexed.player_1(), Links(role_type_1));
            push_edge!(edges, context, indexed.relation(), indexed.player_2(), Links(role_type_2));
        }
        Constraint::ExpressionBinding(expr) => {
            // TODO: get expression text from the query string
            let expr_vertex =
                EncodedQueryStructureVertex::Expression { repr: format!("Expression#{index}").to_owned() };
            expr.ids_assigned().try_for_each(|variable| {
                let assigned = query_structure_vertex(context, &Vertex::Variable(variable))?;
                let assigned_name =
                    context.query_structure.get_variable_name(&variable).unwrap_or_else(|| variable.to_string());
                let edge_type = EncodedQueryStructureEdgeType::Assigned(assigned_name);
                edges.push(EncodedQueryStructureEdge { r#type: edge_type, from: expr_vertex.clone(), to: assigned });
                Ok::<_, Box<ConceptReadError>>(())
            })?;
            expr.required_ids().try_for_each(|variable| {
                let argument = query_structure_vertex(context, &Vertex::Variable(variable))?;
                let arg_name = context.query_structure.get_variable_name(&variable).unwrap_or_else(|| variable.to_string());
                let edge_type = EncodedQueryStructureEdgeType::Argument(arg_name);
                edges.push(EncodedQueryStructureEdge { r#type: edge_type, from: argument, to: expr_vertex.clone() });
                Ok::<_, Box<ConceptReadError>>(())
            })?;
        }
        Constraint::FunctionCallBinding(function_call) => {
            // TODO: get function call text from the query string
            let func_vertex =
                { EncodedQueryStructureVertex::FunctionCall { repr: format!("Function#{index}").to_owned() } };
            function_call.ids_assigned().try_for_each(|variable| {
                let assigned = query_structure_vertex(context, &Vertex::Variable(variable))?;
                let assigned_name =
                    context.query_structure.get_variable_name(&variable).unwrap_or_else(|| variable.to_string());
                let edge_type = EncodedQueryStructureEdgeType::Assigned(assigned_name);
                edges.push(EncodedQueryStructureEdge { r#type: edge_type, from: func_vertex.clone(), to: assigned });
                Ok::<_, Box<ConceptReadError>>(())
            })?;
            function_call.required_ids().try_for_each(|variable| {
                let argument = query_structure_vertex(context, &Vertex::Variable(variable))?;
                let arg_name = context.query_structure.get_variable_name(&variable).unwrap_or_else(|| variable.to_string());
                let edge_type = EncodedQueryStructureEdgeType::Argument(arg_name);
                edges.push(EncodedQueryStructureEdge { r#type: edge_type, from: argument, to: func_vertex.clone() });
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
        // Optimisations don't represent the structure
        Constraint::LinksDeduplication(_) | Constraint::Unsatisfiable(_) => {}
    };
    Ok(())
}

fn query_structure_vertex(context: &QueryStructureContext<'_, impl ReadableSnapshot>, vertex: &Vertex<Variable>) -> Result<EncodedQueryStructureVertex, Box<ConceptReadError>> {
    let vertex = match vertex {
        Vertex::Variable(variable) => {
            let name = context.query_structure.get_variable_name(variable).unwrap_or_else(|| variable.to_string());
            if context.query_structure.available_variables.contains(variable) {
                EncodedQueryStructureVertex::Variable { variable: name }
            } else {
                EncodedQueryStructureVertex::UnavailableVariable { variable: name }
            }
        }
        Vertex::Label(label) => {
            let type_ = context.query_structure.get_type(label).unwrap();
            EncodedQueryStructureVertex::Label(serde_json::json!(encode_type_concept(&type_, context.snapshot, context.type_manager)?))
        }
        Vertex::Parameter(param) => {
            let value = context.query_structure.get_parameter_value(param).unwrap();
            EncodedQueryStructureVertex::Value(encode_value(value))
        }
    };
    Ok(vertex)
}

fn query_structure_role_type_as_vertex(
    context: &QueryStructureContext<'_, impl ReadableSnapshot>,
    role_type: &Vertex<Variable>,
) -> Result<EncodedQueryStructureVertex, Box<ConceptReadError>> {
    if let Some(label) =
        context.get_role_type(&role_type.as_variable().unwrap())
    {   // Manually encode, because it could be ambiguous so we don't want to pass through a type.
        Ok(EncodedQueryStructureVertex::Label(serde_json::json!(RoleTypeResponse {label: label.to_owned()})))
    } else {
        query_structure_vertex(context, role_type)
    }
}
