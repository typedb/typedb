/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;
use answer::variable::Variable;
use compiler::query_structure::QueryStructure;
use itertools::Itertools;
use encoding::{graph::type_::Kind, value::ValueEncodable};
use ir::pattern::{
    constraint::{Constraint, IsaKind, SubKind},
    Vertex,
};
use serde::{Deserialize, Serialize};
use concept::type_::type_manager::TypeManager;
use storage::snapshot::ReadableSnapshot;

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
    Label { kind: String, label: String },
    Value { value_type: String, value: String }, // TODO: Try to re-use the standard encoding for values in rows.
    Expression { repr: String },
    FunctionCall { repr: String },
    UnavailableVariable { variable: String },
}

struct QueryStructureContext<'a, Snapshot: ReadableSnapshot> {
    query_structure: &'a QueryStructure,
    snapshot: &'a Snapshot,
    type_manager: &'a TypeManager,
}

pub(crate) fn encode_query_structure(snapshot: &impl ReadableSnapshot, type_manager: &TypeManager, query_structure: &QueryStructure) -> EncodedQueryStructure {
    let context = QueryStructureContext { query_structure, snapshot, type_manager };
    let branches = context.query_structure
        .parametrised_structure
        .branches
        .iter()
        .filter_map(|branch_opt| {
            branch_opt.as_ref().map(|branch| encode_query_structure_branch(&context, branch))
        })
        .collect::<Vec<_>>();
    EncodedQueryStructure { branches }
}

fn encode_query_structure_branch(
    context: &QueryStructureContext<'_, impl ReadableSnapshot>,
    branch: &[Constraint<Variable>],
) -> EncodedQueryStructureBranch {
    let mut edges = Vec::new();
    branch
        .iter()
        .enumerate()
        .for_each(|(index, constraint)| query_structure_edge(context, constraint, &mut edges, index));
    EncodedQueryStructureBranch { edges }
}

macro_rules! push_edge {
    ($edges:ident, $query_structure:expr, $from:expr, $to:expr, $variant:ident) => {{
        let from = query_structure_vertex($query_structure, $from);
        let to = query_structure_vertex($query_structure, $to);
        $edges.push(EncodedQueryStructureEdge { r#type: EncodedQueryStructureEdgeType::$variant, from, to });
    }};
    ($edges:ident, $query_structure:expr, $from:expr, $to:expr, $variant:ident($param:expr)) => {{
        let from = query_structure_vertex($query_structure, $from);
        let to = query_structure_vertex($query_structure, $to);
        $edges.push(EncodedQueryStructureEdge { r#type: EncodedQueryStructureEdgeType::$variant($param), from, to });
    }};
}

fn query_structure_edge(
    context: &QueryStructureContext<'_, impl ReadableSnapshot>,
    constraint: &Constraint<Variable>,
    edges: &mut Vec<EncodedQueryStructureEdge>,
    index: usize,
) {
    match constraint {
        Constraint::Links(links) => {
            let role_type = query_structure_role_type_as_vertex(context, links.role_type());
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
            let role_type_1 = query_structure_role_type_as_vertex(context, indexed.role_type_1());
            let role_type_2 = query_structure_role_type_as_vertex(context, indexed.role_type_2());
            push_edge!(edges, context, indexed.relation(), indexed.player_1(), Links(role_type_1));
            push_edge!(edges, context, indexed.relation(), indexed.player_2(), Links(role_type_2));
        }
        Constraint::ExpressionBinding(expr) => {
            let expr_vertex =
                { EncodedQueryStructureVertex::Expression { repr: format!("Expression#{index}").to_owned() } }; // TODO
            expr.ids_assigned().for_each(|variable| {
                let assigned = query_structure_vertex(context, &Vertex::Variable(variable));
                let assigned_name =
                    context.query_structure.get_variable_name(&variable).unwrap_or_else(|| variable.to_string());
                let edge_type = EncodedQueryStructureEdgeType::Assigned(assigned_name);
                edges.push(EncodedQueryStructureEdge { r#type: edge_type, from: expr_vertex.clone(), to: assigned });
            });
            expr.required_ids().for_each(|variable| {
                let argument = query_structure_vertex(context, &Vertex::Variable(variable));
                let arg_name = context.query_structure.get_variable_name(&variable).unwrap_or_else(|| variable.to_string());
                let edge_type = EncodedQueryStructureEdgeType::Argument(arg_name);
                edges.push(EncodedQueryStructureEdge { r#type: edge_type, from: argument, to: expr_vertex.clone() });
            });
        }
        Constraint::FunctionCallBinding(function_call) => {
            let func_vertex =
                { EncodedQueryStructureVertex::FunctionCall { repr: format!("Function#{index}").to_owned() } }; // TODO
            function_call.ids_assigned().for_each(|variable| {
                let assigned = query_structure_vertex(context, &Vertex::Variable(variable));
                let assigned_name =
                    context.query_structure.get_variable_name(&variable).unwrap_or_else(|| variable.to_string());
                let edge_type = EncodedQueryStructureEdgeType::Assigned(assigned_name);
                edges.push(EncodedQueryStructureEdge { r#type: edge_type, from: func_vertex.clone(), to: assigned });
            });
            function_call.required_ids().for_each(|variable| {
                let argument = query_structure_vertex(context, &Vertex::Variable(variable));
                let arg_name = context.query_structure.get_variable_name(&variable).unwrap_or_else(|| variable.to_string());
                let edge_type = EncodedQueryStructureEdgeType::Argument(arg_name);
                edges.push(EncodedQueryStructureEdge { r#type: edge_type, from: argument, to: func_vertex.clone() });
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
    }
}

fn query_structure_vertex(context: &QueryStructureContext<'_, impl ReadableSnapshot>, vertex: &Vertex<Variable>) -> EncodedQueryStructureVertex {
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
            // TODO: Encode as in rows
            let type_ = context.query_structure.get_type(label).unwrap();
            EncodedQueryStructureVertex::Label { label: label.to_string(), kind: type_.kind().name().to_string() }
        }
        Vertex::Parameter(param) => {
            // TODO: Encode as in rows
            let value = context.query_structure.get_parameter_value(param).unwrap();
            let as_string = value.to_string();
            let str = as_string.as_str();
            let escaped = str.strip_prefix("\"").unwrap_or(str).strip_suffix("\"").unwrap_or(str).escape_default();
            EncodedQueryStructureVertex::Value {
                value_type: value.value_type().to_string(),
                value: escaped.to_string(),
            }
        }
    };
    vertex
}

fn query_structure_role_type_as_vertex(
    context: &QueryStructureContext<'_, impl ReadableSnapshot>,
    role_type: &Vertex<Variable>,
) -> EncodedQueryStructureVertex {
    if let Some(label) =
        context.query_structure.parametrised_structure.resolved_role_names.get(&role_type.as_variable().unwrap())
    {
        EncodedQueryStructureVertex::Label { kind: Kind::Role.to_string(), label: label.to_owned() }
    } else {
        query_structure_vertex(context, role_type)
    }
}
