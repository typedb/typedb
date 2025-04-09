/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;

use answer::variable::Variable;
use compiler::executable::pipeline::QueryStructure;
use ir::pattern::{constraint::Constraint, Vertex};
use itertools::Itertools;

const JSON_NONE: &str = "{}";

pub(super) fn encode_query_structure(query_structure: &QueryStructure) -> String {
    let branches = query_structure
        .parametrised_structure
        .branches
        .iter()
        .enumerate()
        .filter_map(|(i, branch_opt)| {
            branch_opt.as_ref().map(|branch| (i as u64, encode_query_structure_branch(&query_structure, branch)))
        })
        .collect::<HashMap<_, _>>();
    format!("[\n{}\n]", branches.iter().sorted().map(|(i, b)| { format!("\t{i}: {b}",) }).join(",\n"))
}

fn encode_query_structure_branch(query_structure: &QueryStructure, branch: &[Constraint<Variable>]) -> String {
    let constraints = branch
        .iter()
        .filter_map(|constraint| query_structure_constraint_edge(query_structure, constraint))
        .collect::<Vec<_>>();
    format!("[\n{}\n\t]", constraints.iter().map(|constraint| { format!("\t\t{constraint},") }).join("\n"))
}

macro_rules! format_edge {
    ( $type_:expr, $from:expr, $to:expr, $param:expr ) => {
        format!("{{\"edge_type\": {}, \"from\": {}, \"to\": {}, \"label\": {}, }}", $type_, $from, $to, $param)
    };
}

fn query_structure_constraint_edge(
    query_structure: &QueryStructure,
    constraint: &Constraint<Variable>,
) -> Option<String> {
    match constraint {
        Constraint::Has(has) => {
            let from = query_structure_constraint_edge_vertex(query_structure, has.owner());
            let to = query_structure_constraint_edge_vertex(query_structure, has.attribute());
            let param = JSON_NONE;
            Some(format_edge!("has", to, from, param))
        }
        Constraint::Links(links) => {
            let from = query_structure_constraint_edge_vertex(query_structure, links.relation());
            let to = query_structure_constraint_edge_vertex(query_structure, links.player());
            let edge_parameter = query_structure_constraint_edge_vertex(query_structure, links.role_type());
            Some(format_edge!("links", to, from, edge_parameter))
        }
        Constraint::Isa(isa) => {
            let edge_parameter = JSON_NONE;
            let from = query_structure_constraint_edge_vertex(query_structure, isa.thing());
            let to = query_structure_constraint_edge_vertex(query_structure, isa.type_());
            Some(format_edge!("isa", to, from, edge_parameter))
        }
        | Constraint::Sub(_)
        | Constraint::Owns(_)
        | Constraint::Relates(_)
        | Constraint::Plays(_)
        | Constraint::ExpressionBinding(_)
        | Constraint::FunctionCallBinding(_)
        | Constraint::Comparison(_) => None,

        // Constraints that I may need to handle
        Constraint::RoleName(_)
        | Constraint::Kind(_)
        | Constraint::Label(_)
        | Constraint::Value(_)
        | Constraint::Is(_)
        | Constraint::Iid(_) => None,
        // Optimisations don't represent the structure
        Constraint::IndexedRelation(_) | Constraint::LinksDeduplication(_) | Constraint::Unsatisfiable(_) => None,
    }
}

fn query_structure_constraint_edge_vertex(query_structure: &QueryStructure, vertex: &Vertex<Variable>) -> String {
    let vertex = match vertex {
        Vertex::Variable(variable) => query_structure
            .get_variable_position(variable)
            .map(|position| format!("{{ \"variable\": {} }}", position.as_usize()))
            .unwrap_or(JSON_NONE.to_string()),
        Vertex::Label(label) => query_structure
            .get_type(label)
            .map(|type_| format!("{{ \"label\": \"{}\" }}", label.to_string()))
            .unwrap_or(JSON_NONE.to_string()),
        Vertex::Parameter(param) => query_structure
            .get_parameter_value(param)
            .map(|value| {
                let as_string = value.to_string();
                let str = as_string.as_str();
                let escaped = str.strip_prefix("\"").unwrap_or(str).strip_suffix("\"").unwrap_or(str).escape_default();
                format!("{{ \"value\": \"{}\" }}", escaped)
            })
            .unwrap_or(JSON_NONE.to_string()),
    };
    vertex
}
