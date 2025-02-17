/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;

use answer::variable::Variable;
use ir::{pattern::constraint::Constraint, pipeline::VariableRegistry};
use itertools::Itertools;
use typeql::common::Span;

use crate::{
    annotation::type_annotations::TypeAnnotations,
    executable::{
        insert::{
            executable::{
                add_inserted_concepts, collect_role_type_bindings, get_thing_input_position, prepare_output_row_schema,
                resolve_links_role,
            },
            instructions::ConceptInstruction,
            VariableSource,
        },
        next_executable_id,
        update::instructions::{ConnectionInstruction, Has, Links},
        WriteCompilationError,
    },
    filter_variants, VariablePosition,
};

#[derive(Debug)]
pub struct UpdateExecutable {
    pub executable_id: u64,
    // Reuse the insert's concept instruction for attributes. Other isas should be validated earlier
    pub concept_instructions: Vec<ConceptInstruction>,
    pub connection_instructions: Vec<ConnectionInstruction>,
    pub output_row_schema: Vec<Option<(Variable, VariableSource)>>,
}

impl UpdateExecutable {
    pub fn output_width(&self) -> usize {
        self.output_row_schema.len()
    }
}

pub fn compile(
    constraints: &[Constraint<Variable>],
    input_variables: &HashMap<Variable, VariablePosition>,
    type_annotations: &TypeAnnotations,
    variable_registry: &VariableRegistry,
    source_span: Option<Span>,
) -> Result<UpdateExecutable, Box<WriteCompilationError>> {
    let mut attributes_inserts = Vec::with_capacity(constraints.len());
    let variables = add_inserted_concepts(
        constraints,
        input_variables,
        type_annotations,
        variable_registry,
        &mut attributes_inserts,
        source_span,
    )?;

    let mut connection_inserts = Vec::with_capacity(constraints.len());

    add_has(constraints, &variables, variable_registry, &mut connection_inserts)?;
    add_role_players(constraints, type_annotations, &variables, variable_registry, &mut connection_inserts)?;

    Ok(UpdateExecutable {
        executable_id: next_executable_id(),
        concept_instructions: attributes_inserts,
        connection_instructions: connection_inserts,
        output_row_schema: prepare_output_row_schema(&variables),
    })
}

fn add_has(
    constraints: &[Constraint<Variable>],
    input_variables: &HashMap<Variable, VariablePosition>,
    variable_registry: &VariableRegistry,
    instructions: &mut Vec<ConnectionInstruction>,
) -> Result<(), Box<WriteCompilationError>> {
    filter_variants!(Constraint::Has: constraints).try_for_each(|has| {
        let owner = get_thing_input_position(
            input_variables,
            has.owner().as_variable().unwrap(),
            variable_registry,
            has.source_span(),
        )?;
        let attribute = get_thing_input_position(
            input_variables,
            has.attribute().as_variable().unwrap(),
            variable_registry,
            has.source_span(),
        )?;
        instructions.push(ConnectionInstruction::Has(Has { owner, attribute }));
        Ok(())
    })
}

fn add_role_players(
    constraints: &[Constraint<Variable>],
    type_annotations: &TypeAnnotations,
    input_variables: &HashMap<Variable, VariablePosition>,
    variable_registry: &VariableRegistry,
    instructions: &mut Vec<ConnectionInstruction>,
) -> Result<(), Box<WriteCompilationError>> {
    let named_role_types = collect_role_type_bindings(constraints, type_annotations, variable_registry)?;
    for links in filter_variants!(Constraint::Links: constraints) {
        let relation = get_thing_input_position(
            input_variables,
            links.relation().as_variable().unwrap(),
            variable_registry,
            links.source_span(),
        )?;
        let player = get_thing_input_position(
            input_variables,
            links.player().as_variable().unwrap(),
            variable_registry,
            links.source_span(),
        )?;
        let role = resolve_links_role(type_annotations, input_variables, variable_registry, &named_role_types, links)?;
        instructions.push(ConnectionInstruction::Links(Links { relation, player, role }));
    }
    Ok(())
}
