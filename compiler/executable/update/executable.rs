/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::{HashMap, HashSet};

use answer::variable::Variable;
use ir::{
    pattern::{Pattern, conjunction::Conjunction, constraint::Constraint, nested_pattern::NestedPattern},
    pipeline::{VariableRegistry, block::Block},
};
use typeql::common::Span;

use crate::{
    VariablePosition,
    annotation::type_annotations::{BlockAnnotations, TypeAnnotations},
    executable::{
        WriteCompilationError, WriteRequiredVariables,
        insert::{
            VariableSource,
            executable::{
                add_inserted_concepts, concept_instructions_map_to_vec, get_thing_position, prepare_output_row_schema,
                resolve_links_roles,
            },
            instructions::ConceptInstruction,
        },
        next_executable_id,
        update::instructions::{ConnectionInstruction, Has, Links},
    },
    filter_variants,
};

#[derive(Debug)]
pub struct UpdateExecutable {
    pub executable_id: u64,
    // Reuse the insert's concept instruction for attributes. Other isas should be validated earlier
    pub updates: Vec<ConditionalUpdate>,
    pub output_row_schema: Vec<Option<(Variable, VariableSource)>>,
}

impl UpdateExecutable {
    pub fn output_width(&self) -> usize {
        self.output_row_schema.len()
    }
}

pub fn compile(
    block: &Block,
    input_variable_positions: &HashMap<Variable, VariablePosition>,
    block_annotations: &BlockAnnotations,
    variable_registry: &VariableRegistry,
    source_span: Option<Span>,
) -> Result<UpdateExecutable, Box<WriteCompilationError>> {
    let unsafely_used_optional_variable = block
        .conjunction()
        .constraints()
        .iter()
        .flat_map(|constraint| constraint.ids())
        .find(|var| variable_registry.is_variable_optional(*var));

    if let Some(var) = unsafely_used_optional_variable {
        let variable = variable_registry.get_variable_name_or_unnamed(var).to_owned();
        return Err(Box::new(WriteCompilationError::OptionalVariableUsedOutsideTry { source_span, variable }));
    }

    let mut variable_positions = input_variable_positions.clone();
    let root_update = ConditionalUpdate::new(
        block.conjunction(),
        block_annotations,
        &mut variable_positions,
        variable_registry,
        source_span,
    )?;
    let mut updates = Vec::with_capacity(1 + block.conjunction().nested_patterns().len());
    updates.push(root_update);
    for nested_pattern in block.conjunction().nested_patterns() {
        let NestedPattern::Optional(optional) = nested_pattern else {
            unreachable!(
                "Non-optional nested patterns in update are illegal and should have been rejected during translation"
            )
        };
        updates.push(ConditionalUpdate::new(
            optional.conjunction(),
            block_annotations,
            &mut variable_positions,
            variable_registry,
            source_span,
        )?);
    }

    Ok(UpdateExecutable {
        executable_id: next_executable_id(),
        updates,
        output_row_schema: prepare_output_row_schema(input_variable_positions, &variable_positions),
    })
}

#[derive(Debug)]
pub struct ConditionalUpdate {
    pub concept_instructions: Vec<ConceptInstruction>,
    pub connection_instructions: Vec<ConnectionInstruction>,
    pub required_input_variables: WriteRequiredVariables,
}

impl ConditionalUpdate {
    fn new(
        conjunction: &ir::pattern::conjunction::Conjunction,
        block_annotations: &BlockAnnotations,
        variable_positions: &mut HashMap<Variable, VariablePosition>,
        variable_registry: &VariableRegistry,
        stage_source_span: Option<Span>,
    ) -> Result<Self, Box<WriteCompilationError>> {
        let concept_instruction_map = add_inserted_concepts(
            conjunction,
            block_annotations,
            variable_registry,
            variable_positions,
            stage_source_span,
        )?;

        let connection_instructions =
            add_connections(conjunction, block_annotations, variable_positions, variable_registry)?;

        let required_input_variables = WriteRequiredVariables(
            conjunction
                .constraints()
                .iter()
                .flat_map(|constraint| constraint.ids())
                .filter(|id| conjunction.is_input(id))
                .filter_map(|id| variable_positions.get(&id).copied())
                .collect(),
        );

        let concept_instructions = concept_instructions_map_to_vec(concept_instruction_map);

        Ok(Self { concept_instructions, connection_instructions, required_input_variables })
    }
}

fn add_connections(
    conjunction: &Conjunction,
    block_annotations: &BlockAnnotations,
    variable_positions: &HashMap<Variable, VariablePosition>,
    variable_registry: &VariableRegistry,
) -> Result<Vec<ConnectionInstruction>, Box<WriteCompilationError>> {
    let constraints = conjunction.constraints();
    let mut connection_instructions = Vec::with_capacity(constraints.len());
    let type_annotations =
        block_annotations.type_annotations_of(conjunction).expect("update conjunction must have type annotations");
    add_has(conjunction, variable_positions, variable_registry, &mut connection_instructions)?;
    add_links(conjunction, type_annotations, variable_positions, variable_registry, &mut connection_instructions)?;
    Ok(connection_instructions)
}

fn add_has(
    conjunction: &Conjunction,
    variable_positions: &HashMap<Variable, VariablePosition>,
    variable_registry: &VariableRegistry,
    instructions: &mut Vec<ConnectionInstruction>,
) -> Result<(), Box<WriteCompilationError>> {
    filter_variants!(Constraint::Has: conjunction.constraints()).try_for_each(|has| {
        let owner = get_thing_position(
            variable_positions,
            has.owner().as_variable().unwrap(),
            variable_registry,
            has.source_span(),
        )?;
        let attribute = get_thing_position(
            variable_positions,
            has.attribute().as_variable().unwrap(),
            variable_registry,
            has.source_span(),
        )?;
        instructions.push(ConnectionInstruction::Has(Has { owner, attribute }));
        Ok(())
    })
}

fn add_links(
    conjunction: &Conjunction,
    type_annotations: &TypeAnnotations,
    variable_positions: &HashMap<Variable, VariablePosition>, // Also contains ones inserted.
    variable_registry: &VariableRegistry,
    instructions: &mut Vec<ConnectionInstruction>,
) -> Result<(), Box<WriteCompilationError>> {
    let resolved_role_types =
        resolve_links_roles(conjunction, type_annotations, variable_positions, variable_registry)?;
    for links in filter_variants!(Constraint::Links: conjunction.constraints()) {
        let relation = get_thing_position(
            variable_positions,
            links.relation().as_variable().unwrap(),
            variable_registry,
            links.source_span(),
        )?;
        let player = get_thing_position(
            variable_positions,
            links.player().as_variable().unwrap(),
            variable_registry,
            links.source_span(),
        )?;
        let role = resolved_role_types.get(&links.role_type().as_variable().unwrap()).unwrap().clone();
        instructions.push(ConnectionInstruction::Links(Links { relation, player, role }));
    }
    Ok(())
}
