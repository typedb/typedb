/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;

use answer::variable::Variable;
use ir::{
    pattern::{conjunction::Conjunction, constraint::Constraint, nested_pattern::NestedPattern},
    pipeline::{VariableRegistry, block::Block},
};
use typeql::common::Span;

use crate::{
    VariablePosition,
    annotation::type_annotations::{BlockAnnotations, TypeAnnotations},
    executable::{
        WriteCompilationError, WritePatternCondition,
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
    pub root_update: ConditionalUpdate,
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
    let mut variable_positions = input_variable_positions.clone();
    let root_update = ConditionalUpdate::build_recursive(
        block.conjunction(),
        block_annotations,
        &mut variable_positions,
        variable_registry,
        source_span,
    )?;

    Ok(UpdateExecutable {
        executable_id: next_executable_id(),
        root_update,
        output_row_schema: prepare_output_row_schema(input_variable_positions, &variable_positions),
    })
}

#[derive(Debug)]
pub struct ConditionalUpdate {
    pub condition: WritePatternCondition,
    pub concept_instructions: Vec<ConceptInstruction>,
    pub connection_instructions: Vec<ConnectionInstruction>,
    pub nested_updates: Vec<ConditionalUpdate>,
}

impl ConditionalUpdate {
    fn build_recursive(
        conjunction: &Conjunction,
        block_annotations: &BlockAnnotations,
        variable_positions: &mut HashMap<Variable, VariablePosition>,
        variable_registry: &VariableRegistry,
        stage_source_span: Option<Span>,
    ) -> Result<Self, Box<WriteCompilationError>> {
        let conjunction_annotations =
            block_annotations.type_annotations_of(conjunction).expect("update must have type annotations");
        let concept_instruction_map = add_inserted_concepts(
            conjunction,
            block_annotations,
            variable_registry,
            variable_positions,
            stage_source_span,
        )?;

        let connection_instructions =
            add_connections(conjunction, block_annotations, variable_positions, variable_registry)?;

        let condition = WritePatternCondition::build(conjunction, variable_positions, conjunction_annotations);

        let concept_instructions = concept_instructions_map_to_vec(concept_instruction_map);

        let nested_updates = conjunction
            .nested_patterns()
            .iter()
            .map(|nested| {
                let NestedPattern::Optional(optional) = nested else {
                    unreachable!("Only optionals are allowed as nested patterns in updates")
                };
                Self::build_recursive(
                    optional.conjunction(),
                    block_annotations,
                    variable_positions,
                    variable_registry,
                    stage_source_span,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self { condition, concept_instructions, connection_instructions, nested_updates })
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
