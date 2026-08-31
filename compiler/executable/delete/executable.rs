/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::{HashMap, HashSet};

use answer::variable::Variable;
use encoding::graph::type_::Kind;
use ir::{
    pattern::{Vertex, constraint::Constraint, nested_pattern::NestedPattern},
    pipeline::{VariableRegistry, block::Block},
};
use typeql::common::Span;

use crate::{
    VariablePosition,
    annotation::type_annotations::{BlockAnnotations, TypeAnnotations},
    executable::{
        WriteCompilationError,
        delete::instructions::{ConnectionInstruction, Has, Links, ThingInstruction},
        insert::{
            ThingPosition,
            executable::{get_thing_position, resolve_links_roles},
        },
        next_executable_id,
    },
};

#[derive(Debug)]
pub struct DeleteExecutable {
    pub executable_id: u64,
    pub concept_instructions: Vec<ThingInstruction>,
    pub connection_instructions: Vec<ConnectionInstruction>,
    pub optional_deletes: Vec<OptionalDelete>,
    pub output_row_schema: Vec<Option<Variable>>,
    // pub debug_info: HashMap<VariableSource, Variable>,
}

pub fn compile(
    input_variables: &HashMap<Variable, VariablePosition>,
    block_annotations: &BlockAnnotations,
    variable_registry: &VariableRegistry,
    block: &Block,
    _source_span: Option<Span>,
) -> Result<DeleteExecutable, Box<WriteCompilationError>> {
    let conjunction_annotations = block_annotations
        .type_annotations_of(block.conjunction())
        .expect("delete conjunction must have type annotations");
    let mut deleted_variables_recursive = HashSet::new();
    let concept_instructions = add_concept_deletes(
        block.conjunction(),
        conjunction_annotations,
        input_variables,
        variable_registry,
        &mut deleted_variables_recursive,
    )?;
    let connection_instructions =
        add_connection_deletes(block.conjunction(), conjunction_annotations, input_variables, variable_registry)?;

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

    let mut optional_deletes = Vec::with_capacity(block.conjunction().nested_patterns().len());
    for nested_pattern in block.conjunction().nested_patterns() {
        let NestedPattern::Optional(optional) = nested_pattern else {
            unreachable!("Only optionals are allowed as nested patterns in delete")
        };
        optional_deletes.push(OptionalDelete::new(optional, block_annotations, variable_registry, input_variables)?);
    }

    // To produce the output stream, we remove the deleted concepts from each map in the stream.
    let mut output_row_schema = Vec::new();
    for (&variable, position) in input_variables {
        if deleted_variables_recursive.contains(&variable) {
            continue;
        }
        let pos_as_usize = position.as_usize();
        if output_row_schema.len() <= pos_as_usize {
            output_row_schema.resize(pos_as_usize + 1, None);
        }
        output_row_schema[pos_as_usize] = Some(variable);
    }

    Ok(DeleteExecutable {
        executable_id: next_executable_id(),
        connection_instructions,
        concept_instructions,
        optional_deletes,
        output_row_schema,
    })
}

#[derive(Debug)]
pub struct OptionalDelete {
    pub connection_instructions: Vec<ConnectionInstruction>,
    pub required_input_variables: HashSet<VariablePosition>,
}

impl OptionalDelete {
    fn new(
        optional: &ir::pattern::optional::Optional,
        block_annotations: &BlockAnnotations,
        variable_registry: &VariableRegistry,
        input_variables: &HashMap<Variable, VariablePosition>,
    ) -> Result<Self, Box<WriteCompilationError>> {
        let conjunction_annotations = block_annotations
            .type_annotations_of(optional.conjunction())
            .expect("delete conjunction must have type annotations");
        let connection_instructions = add_connection_deletes(
            optional.conjunction(),
            conjunction_annotations,
            input_variables,
            variable_registry,
        )?;

        let required_input_variables = optional
            .conjunction()
            .constraints()
            .iter()
            .flat_map(|constraint| constraint.ids())
            .filter_map(|id| input_variables.get(&id).copied())
            .collect();

        Ok(Self { connection_instructions, required_input_variables })
    }
}

fn add_concept_deletes(
    conjunction: &ir::pattern::conjunction::Conjunction,
    conjunction_annotations: &TypeAnnotations,
    input_variables: &HashMap<Variable, VariablePosition>,
    variable_registry: &VariableRegistry,
    deleted_variables_recursive: &mut HashSet<Variable>,
) -> Result<Vec<ThingInstruction>, Box<WriteCompilationError>> {
    let mut concept_instructions = Vec::new();
    for constraint in conjunction.constraints().iter().filter_map(|c| c.as_delete_concepts()) {
        for variable in constraint.ids() {
            let Some(input_position) = input_variables.get(&variable) else {
                return Err(Box::new(WriteCompilationError::DeletedThingWasNotInInput {
                    variable: variable_registry.get_variable_name_or_unnamed(variable).to_owned(),
                    source_span: constraint.source_span(),
                }));
            };
            assert!(conjunction_annotations.vertex_annotations_of(&Vertex::Variable(variable)).is_some());
            if conjunction_annotations
                .vertex_annotations_of(&Vertex::Variable(variable))
                .is_some_and(|anno| anno.iter().any(|type_| type_.kind() == Kind::Role))
            {
                return Err(Box::new(WriteCompilationError::DeleteIllegalRoleVariable {
                    variable: variable_registry.get_variable_name_or_unnamed(variable).to_owned(),
                    source_span: constraint.source_span(),
                }));
            } else {
                deleted_variables_recursive.insert(variable);
                concept_instructions.push(ThingInstruction { thing: ThingPosition(*input_position) });
            };
        }
    }
    Ok(concept_instructions)
}

fn add_connection_deletes(
    conjunction: &ir::pattern::conjunction::Conjunction,
    block_annotations: &TypeAnnotations,
    input_variables: &HashMap<Variable, VariablePosition>,
    variable_registry: &VariableRegistry,
) -> Result<Vec<ConnectionInstruction>, Box<WriteCompilationError>> {
    let resolved_roles =
        resolve_links_roles(conjunction.constraints(), block_annotations, input_variables, variable_registry)?;
    let mut connection_deletes = Vec::new();
    for constraint in conjunction.constraints() {
        match constraint {
            Constraint::Has(has) => {
                connection_deletes.push(ConnectionInstruction::Has(Has {
                    owner: get_thing_position(
                        input_variables,
                        has.owner().as_variable().unwrap(),
                        variable_registry,
                        has.source_span(),
                    )?,
                    attribute: get_thing_position(
                        input_variables,
                        has.attribute().as_variable().unwrap(),
                        variable_registry,
                        has.source_span(),
                    )?,
                }));
            }
            Constraint::Links(links) => {
                let relation = get_thing_position(
                    input_variables,
                    links.relation().as_variable().unwrap(),
                    variable_registry,
                    links.source_span(),
                )?;
                let player = get_thing_position(
                    input_variables,
                    links.player().as_variable().unwrap(),
                    variable_registry,
                    links.source_span(),
                )?;
                let role = resolved_roles.get(&links.role_type().as_variable().unwrap()).unwrap().clone();
                connection_deletes.push(ConnectionInstruction::Links(Links { relation, player, role }));
            }
            Constraint::LinksDeduplication(_) | Constraint::RoleName(_) => (), // Ignore. It will have done its job during type-inference
            Constraint::DeleteConcepts(_) => (),                               // Is a ConceptInstruction
            Constraint::Iid(_)
            | Constraint::Isa(_)
            | Constraint::Kind(_)
            | Constraint::Label(_)
            | Constraint::Owns(_)
            | Constraint::Relates(_)
            | Constraint::Plays(_)
            | Constraint::ExpressionBinding(_)
            | Constraint::Is(_)
            | Constraint::Comparison(_)
            | Constraint::Sub(_)
            | Constraint::Value(_)
            | Constraint::FunctionCallBinding(_)
            | Constraint::IndexedRelation(_)
            | Constraint::Unsatisfiable(_) => {
                unreachable!()
            }
        }
    }
    Ok(connection_deletes)
}
