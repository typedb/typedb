/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::{
    error::Error,
    fmt::{Debug, Display, Formatter},
};

use compiler::write::{
    delete::{DeleteInstruction, DeletePlan},
    insert::{InsertInstruction, InsertPlan},
};
use concept::{error::ConceptWriteError, thing::thing_manager::ThingManager};
use storage::snapshot::WritableSnapshot;

use crate::{
    batch::Row,
    write::{
        common::populate_output_row,
        write_instruction::{AsDeleteInstruction, AsInsertInstruction},
    },
};

pub struct InsertExecutor {
    plan: InsertPlan,
    reused_created_things: Vec<answer::Thing<'static>>,
}

impl InsertExecutor {
    pub fn new(plan: InsertPlan) -> Self {
        // let output_row = Vec::new(); // TODO
        let reused_created_things = Vec::with_capacity(plan.n_created_concepts);
        Self { plan, reused_created_things }
    }
}

pub fn execute_insert<'input, 'output>(
    // TODO: pub(crate)
    snapshot: &mut impl WritableSnapshot,
    thing_manager: &ThingManager,
    plan: &InsertPlan,
    input: &Row<'input>,
    output: Row<'output>,
    reused_created_things: &mut Vec<answer::Thing<'static>>,
) -> Result<Row<'output>, WriteError> {
    debug_assert!(input.multiplicity() == 1); // Else, we have to return a set of rows.
    for instruction in &plan.instructions {
        let inserted = match instruction {
            InsertInstruction::PutEntity(isa_entity) => {
                isa_entity.insert(snapshot, thing_manager, &input, reused_created_things)?
            }
            InsertInstruction::PutAttribute(isa_attr) => {
                isa_attr.insert(snapshot, thing_manager, &input, reused_created_things)?
            }
            InsertInstruction::PutRelation(isa_relation) => {
                isa_relation.insert(snapshot, thing_manager, &input, reused_created_things)?
            }
            InsertInstruction::Has(has) => has.insert(snapshot, thing_manager, &input, reused_created_things)?,
            InsertInstruction::RolePlayer(role_player) => {
                role_player.insert(snapshot, thing_manager, &input, reused_created_things)?
            }
        };
        if let Some(thing) = inserted {
            reused_created_things.push(thing);
        }
    }
    let mut output = output;
    populate_output_row(&plan.output_row_plan, input, reused_created_things.as_slice(), &mut output);
    Ok(output) // TODO: Create output row
}

pub fn execute_delete<'input, 'output>(
    // TODO: pub(crate)
    snapshot: &mut impl WritableSnapshot,
    thing_manager: &ThingManager,
    plan: &DeletePlan,
    input: &Row<'input>,
    output: Row<'output>,
) -> Result<Row<'output>, WriteError> {
    debug_assert!(input.multiplicity() == 1); // Else, we have to return a set of rows.

    for instruction in &plan.instructions {
        match instruction {
            DeleteInstruction::Entity(isa_entity) => isa_entity.delete(snapshot, thing_manager, input)?,
            DeleteInstruction::Attribute(isa_attr) => isa_attr.delete(snapshot, thing_manager, input)?,
            DeleteInstruction::Relation(isa_relation) => isa_relation.delete(snapshot, thing_manager, input)?,
            DeleteInstruction::Has(has) => has.delete(snapshot, thing_manager, input)?,
            DeleteInstruction::RolePlayer(role_player) => role_player.delete(snapshot, thing_manager, input)?,
        }
    }
    let mut output = output;
    populate_output_row(&plan.output_row_plan, input, [].as_slice(), &mut output);
    Ok(output) // TODO: Create output row
}

#[derive(Debug, Clone)]
pub enum WriteError {
    ConceptWrite { source: ConceptWriteError },
}

impl Display for WriteError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl Error for WriteError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::ConceptWrite { source, .. } => Some(source),
        }
    }
}
