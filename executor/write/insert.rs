/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use compiler::insert::{
    instructions::{ConceptInstruction, ConnectionInstruction},
    program::InsertProgram,
};
use concept::thing::thing_manager::ThingManager;
use storage::snapshot::WritableSnapshot;

use crate::{
    batch::Row,
    write::{write_instruction::AsWriteInstruction, WriteError},
};

pub struct InsertExecutor {
    program: InsertProgram,
}

impl InsertExecutor {
    pub fn new(program: InsertProgram) -> Self {
        Self { program }
    }

    pub(crate) fn program(&self) -> &InsertProgram {
        &self.program
    }

    pub fn execute_insert(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        row: &mut Row<'_>,
    ) -> Result<(), WriteError> {
        debug_assert!(row.multiplicity() == 1); // The accumulator should de-duplicate for insert
        let Self { program } = self;
        for instruction in &program.concept_instructions {
            match instruction {
                ConceptInstruction::PutAttribute(isa_attr) => {
                    isa_attr.execute(snapshot, thing_manager, row)?;
                }
                ConceptInstruction::PutObject(isa_object) => {
                    isa_object.execute(snapshot, thing_manager, row)?;
                }
            }
        }
        for instruction in &program.connection_instructions {
            match instruction {
                ConnectionInstruction::Has(has) => {
                    has.execute(snapshot, thing_manager, row)?;
                }
                ConnectionInstruction::RolePlayer(role_player) => {
                    role_player.execute(snapshot, thing_manager, row)?;
                }
            };
        }
        Ok(())
    }
}
