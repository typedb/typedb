/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use compiler::delete::{instructions::ConnectionInstruction, program::DeleteProgram};
use concept::thing::thing_manager::ThingManager;
use storage::snapshot::WritableSnapshot;

use crate::write::{write_instruction::AsWriteInstruction, WriteError};
use crate::row::Row;

pub struct DeleteExecutor {
    program: DeleteProgram,
}

impl DeleteExecutor {
    pub fn new(program: DeleteProgram) -> Self {
        Self { program }
    }

    pub fn program(&self) -> &DeleteProgram {
        &self.program
    }

    pub fn execute_delete(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        input_output_row: &mut Row<'_>,
    ) -> Result<(), WriteError> {
        // Row multiplicity doesn't matter. You can't delete the same thing twice
        for instruction in &self.program.connection_instructions {
            match instruction {
                ConnectionInstruction::Has(has) => has.execute(snapshot, thing_manager, input_output_row)?,
                ConnectionInstruction::RolePlayer(role_player) => {
                    role_player.execute(snapshot, thing_manager, input_output_row)?
                }
            }
        }

        for instruction in &self.program.concept_instructions {
            instruction.execute(snapshot, thing_manager, input_output_row)?;
        }
        Ok(())
    }
}
