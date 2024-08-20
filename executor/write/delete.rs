use compiler::delete::{delete::DeletePlan, instructions::DeleteEdge};
use concept::thing::thing_manager::ThingManager;
use storage::snapshot::WritableSnapshot;

use crate::{
    batch::Row,
    write::{write_instruction::AsWriteInstruction, WriteError},
};

pub fn execute_delete(
    // TODO: pub(crate)
    snapshot: &mut impl WritableSnapshot,
    thing_manager: &ThingManager,
    plan: &DeletePlan,
    row: &mut Row<'_>,
) -> Result<(), WriteError> {
    // Row multiplicity doesn't matter. You can't delete the same thing twice
    for instruction in &plan.edge_instructions {
        match instruction {
            DeleteEdge::Has(has) => has.execute(snapshot, thing_manager, row)?,
            DeleteEdge::RolePlayer(role_player) => role_player.execute(snapshot, thing_manager, row)?,
        }
    }

    for instruction in &plan.vertex_instructions {
        instruction.execute(snapshot, thing_manager, row)?;
    }
    Ok(())
}
