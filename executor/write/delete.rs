use compiler::delete::{delete::DeletePlan, instructions::DeleteEdge};
use concept::thing::thing_manager::ThingManager;
use storage::snapshot::WritableSnapshot;

use crate::{
    batch::Row,
    write::{write_instruction::AsWriteInstruction, WriteError},
};

pub struct DeleteExecutor {
    plan: DeletePlan,
}

// TODO: pub(crate)
impl DeleteExecutor {
    pub fn new(plan: DeletePlan) -> Self {
        Self { plan }
    }

    pub fn plan(&self) -> &DeletePlan {
        &self.plan
    }

    pub fn execute_delete(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        row: &mut Row<'_>,
    ) -> Result<(), WriteError> {
        // Row multiplicity doesn't matter. You can't delete the same thing twice
        for instruction in &self.plan.edge_instructions {
            match instruction {
                DeleteEdge::Has(has) => has.execute(snapshot, thing_manager, row)?,
                DeleteEdge::RolePlayer(role_player) => role_player.execute(snapshot, thing_manager, row)?,
            }
        }

        for instruction in &self.plan.vertex_instructions {
            instruction.execute(snapshot, thing_manager, row)?;
        }
        Ok(())
    }
}
