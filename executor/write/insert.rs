/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::fmt::{Debug, Display};

use compiler::insert::{
    insert::InsertPlan,
    instructions::{InsertEdgeInstruction, InsertVertexInstruction},
};
use concept::thing::thing_manager::ThingManager;
use storage::snapshot::WritableSnapshot;

use crate::{
    batch::Row,
    write::{write_instruction::AsWriteInstruction, WriteError},
};

pub struct InsertExecutor {
    plan: InsertPlan,
}

impl InsertExecutor {
    pub fn new(plan: InsertPlan) -> Self {
        Self { plan }
    }

    pub(crate) fn plan(&self) -> &InsertPlan {
        &self.plan
    }

    pub fn execute_insert(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        row: &mut Row<'_>,
    ) -> Result<(), WriteError> {
        debug_assert!(row.multiplicity() == 1); // The accumulator should de-duplicate for insert
        let Self { plan } = self;
        for instruction in &plan.vertex_instructions {
            match instruction {
                InsertVertexInstruction::PutAttribute(isa_attr) => {
                    isa_attr.execute(snapshot, thing_manager, row)?;
                }
                InsertVertexInstruction::PutObject(isa_object) => {
                    isa_object.execute(snapshot, thing_manager, row)?;
                }
            }
        }
        for instruction in &plan.edge_instructions {
            match instruction {
                InsertEdgeInstruction::Has(has) => {
                    has.execute(snapshot, thing_manager, row)?;
                }
                InsertEdgeInstruction::RolePlayer(role_player) => {
                    role_player.execute(snapshot, thing_manager, row)?;
                }
            };
        }
        Ok(())
    }
}
