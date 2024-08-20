/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::fmt::{Debug, Display};

use answer::variable_value::VariableValue;
use compiler::{
    insert::{
        insert::InsertPlan,
        instructions::{InsertEdgeInstruction, InsertVertexInstruction},
    },
    VariablePosition,
};
use concept::thing::thing_manager::ThingManager;
use itertools::Either;
use lending_iterator::LendingIterator;
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};

use crate::{
    batch::{ImmutableRow, Row},
    pipeline::{
        accumulator::{AccumulatedRowIterator, AccumulatingStageAPI, Accumulator},
        PipelineContext, PipelineError, PipelineStageAPI, WritablePipelineStage,
    },
    write::{write_instruction::AsWriteInstruction, WriteError},
};

//
pub struct InsertExecutor {
    plan: InsertPlan,
}

impl InsertExecutor {
    pub fn new(plan: InsertPlan) -> Self {
        Self { plan }
    }
}

impl<Snapshot: WritableSnapshot + 'static> AccumulatingStageAPI<Snapshot> for InsertExecutor {
    fn process_accumulated(
        &self,
        context: &mut PipelineContext<Snapshot>,
        rows: &mut Box<[(Box<[VariableValue<'static>]>, u64)]>,
    ) -> Result<(), PipelineError> {
        let (snapshot, thing_manager) = context.borrow_parts_mut();
        for (row, multiplicity) in rows {
            self.execute_insert(snapshot, thing_manager, &mut Row::new(row, multiplicity))
                .map_err(|source| PipelineError::WriteError(source))?;
        }
        Ok(())
    }

    fn store_incoming_row_into(&self, incoming: &ImmutableRow<'_>, stored_row: &mut Box<[VariableValue<'static>]>) {
        (0..incoming.width()).for_each(|i| {
            stored_row[i] = incoming.get(VariablePosition::new(i as u32)).clone().into_owned();
        });
    }

    fn must_deduplicate_incoming_rows(&self) -> bool {
        true
    }

    fn row_width(&self) -> usize {
        self.plan.output_row_plan.len()
    }
}

impl InsertExecutor {
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
