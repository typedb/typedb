/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::{
    error::Error,
    fmt::{Debug, Display, Formatter},
};

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
    accumulator::{AccumulatedRowIterator, AccumulatingStageAPI, Accumulator},
    batch::{ImmutableRow, Row},
    pipeline::{PipelineContext, PipelineError, PipelineStageAPI, WritablePipelineStage},
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

type InsertAccumulator<Snapshot: WritableSnapshot + 'static> =
    Accumulator<Snapshot, WritablePipelineStage<Snapshot>, InsertExecutor>;
pub struct InsertStage<Snapshot: WritableSnapshot + 'static> {
    inner: Option<Either<InsertAccumulator<Snapshot>, AccumulatedRowIterator<Snapshot>>>, // TODO: Figure out how to neatly turn one into the other
    error: Option<PipelineError>,
}

impl<Snapshot: WritableSnapshot + 'static> InsertStage<Snapshot> {
    pub fn new(upstream: Box<WritablePipelineStage<Snapshot>>, executor: InsertExecutor) -> InsertStage<Snapshot> {
        let accumulator = Accumulator::new(upstream, executor);
        Self { inner: Some(Either::Left(accumulator)), error: None }
    }
}

impl<Snapshot: WritableSnapshot> LendingIterator for InsertStage<Snapshot> {
    type Item<'a> = Result<ImmutableRow<'a>, PipelineError>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        if self.inner.is_some() && self.inner.as_ref().unwrap().is_left() {
            let Either::Left(accumulator) = self.inner.take().unwrap() else { unreachable!() };
            match accumulator.accumulate_process_and_iterate() {
                Ok(iterator) => self.inner = Some(Either::Right(iterator)),
                Err(err) => {
                    self.error = Some(err);
                }
            }
        };

        if self.error.is_some() {
            Some(Err(self.error.as_ref().unwrap().clone()))
        } else {
            debug_assert!(self.inner.is_some() && self.inner.as_ref().unwrap().is_right());
            let Either::Right(iterator) = self.inner.as_mut().unwrap() else { unreachable!() };
            iterator.next()
        }
    }
}

impl<Snapshot: WritableSnapshot> PipelineStageAPI<Snapshot> for InsertStage<Snapshot> {
    fn finalise(self) -> PipelineContext<Snapshot> {
        match self.inner {
            Some(Either::Left(accumulator)) => todo!("Illegal, but unhandled"),
            Some(Either::Right(iterator)) => iterator.finalise(),
            None => todo!("Illegal again, but I don't prevent it?"),
        }
    }
}
