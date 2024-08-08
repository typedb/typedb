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
use compiler::planner::insert_planner::{InsertInstruction, InsertPlan};
use concept::{error::ConceptWriteError, thing::thing_manager::ThingManager};
use storage::snapshot::WritableSnapshot;

use crate::{
    batch::Row,
    write::write_instruction::{WriteExecutionContext, WriteInstruction},
    VariablePosition,
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

pub fn execute<'input, 'output>(
    // TODO: pub(crate)
    snapshot: &mut impl WritableSnapshot,
    thing_manager: &ThingManager,
    insert: &mut InsertExecutor,
    input: &Row<'input>,
    output: Row<'output>,
) -> Result<Row<'output>, InsertError> {
    debug_assert!(input.multiplicity() == 1); // Else, we have to return a set of rows.

    let InsertExecutor { plan, reused_created_things } = insert;
    let InsertPlan { type_constants, value_constants, output_row: output_row_plan, .. } = plan;

    let mut context = WriteExecutionContext::new(input, type_constants, value_constants, reused_created_things);
    for instruction in &plan.instructions {
        match instruction {
            InsertInstruction::Entity(isa_entity) => isa_entity.insert(snapshot, thing_manager, &mut context)?,
            InsertInstruction::Attribute(isa_attr) => isa_attr.insert(snapshot, thing_manager, &mut context)?,
            InsertInstruction::Relation(isa_relation) => isa_relation.insert(snapshot, thing_manager, &mut context)?,
            InsertInstruction::Has(has) => has.insert(snapshot, thing_manager, &mut context)?,
            InsertInstruction::RolePlayer(role_player) => role_player.insert(snapshot, thing_manager, &mut context)?,
        }
    }
    let mut output = output;
    context.populate_output_row(output_row_plan, &mut output);
    Ok(output) // TODO: Create output row
}

#[derive(Debug, Clone)]
pub enum InsertError {
    ConceptWrite { source: ConceptWriteError },
}

impl Display for InsertError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl Error for InsertError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::ConceptWrite { source, .. } => Some(source),
        }
    }
}
