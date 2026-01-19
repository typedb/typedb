/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{borrow::Cow, fmt, sync::Arc};

use answer::variable_value::VariableValue;
use bytes::util::HexBytesFormatter;
use compiler::VariablePosition;
use concept::error::ConceptReadError;
use encoding::value::value::Value;
use ir::pattern::expression::BuiltinConceptFunctionID;
use lending_iterator::{LendingIterator, Peekable};
use resource::profile::StepProfile;
use storage::snapshot::ReadableSnapshot;

use crate::{
    batch::{FixedBatch, FixedBatchRowIterator},
    error::ReadExecutionError,
    pipeline::stage::ExecutionContext,
    row::MaybeOwnedRow,
    ExecutionInterrupt,
};

pub(crate) struct BuiltinCallExecutor {
    builtin_id: BuiltinConceptFunctionID,
    argument_positions: Vec<VariablePosition>,
    assignment_positions: Vec<Option<VariablePosition>>,
    output_width: u32,
    input: Option<FixedBatch>,
    profile: Arc<StepProfile>,
}

impl fmt::Debug for BuiltinCallExecutor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "BuiltinCallExecutor (function id {:?})", self.builtin_id)
    }
}

impl BuiltinCallExecutor {
    pub(crate) fn new(
        builtin_id: BuiltinConceptFunctionID,
        argument_positions: Vec<VariablePosition>,
        assignment_positions: Vec<Option<VariablePosition>>,
        output_width: u32,
        profile: Arc<StepProfile>,
    ) -> Self {
        Self { builtin_id, argument_positions, assignment_positions, output_width, input: None, profile }
    }

    pub(crate) fn output_width(&self) -> u32 {
        self.output_width
    }

    pub(crate) fn prepare(
        &mut self,
        input_batch: FixedBatch,
        _context: &ExecutionContext<impl ReadableSnapshot + 'static>,
    ) -> Result<(), ReadExecutionError> {
        self.input = Some(input_batch);
        Ok(())
    }

    pub(crate) fn batch_continue(
        &mut self,
        context: &ExecutionContext<impl ReadableSnapshot>,
        _interrupt: &mut ExecutionInterrupt,
    ) -> Result<Option<FixedBatch>, ReadExecutionError> {
        let Some(input_batch) = self.input.take() else {
            return Ok(None);
        };
        let measurement = self.profile.start_measurement();
        let mut input = Peekable::new(FixedBatchRowIterator::new(Ok(input_batch)));
        debug_assert!(input.peek().is_some());

        let mut output = FixedBatch::new(self.output_width);

        while let Some(row) = input.next() {
            let input_row = row.map_err(|err| err.clone())?;
            let output_row = self
                .call_builtin(context, &input_row)
                .map_err(|err| ReadExecutionError::ConceptRead { typedb_source: err })?;
            output.append(|mut row| row.copy_from_row(output_row));
        }
        measurement.end(&self.profile, 1, output.len() as u64);
        if output.is_empty() {
            Ok(None)
        } else {
            Ok(Some(output))
        }
    }

    fn call_builtin<'a>(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot>,
        input_row: &MaybeOwnedRow<'a>,
    ) -> Result<MaybeOwnedRow<'a>, Box<ConceptReadError>> {
        let Some(res) = self.assignment_positions[0] else { return Ok(input_row.clone()) };
        let (mut row, multiplicity, provenance) = input_row.clone().into_owned_parts();
        if row.len() <= res.as_usize() {
            row.resize(res.as_usize() + 1, VariableValue::None);
        }

        row[res.as_usize()] = match self.builtin_id {
            BuiltinConceptFunctionID::Iid => {
                let iid = row[self.argument_positions[0].as_usize()].as_thing().iid();
                VariableValue::Value(Value::String(Cow::Owned(format!("{iid:x}"))))
            }
            BuiltinConceptFunctionID::Label => {
                let ty = row[self.argument_positions[0].as_usize()].as_type();
                let label = ty.get_label(&**context.snapshot(), context.type_manager())?;
                VariableValue::Value(Value::String(Cow::Owned(label.to_string())))
            }
        };

        Ok(MaybeOwnedRow::new_owned(row, multiplicity, provenance))
    }
}
