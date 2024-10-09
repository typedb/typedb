/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use answer::variable_value::VariableValue;
use compiler::executable::match_::instructions::thing::ConstantInstruction;
use concept::error::ConceptReadError;
use storage::snapshot::ReadableSnapshot;

use crate::{
    instruction::{
        iterator::{SortedTupleIterator, TupleIterator},
        tuple::{Tuple, TuplePositions, TupleResult},
        VariableModes,
    },
    pipeline::stage::ExecutionContext,
    row::MaybeOwnedRow,
    VariablePosition,
};

pub(crate) struct ConstantExecutor {
    variable_modes: VariableModes,
    tuple_positions: TuplePositions,
    value: VariableValue<'static>,
}

pub(crate) type ConstantValueIterator = lending_iterator::Once<TupleResult<'static>>;

impl ConstantExecutor {
    pub(crate) fn new(
        constant: ConstantInstruction<VariablePosition>,
        variable_modes: VariableModes,
        _sort_by: Option<VariablePosition>,
    ) -> Self {
        debug_assert!(!variable_modes.all_inputs());
        let ConstantInstruction { var, value, .. } = constant;
        debug_assert_eq!(Some(var), _sort_by);
        let tuple_positions = TuplePositions::Single([Some(var)]);
        Self { variable_modes, tuple_positions, value }
    }

    pub(crate) fn get_iterator(
        &self,
        _: &ExecutionContext<impl ReadableSnapshot + 'static>,
        _: MaybeOwnedRow<'_>,
    ) -> Result<TupleIterator, ConceptReadError> {
        Ok(TupleIterator::Constant(SortedTupleIterator::new(
            lending_iterator::once(Ok(Tuple::Single([self.value.clone()]))),
            self.tuple_positions.clone(),
            &self.variable_modes,
        )))
    }
}
