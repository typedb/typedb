/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{sync::Arc, vec};

use answer::Type;
use compiler::match_::instructions::type_::TypeInstruction;
use concept::{error::ConceptReadError, thing::thing_manager::ThingManager};
use itertools::Itertools;
use lending_iterator::{adaptors::Map, higher_order::AdHocHkt, AsLendingIterator, LendingIterator};
use storage::snapshot::ReadableSnapshot;

use crate::{
    instruction::{
        iterator::{SortedTupleIterator, TupleIterator},
        sub_executor::NarrowingTupleIterator,
        tuple::{type_to_tuple, TuplePositions, TupleResult, TypeToTupleFn},
        VariableModes,
    },
    row::MaybeOwnedRow,
    VariablePosition,
};

pub(crate) struct TypeExecutor {
    variable_modes: VariableModes,
    tuple_positions: TuplePositions,
    types: Vec<Type>,
}

pub(crate) type TypeIterator =
    NarrowingTupleIterator<Map<AsLendingIterator<vec::IntoIter<Type>>, TypeToTupleFn, AdHocHkt<TupleResult<'static>>>>;

impl TypeExecutor {
    pub(crate) fn new(
        type_: TypeInstruction<VariablePosition>,
        variable_modes: VariableModes,
        _sort_by: Option<VariablePosition>,
    ) -> Self {
        debug_assert!(!variable_modes.all_inputs());
        let types = type_.types().iter().cloned().sorted().collect_vec();
        debug_assert!(!types.is_empty());
        let TypeInstruction { type_var, .. } = type_;
        debug_assert_eq!(Some(type_var), _sort_by);
        let tuple_positions = TuplePositions::Single([type_var]);
        Self { variable_modes, tuple_positions, types }
    }

    pub(crate) fn get_iterator(
        &self,
        _: &Arc<impl ReadableSnapshot + 'static>,
        _: &Arc<ThingManager>,
        _: MaybeOwnedRow<'_>,
    ) -> Result<TupleIterator, ConceptReadError> {
        Ok(TupleIterator::Type(SortedTupleIterator::new(
            NarrowingTupleIterator(AsLendingIterator::new(self.types.clone()).map(type_to_tuple)),
            self.tuple_positions.clone(),
            &self.variable_modes,
        )))
    }
}
