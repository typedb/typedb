/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashSet, sync::Arc};

use answer::{Thing, Type};
use compiler::match_::instructions::IsaReverseInstruction;
use concept::{error::ConceptReadError, thing::thing_manager::ThingManager};
use ir::pattern::constraint::Isa;
use lending_iterator::{adaptors::Map, AsHkt, LendingIterator};
use storage::snapshot::ReadableSnapshot;

use crate::{
    batch::ImmutableRow,
    instruction::{
        isa_executor::{
            instances_of_all_types_chained, instances_of_single_type, MultipleTypeIsaIterator, SingleTypeIsaIterator,
        },
        iterator::{SortedTupleIterator, TupleIterator},
        tuple::{isa_to_tuple_thing_type, isa_to_tuple_type_thing, TuplePositions, TupleResult},
        BinaryIterateMode, VariableModes,
    },
    VariablePosition,
};

pub(crate) struct IsaReverseExecutor {
    isa: Isa<VariablePosition>,
    iterate_mode: BinaryIterateMode,
    variable_modes: VariableModes,
    thing_types: Arc<HashSet<Type>>,
}

pub(crate) type IsaReverseUnboundedSortedTypeSingle =
    Map<SingleTypeIsaIterator, ThingToTupleFn, AsHkt![TupleResult<'_>]>;
pub(crate) type IsaReverseUnboundedSortedThingSingle =
    Map<SingleTypeIsaIterator, ThingToTupleFn, AsHkt![TupleResult<'_>]>;
pub(crate) type IsaReverseBoundedSortedThing = Map<SingleTypeIsaIterator, ThingToTupleFn, AsHkt![TupleResult<'_>]>;

pub(crate) type IsaReverseUnboundedSortedTypeMerged =
    Map<MultipleTypeIsaIterator, ThingToTupleFn, AsHkt![TupleResult<'_>]>;
pub(crate) type IsaReverseUnboundedSortedThingMerged =
    Map<MultipleTypeIsaIterator, ThingToTupleFn, AsHkt![TupleResult<'_>]>;

type ThingToTupleFn = for<'a> fn(Result<Thing<'a>, ConceptReadError>) -> TupleResult<'a>;

impl IsaReverseExecutor {
    pub(crate) fn new(
        isa_reverse: IsaReverseInstruction<VariablePosition>,
        variable_modes: VariableModes,
        sort_by: Option<VariablePosition>,
    ) -> Self {
        let thing_types = isa_reverse.types().clone();
        debug_assert!(thing_types.len() > 0);
        debug_assert!(!thing_types.iter().any(|type_| matches!(type_, Type::RoleType(_))));
        let isa = isa_reverse.isa;
        let iterate_mode = BinaryIterateMode::new(isa.type_(), isa.thing(), &variable_modes, sort_by);

        Self { isa, iterate_mode, variable_modes, thing_types }
    }

    pub(crate) fn get_iterator(
        &self,
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        row: ImmutableRow<'_>,
    ) -> Result<TupleIterator, ConceptReadError> {
        match self.iterate_mode {
            BinaryIterateMode::Unbound => {
                let positions = TuplePositions::Pair([self.isa.type_(), self.isa.thing()]);
                if self.thing_types.len() == 1 {
                    // no heap allocs needed if there is only 1 iterator
                    let type_ = self.thing_types.iter().next().unwrap();
                    let iterator = instances_of_single_type(type_, thing_manager, snapshot)?;
                    let as_tuples: IsaReverseUnboundedSortedTypeSingle = iterator.map(isa_to_tuple_type_thing);
                    Ok(TupleIterator::IsaReverseUnboundedSingle(SortedTupleIterator::new(
                        as_tuples,
                        positions,
                        &self.variable_modes,
                    )))
                } else {
                    let thing_iter = instances_of_all_types_chained(&self.thing_types, thing_manager, snapshot)?;
                    let as_tuples: IsaReverseUnboundedSortedTypeMerged = thing_iter.map(isa_to_tuple_type_thing);
                    Ok(TupleIterator::IsaReverseUnboundedMerged(SortedTupleIterator::new(
                        as_tuples,
                        positions,
                        &self.variable_modes,
                    )))
                }
            }

            BinaryIterateMode::UnboundInverted => {
                let positions = TuplePositions::Pair([self.isa.thing(), self.isa.type_()]);
                if self.thing_types.len() == 1 {
                    // no heap allocs needed if there is only 1 iterator
                    let type_ = self.thing_types.iter().next().unwrap();
                    let iterator = instances_of_single_type(type_, thing_manager, snapshot)?;
                    let as_tuples: IsaReverseUnboundedSortedThingSingle = iterator.map(isa_to_tuple_thing_type);
                    Ok(TupleIterator::IsaReverseUnboundedInvertedSingle(SortedTupleIterator::new(
                        as_tuples,
                        positions,
                        &self.variable_modes,
                    )))
                } else {
                    let thing_iter = instances_of_all_types_chained(&self.thing_types, thing_manager, snapshot)?;
                    let as_tuples: IsaReverseUnboundedSortedThingMerged = thing_iter.map(isa_to_tuple_thing_type);
                    Ok(TupleIterator::IsaReverseUnboundedInvertedMerged(SortedTupleIterator::new(
                        as_tuples,
                        positions,
                        &self.variable_modes,
                    )))
                }
            }

            BinaryIterateMode::BoundFrom => {
                let positions = TuplePositions::Pair([self.isa.thing(), self.isa.type_()]);
                let type_ = row.get(self.isa.type_()).as_type();
                let iterator = instances_of_single_type(type_, thing_manager, snapshot)?;
                let as_tuples: IsaReverseBoundedSortedThing = iterator.map(isa_to_tuple_thing_type);
                Ok(TupleIterator::IsaReverseBounded(SortedTupleIterator::new(
                    as_tuples,
                    positions,
                    &self.variable_modes,
                )))
            }
        }
    }
}
