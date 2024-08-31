/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{HashMap, HashSet},
    marker::PhantomData,
    sync::Arc,
};

use answer::{Thing, Type};
use compiler::match_::instructions::IsaReverseInstruction;
use concept::{error::ConceptReadError, thing::thing_manager::ThingManager};
use ir::pattern::constraint::Isa;
use lending_iterator::{AsHkt, LendingIterator};
use storage::snapshot::ReadableSnapshot;

use crate::{
    instruction::{
        BinaryIterateMode,
        isa_executor::{
            instances_of_all_types_chained, instances_of_single_type, IsaFilterFn, IsaTupleIterator,
            MultipleTypeIsaIterator, SingleTypeIsaIterator, EXTRACT_THING, EXTRACT_TYPE,
        },
        iterator::{SortedTupleIterator, TupleIterator},
        tuple::{isa_to_tuple_thing_type, isa_to_tuple_type_thing, TuplePositions, TupleResult}, VariableModes,
    },
    VariablePosition,
};
use crate::row::MaybeOwnedRow;

pub(crate) struct IsaReverseExecutor {
    isa: Isa<VariablePosition>,
    iterate_mode: BinaryIterateMode,
    variable_modes: VariableModes,
    thing_types: Arc<HashSet<Type>>,
    checker: Checker<AsHkt![Thing<'_>]>,
}

pub(crate) type IsaReverseUnboundedSortedTypeSingle = IsaTupleIterator<SingleTypeIsaIterator>;
pub(crate) type IsaReverseUnboundedSortedThingSingle = IsaTupleIterator<SingleTypeIsaIterator>;
pub(crate) type IsaReverseBoundedSortedThing = IsaTupleIterator<SingleTypeIsaIterator>;

pub(crate) type IsaReverseUnboundedSortedTypeMerged = IsaTupleIterator<MultipleTypeIsaIterator>;
pub(crate) type IsaReverseUnboundedSortedThingMerged = IsaTupleIterator<MultipleTypeIsaIterator>;

impl IsaReverseExecutor {
    pub(crate) fn new(
        isa_reverse: IsaReverseInstruction<VariablePosition>,
        variable_modes: VariableModes,
        sort_by: Option<VariablePosition>,
    ) -> Self {
        let thing_types = isa_reverse.types().clone();
        debug_assert!(thing_types.len() > 0);
        debug_assert!(!thing_types.iter().any(|type_| matches!(type_, Type::RoleType(_))));
        let IsaReverseInstruction { isa, checks, .. } = isa_reverse;
        let iterate_mode = BinaryIterateMode::new(isa.type_(), isa.thing(), &variable_modes, sort_by);
        let checker = Checker::<Thing<'_>> {
            checks,
            extractors: HashMap::from([(isa.thing(), EXTRACT_THING), (isa.type_(), EXTRACT_TYPE)]),
            _phantom_data: PhantomData,
        };

        Self { isa, iterate_mode, variable_modes, thing_types, checker }
    }

    pub(crate) fn get_iterator(
        &self,
        snapshot: &Arc<impl ReadableSnapshot + 'static>,
        thing_manager: &Arc<ThingManager>,
        row: MaybeOwnedRow<'_>,
    ) -> Result<TupleIterator, ConceptReadError> {
        let filter_for_row = self.checker.filter_for_row(snapshot, thing_manager, &row);
        match self.iterate_mode {
            BinaryIterateMode::Unbound => {
                let positions = TuplePositions::Pair([self.isa.type_(), self.isa.thing()]);
                if self.thing_types.len() == 1 {
                    // no heap allocs needed if there is only 1 iterator
                    let type_ = self.thing_types.iter().next().unwrap();
                    let iterator = instances_of_single_type(type_, thing_manager, &**snapshot)?;
                    let as_tuples: IsaReverseUnboundedSortedTypeSingle = iterator
                        .try_filter::<_, IsaFilterFn, Thing<'_>, _>(filter_for_row)
                        .map(isa_to_tuple_type_thing);
                    Ok(TupleIterator::IsaReverseUnboundedSingle(SortedTupleIterator::new(
                        as_tuples,
                        positions,
                        &self.variable_modes,
                    )))
                } else {
                    let thing_iter = instances_of_all_types_chained(&self.thing_types, thing_manager, &**snapshot)?;
                    let as_tuples: IsaReverseUnboundedSortedTypeMerged = thing_iter
                        .try_filter::<_, IsaFilterFn, Thing<'_>, _>(filter_for_row)
                        .map(isa_to_tuple_type_thing);
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
                    let iterator = instances_of_single_type(type_, thing_manager, &**snapshot)?;
                    let as_tuples: IsaReverseUnboundedSortedThingSingle = iterator
                        .try_filter::<_, IsaFilterFn, Thing<'_>, _>(filter_for_row)
                        .map(isa_to_tuple_thing_type);
                    Ok(TupleIterator::IsaReverseUnboundedInvertedSingle(SortedTupleIterator::new(
                        as_tuples,
                        positions,
                        &self.variable_modes,
                    )))
                } else {
                    let thing_iter = instances_of_all_types_chained(&self.thing_types, thing_manager, &**snapshot)?;
                    let as_tuples: IsaReverseUnboundedSortedThingMerged = thing_iter
                        .try_filter::<_, IsaFilterFn, Thing<'_>, _>(filter_for_row)
                        .map(isa_to_tuple_thing_type);
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
                let iterator = instances_of_single_type(type_, thing_manager, &**snapshot)?;
                let as_tuples: IsaReverseBoundedSortedThing =
                    iterator.try_filter::<_, IsaFilterFn, Thing<'_>, _>(filter_for_row).map(isa_to_tuple_thing_type);
                Ok(TupleIterator::IsaReverseBounded(SortedTupleIterator::new(
                    as_tuples,
                    positions,
                    &self.variable_modes,
                )))
            }
        }
    }
}
