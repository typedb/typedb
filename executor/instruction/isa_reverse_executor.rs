/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::BTreeSet, sync::Arc};

use answer::{Thing, Type};
use compiler::{executable::match_::instructions::thing::IsaReverseInstruction, ExecutorVariable};
use concept::error::ConceptReadError;
use ir::pattern::constraint::{Isa, IsaKind, SubKind};
use lending_iterator::{AsHkt, LendingIterator};
use storage::snapshot::ReadableSnapshot;

use crate::{
    instruction::{
        isa_executor::{
            instances_of_all_types_chained, instances_of_single_type, IsaFilterFn, IsaTupleIterator,
            MultipleTypeIsaIterator, SingleTypeIsaIterator, EXTRACT_THING, EXTRACT_TYPE,
        },
        iterator::{SortedTupleIterator, TupleIterator},
        sub_reverse_executor::get_subtypes,
        tuple::{isa_to_tuple_thing_type, isa_to_tuple_type_thing, TuplePositions},
        type_from_row_or_annotations, BinaryIterateMode, Checker, VariableModes,
    },
    pipeline::stage::ExecutionContext,
    row::MaybeOwnedRow,
};

pub(crate) struct IsaReverseExecutor {
    isa: Isa<ExecutorVariable>,
    iterate_mode: BinaryIterateMode,
    variable_modes: VariableModes,
    tuple_positions: TuplePositions,
    types: Arc<BTreeSet<Type>>,
    thing_types: Arc<BTreeSet<Type>>,
    checker: Checker<(AsHkt![Thing<'_>], Type)>,
}

pub(crate) type IsaReverseUnboundedSortedTypeSingle = IsaTupleIterator<SingleTypeIsaIterator>;
pub(crate) type IsaReverseUnboundedSortedThingSingle = IsaTupleIterator<SingleTypeIsaIterator>;
pub(crate) type IsaReverseBoundedSortedThing = IsaTupleIterator<MultipleTypeIsaIterator>;

pub(crate) type IsaReverseUnboundedSortedTypeMerged = IsaTupleIterator<MultipleTypeIsaIterator>;
pub(crate) type IsaReverseUnboundedSortedThingMerged = IsaTupleIterator<MultipleTypeIsaIterator>;

impl IsaReverseExecutor {
    pub(crate) fn new(
        isa_reverse: IsaReverseInstruction<ExecutorVariable>,
        variable_modes: VariableModes,
        sort_by: ExecutorVariable,
    ) -> Self {
        let thing_types = isa_reverse.thing_types().clone();
        let types = isa_reverse.types().clone();
        debug_assert!(thing_types.len() > 0);
        debug_assert!(!thing_types.iter().any(|type_| matches!(type_, Type::RoleType(_))));
        let IsaReverseInstruction { isa, checks, .. } = isa_reverse;
        let iterate_mode = BinaryIterateMode::new(isa.type_(), isa.thing(), &variable_modes, sort_by);

        let thing = isa.thing().as_variable();
        let type_ = isa.type_().as_variable();

        let output_tuple_positions = if iterate_mode.is_inverted() {
            TuplePositions::Pair([thing, type_])
        } else {
            TuplePositions::Pair([type_, thing])
        };

        let checker = Checker::<(Thing<'_>, Type)>::new(
            checks,
            [(thing, EXTRACT_THING), (type_, EXTRACT_TYPE)]
                .into_iter()
                .filter_map(|(var, ex)| Some((var?, ex)))
                .collect(),
        );

        Self { isa, iterate_mode, variable_modes, tuple_positions: output_tuple_positions, types, thing_types, checker }
    }

    pub(crate) fn get_iterator(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: MaybeOwnedRow<'_>,
    ) -> Result<TupleIterator, ConceptReadError> {
        let filter_for_row = self.checker.filter_for_row(context, &row);
        let snapshot = &**context.snapshot();
        let thing_manager = context.thing_manager();
        match self.iterate_mode {
            BinaryIterateMode::Unbound => {
                if self.thing_types.len() == 1 {
                    // no heap allocs needed if there is only 1 iterator
                    let type_ = self.thing_types.iter().next().unwrap();
                    let iterator = instances_of_single_type(snapshot, thing_manager, type_)?;
                    let as_tuples: IsaReverseUnboundedSortedTypeSingle = iterator
                        .try_filter::<_, IsaFilterFn, (Thing<'_>, Type), _>(filter_for_row)
                        .map(isa_to_tuple_type_thing);
                    Ok(TupleIterator::IsaReverseUnboundedSingle(SortedTupleIterator::new(
                        as_tuples,
                        self.tuple_positions.clone(),
                        &self.variable_modes,
                    )))
                } else {
                    let thing_iter = instances_of_all_types_chained(
                        snapshot,
                        thing_manager,
                        &*self.thing_types,
                        self.isa.isa_kind(),
                    )?;
                    let as_tuples: IsaReverseUnboundedSortedTypeMerged = thing_iter
                        .try_filter::<_, IsaFilterFn, (Thing<'_>, Type), _>(filter_for_row)
                        .map(isa_to_tuple_type_thing);
                    Ok(TupleIterator::IsaReverseUnboundedMerged(SortedTupleIterator::new(
                        as_tuples,
                        self.tuple_positions.clone(),
                        &self.variable_modes,
                    )))
                }
            }

            BinaryIterateMode::UnboundInverted => {
                if self.thing_types.len() == 1 {
                    // no heap allocs needed if there is only 1 iterator
                    let type_ = self.thing_types.iter().next().unwrap();
                    let iterator = instances_of_single_type(snapshot, thing_manager, type_)?;
                    let as_tuples: IsaReverseUnboundedSortedThingSingle = iterator
                        .try_filter::<_, IsaFilterFn, (Thing<'_>, Type), _>(filter_for_row)
                        .map(isa_to_tuple_thing_type);
                    Ok(TupleIterator::IsaReverseUnboundedInvertedSingle(SortedTupleIterator::new(
                        as_tuples,
                        self.tuple_positions.clone(),
                        &self.variable_modes,
                    )))
                } else {
                    let thing_iter = instances_of_all_types_chained(
                        snapshot,
                        thing_manager,
                        &*self.thing_types,
                        self.isa.isa_kind(),
                    )?;
                    let as_tuples: IsaReverseUnboundedSortedThingMerged = thing_iter
                        .try_filter::<_, IsaFilterFn, (Thing<'_>, Type), _>(filter_for_row)
                        .map(isa_to_tuple_thing_type);
                    Ok(TupleIterator::IsaReverseUnboundedInvertedMerged(SortedTupleIterator::new(
                        as_tuples,
                        self.tuple_positions.clone(),
                        &self.variable_modes,
                    )))
                }
            }

            BinaryIterateMode::BoundFrom => {
                let type_ = type_from_row_or_annotations(self.isa.type_(), row, self.types.iter());
                let types = match self.isa.isa_kind() {
                    IsaKind::Exact => vec![type_.clone()],
                    IsaKind::Subtype => get_subtypes(snapshot, context.type_manager(), &type_, SubKind::Subtype)?,
                };
                let iterator = instances_of_all_types_chained(snapshot, thing_manager, &types, self.isa.isa_kind())?;
                let as_tuples: IsaReverseBoundedSortedThing = iterator
                    .try_filter::<Box<IsaFilterFn>, IsaFilterFn, (Thing<'_>, Type), _>(Box::new(
                        move |res: &_| match res {
                            Ok((_, ty)) if ty == &type_ => filter_for_row(res),
                            Ok(_) => Ok(false),
                            Err(err) => Err(err.clone()),
                        },
                    ))
                    .map(isa_to_tuple_type_thing);
                Ok(TupleIterator::IsaReverseBounded(SortedTupleIterator::new(
                    as_tuples,
                    self.tuple_positions.clone(),
                    &self.variable_modes,
                )))
            }
        }
    }
}
