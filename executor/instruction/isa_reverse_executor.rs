/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::BTreeSet, iter, option, sync::Arc, vec};

use answer::{Thing, Type};
use compiler::{executable::match_::instructions::thing::IsaReverseInstruction, ExecutorVariable};
use concept::{
    error::ConceptReadError,
    iterator::InstanceIterator,
    thing::{
        attribute::{Attribute, AttributeIterator},
        object::Object,
        thing_manager::ThingManager,
    },
};
use ir::pattern::constraint::{Isa, IsaKind, SubKind};
use itertools::Itertools;
use lending_iterator::{
    adaptors::{Chain, Flatten, Map, Zip},
    AsHkt, AsLendingIterator, LendingIterator,
};
use storage::snapshot::ReadableSnapshot;

use super::isa_executor::{AttributeEraseFn, MapToThing, ObjectEraseFn};
use crate::{
    instruction::{
        isa_executor::{
            instances_of_single_type, IsaFilterFn, IsaTupleIterator, SingleTypeIsaIterator, EXTRACT_THING, EXTRACT_TYPE,
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

type MultipleTypeIsaObjectIterator = Flatten<
    AsLendingIterator<vec::IntoIter<ThingWithType<MapToThing<InstanceIterator<AsHkt![Object<'_>]>, ObjectEraseFn>>>>,
>;
type MultipleTypeIsaAttributeIterator = Flatten<
    AsLendingIterator<
        vec::IntoIter<
            ThingWithType<MapToThing<AttributeIterator<InstanceIterator<AsHkt![Attribute<'_>]>>, AttributeEraseFn>>,
        >,
    >,
>;

pub(super) type MultipleTypeIsaIterator = Chain<MultipleTypeIsaObjectIterator, MultipleTypeIsaAttributeIterator>;

type ThingWithType<I> = Map<
    Zip<I, AsLendingIterator<iter::Cycle<option::IntoIter<Type>>>>,
    for<'a> fn((Result<Thing<'a>, ConceptReadError>, Type)) -> Result<(Thing<'a>, Type), ConceptReadError>,
    Result<(AsHkt![Thing<'_>], Type), ConceptReadError>,
>;

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

fn with_type<I: for<'a> LendingIterator<Item<'a> = Result<Thing<'a>, ConceptReadError>>>(
    iter: I,
    type_: Type,
) -> ThingWithType<I> {
    iter.zip(AsLendingIterator::new(Some(type_).into_iter().cycle())).map(|(thing_res, ty)| match thing_res {
        Ok(thing) => Ok((thing, ty)),
        Err(err) => Err(err.clone()),
    })
}

pub(super) fn instances_of_all_types_chained<'a>(
    snapshot: &impl ReadableSnapshot,
    thing_manager: &ThingManager,
    thing_types: impl IntoIterator<Item = &'a Type>,
    isa_kind: IsaKind,
) -> Result<MultipleTypeIsaIterator, ConceptReadError> {
    let type_manager = thing_manager.type_manager();
    let thing_types: Vec<_> = thing_types
        .into_iter()
        .cloned()
        .map(|type_| {
            let types = match isa_kind {
                IsaKind::Exact => vec![type_.clone()],
                IsaKind::Subtype => get_subtypes(snapshot, type_manager, &type_, SubKind::Subtype)?,
            };
            Ok((type_, types))
        })
        .try_collect()?;

    let (attribute_types, object_types) =
        thing_types.into_iter().partition::<Vec<_>, _>(|(type_, _)| matches!(type_, Type::Attribute(_)));

    let object_iters: Vec<_> = object_types
        .into_iter()
        .flat_map(|(type_, types)| {
            let type_ = type_.clone();
            types.into_iter().map(move |subtype| {
                Ok(with_type(
                    thing_manager
                        .get_objects_in(snapshot, subtype.as_object_type())
                        .map((|res| res.map(Thing::from)) as ObjectEraseFn),
                    type_.clone(),
                ))
            })
        })
        .try_collect()?;
    let object_iter: MultipleTypeIsaObjectIterator = AsLendingIterator::new(object_iters).flatten();

    let attribute_iters: Vec<_> = attribute_types
        .into_iter()
        .flat_map(|(type_, types)| {
            let type_ = type_.clone();
            types.into_iter().map(move |subtype| {
                Ok(with_type(
                    thing_manager
                        .get_attributes_in(snapshot, subtype.as_attribute_type())?
                        .map((|res| res.map(Thing::Attribute)) as AttributeEraseFn),
                    type_.clone(),
                ))
            })
        })
        .try_collect()?;
    let attribute_iter: MultipleTypeIsaAttributeIterator = AsLendingIterator::new(attribute_iters).flatten();

    let thing_iter: MultipleTypeIsaIterator = object_iter.chain(attribute_iter);
    Ok(thing_iter)
}
