/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{BTreeMap, BTreeSet},
    iter, option,
    sync::Arc,
    vec,
};

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
use encoding::graph::{
    thing::{vertex_attribute::AttributeVertex, ThingVertex},
    type_::vertex::TypeVertexEncoding,
    Typed,
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
        isa_executor::{IsaFilterFn, IsaTupleIterator, SingleTypeIsaIterator, EXTRACT_THING, EXTRACT_TYPE},
        iterator::{SortedTupleIterator, TupleIterator},
        sub_reverse_executor::get_subtypes,
        tuple::{isa_to_tuple_type_thing, TuplePositions},
        type_from_row_or_annotations, BinaryIterateMode, Checker, VariableModes, TYPES_EMPTY,
    },
    pipeline::stage::ExecutionContext,
    row::MaybeOwnedRow,
};

pub(crate) struct IsaReverseExecutor {
    isa: Isa<ExecutorVariable>,
    iterate_mode: BinaryIterateMode,
    variable_modes: VariableModes,
    tuple_positions: TuplePositions,
    type_to_instance_types: Arc<BTreeMap<Type, Vec<Type>>>,
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
        let IsaReverseInstruction { isa, checks, type_to_instance_types, .. } = isa_reverse;
        debug_assert!(type_to_instance_types.len() > 0);
        debug_assert!(!type_to_instance_types.iter().any(|(type_, _)| matches!(type_, Type::RoleType(_))));
        let iterate_mode = BinaryIterateMode::new(isa.type_(), isa.thing(), &variable_modes, sort_by);

        let thing = isa.thing().as_variable();
        let type_ = isa.type_().as_variable();

        let output_tuple_positions = match iterate_mode {
            BinaryIterateMode::Unbound => TuplePositions::Pair([type_, thing]),
            _ => TuplePositions::Pair([thing, type_]),
        };

        let checker = Checker::<(Thing<'_>, Type)>::new(
            checks,
            [(thing, EXTRACT_THING), (type_, EXTRACT_TYPE)]
                .into_iter()
                .filter_map(|(var, ex)| Some((var?, ex)))
                .collect(),
        );

        Self {
            isa,
            iterate_mode,
            variable_modes,
            tuple_positions: output_tuple_positions,
            type_to_instance_types,
            checker,
        }
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
                let thing_iter = instances_of_types_chained(
                    snapshot,
                    thing_manager,
                    self.type_to_instance_types.keys(),
                    self.type_to_instance_types.as_ref(),
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
            BinaryIterateMode::UnboundInverted => {
                unreachable!()
            }
            BinaryIterateMode::BoundFrom => {
                let type_ = type_from_row_or_annotations(self.isa.type_(), row, self.type_to_instance_types.keys());
                let iterator = instances_of_types_chained(
                    snapshot,
                    thing_manager,
                    [&type_].into_iter(),
                    self.type_to_instance_types.as_ref(),
                    self.isa.isa_kind(),
                )?;
                let as_tuples: IsaReverseBoundedSortedThing = iterator
                    .try_filter::<Box<IsaFilterFn>, IsaFilterFn, (Thing<'_>, Type), _>(Box::new(
                        move |res: &_| match res {
                            Ok((_, ty)) if ty == &type_ => filter_for_row(res),
                            Ok(_) => Ok(false),
                            Err(err) => Err(err.clone()),
                        },
                    ))
                    .map(isa_to_tuple_thing_type);
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

pub(super) fn instances_of_types_chained<'a>(
    snapshot: &impl ReadableSnapshot,
    thing_manager: &ThingManager,
    types: impl Iterator<Item = &'a Type>,
    type_to_instance_types: &BTreeMap<Type, Vec<Type>>,
    isa_kind: IsaKind,
) -> Result<MultipleTypeIsaIterator, ConceptReadError> {
    let type_manager = thing_manager.type_manager();
    let (attribute_types, object_types) =
        types.into_iter().partition::<Vec<_>, _>(|type_| matches!(type_, Type::Attribute(_)));

    let object_iters: Vec<_> = object_types
        .into_iter()
        .flat_map(|type_| {
            let returned_types = if matches!(isa_kind, IsaKind::Subtype) {
                type_to_instance_types.get(type_).unwrap_or(&TYPES_EMPTY).clone()
            } else {
                vec![type_.clone()]
            };
            returned_types.into_iter().map(move |subtype| {
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

    // TODO: don't unwrap inside the operators
    let attribute_iters: Vec<_> = attribute_types
        .into_iter()
        .filter(|type_| type_.as_attribute_type().get_value_type(snapshot, type_manager).unwrap().is_some())
        .sorted_by_key(|type_| {
            AttributeVertex::build_prefix_type(
                AttributeVertex::value_type_category_to_prefix_type(
                    type_.as_attribute_type().get_value_type(snapshot, type_manager).unwrap().unwrap().0.category(),
                ),
                type_.as_attribute_type().vertex().type_id_(),
            )
        })
        .flat_map(|type_| {
            let returned_types = if matches!(isa_kind, IsaKind::Subtype) {
                type_to_instance_types.get(type_).unwrap_or(&TYPES_EMPTY).clone()
            } else {
                vec![type_.clone()]
            };
            returned_types.into_iter().map(move |subtype| {
                Ok(with_type(
                    thing_manager
                        .get_attributes_in(snapshot, type_.as_attribute_type())?
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
