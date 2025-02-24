/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::BTreeMap, fmt, iter, ops::Bound, sync::Arc, vec};

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
use encoding::value::value::Value;
use ir::pattern::constraint::{Isa, IsaKind};
use itertools::Itertools;
use storage::snapshot::ReadableSnapshot;

use crate::{
    instruction::{
        isa_executor::{
            AttributeEraseFn, IsaFilterMapFn, IsaTupleIterator, ObjectEraseFn, EXTRACT_THING, EXTRACT_TYPE,
        },
        iterator::{SortedTupleIterator, TupleIterator},
        tuple::{isa_to_tuple_thing_type, isa_to_tuple_type_thing, TuplePositions},
        type_from_row_or_annotations, BinaryIterateMode, Checker, VariableModes, TYPES_EMPTY,
    },
    pipeline::stage::ExecutionContext,
    row::MaybeOwnedRow,
};

#[derive(Debug)]
pub(crate) struct IsaReverseExecutor {
    isa: Isa<ExecutorVariable>,
    iterate_mode: BinaryIterateMode,
    variable_modes: VariableModes,
    tuple_positions: TuplePositions,
    type_to_instance_types: Arc<BTreeMap<Type, Vec<Type>>>,
    checker: Checker<(Thing, Type)>,
}

pub(crate) type IsaReverseBoundedSortedThing = IsaTupleIterator<MultipleTypeIsaIterator>;
pub(crate) type IsaReverseUnboundedSortedType = IsaTupleIterator<MultipleTypeIsaIterator>;

type MultipleTypeIsaObjectIterator =
    iter::Flatten<vec::IntoIter<ThingWithType<iter::Map<InstanceIterator<Object>, ObjectEraseFn>>>>;
type MultipleTypeIsaAttributeIterator = iter::Flatten<
    vec::IntoIter<ThingWithType<iter::Map<AttributeIterator<InstanceIterator<Attribute>>, AttributeEraseFn>>>,
>;

pub(super) type MultipleTypeIsaIterator = iter::Chain<MultipleTypeIsaObjectIterator, MultipleTypeIsaAttributeIterator>;

type ThingWithType<I> = iter::Map<
    iter::Zip<I, iter::Repeat<Type>>,
    fn((Result<Thing, Box<ConceptReadError>>, Type)) -> Result<(Thing, Type), Box<ConceptReadError>>,
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

        let checker = Checker::<(Thing, Type)>::new(
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
    ) -> Result<TupleIterator, Box<ConceptReadError>> {
        let check = self.checker.filter_for_row(context, &row);
        let filter_for_row: Box<IsaFilterMapFn> = Box::new(move |item| match check(&item) {
            Ok(true) | Err(_) => Some(item),
            Ok(false) => None,
        });

        let range =
            self.checker.value_range_for(context, Some(row.as_reference()), self.isa.thing().as_variable().unwrap())?;

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
                    &range,
                )?;
                let as_tuples: IsaReverseUnboundedSortedType =
                    thing_iter.filter_map(filter_for_row).map(isa_to_tuple_type_thing);
                Ok(TupleIterator::IsaReverseUnbounded(SortedTupleIterator::new(
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
                let Some(type_) = type_ else { return Ok(TupleIterator::empty()) };
                let iterator = instances_of_types_chained(
                    snapshot,
                    thing_manager,
                    [&type_].into_iter(),
                    self.type_to_instance_types.as_ref(),
                    self.isa.isa_kind(),
                    &range,
                )?;
                let as_tuples: IsaReverseBoundedSortedThing = iterator
                    .filter_map(Box::new(move |res| match res {
                        Ok((_, ty)) if ty == type_ => filter_for_row(res),
                        Ok(_) => None,
                        Err(err) => Some(Err(err)),
                    }) as _)
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

impl fmt::Display for IsaReverseExecutor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Reverse[{}], mode={}", &self.isa, &self.iterate_mode)
    }
}

fn with_type<I: Iterator<Item = Result<Thing, Box<ConceptReadError>>>>(iter: I, type_: Type) -> ThingWithType<I> {
    iter.zip(iter::repeat(type_)).map(|(thing_res, ty)| match thing_res {
        Ok(thing) => Ok((thing, ty)),
        Err(err) => Err(err),
    })
}

pub(super) fn instances_of_types_chained<'a>(
    snapshot: &impl ReadableSnapshot,
    thing_manager: &ThingManager,
    types: impl Iterator<Item = &'a Type>,
    type_to_instance_types: &BTreeMap<Type, Vec<Type>>,
    isa_kind: IsaKind,
    range: &(Bound<Value<'_>>, Bound<Value<'_>>),
) -> Result<MultipleTypeIsaIterator, Box<ConceptReadError>> {
    let (attribute_types, object_types) =
        types.into_iter().partition::<Vec<_>, _>(|type_| matches!(type_, Type::Attribute(_)));

    let object_iters: Vec<ThingWithType<iter::Map<InstanceIterator<Object>, ObjectEraseFn>>> = object_types
        .into_iter()
        .flat_map(|type_| {
            let returned_types = if matches!(isa_kind, IsaKind::Subtype) {
                type_to_instance_types.get(type_).unwrap_or(&TYPES_EMPTY).clone()
            } else {
                vec![*type_]
            };
            returned_types.into_iter().map(move |subtype| {
                Ok::<_, Box<_>>(with_type(
                    thing_manager
                        .get_objects_in(snapshot, subtype.as_object_type())
                        .map((|res| res.map(Thing::from)) as ObjectEraseFn),
                    *type_,
                ))
            })
        })
        .try_collect()?;
    let object_iter: MultipleTypeIsaObjectIterator = object_iters.into_iter().flatten();

    // TODO: don't unwrap inside the operators
    let attribute_iters: Vec<_> = attribute_types
        .into_iter()
        .flat_map(|type_| {
            let returned_types = if matches!(isa_kind, IsaKind::Subtype) {
                type_to_instance_types.get(type_).unwrap_or(&TYPES_EMPTY).clone()
            } else {
                vec![*type_]
            };
            returned_types.into_iter().map(move |subtype| {
                Ok::<_, Box<_>>(with_type(
                    thing_manager
                        .get_attributes_in_range(snapshot, subtype.as_attribute_type(), range)?
                        .map((|res| res.map(Thing::Attribute)) as AttributeEraseFn),
                    *type_,
                ))
            })
        })
        .try_collect()?;
    let attribute_iter: MultipleTypeIsaAttributeIterator = attribute_iters.into_iter().flatten();

    let thing_iter: MultipleTypeIsaIterator = object_iter.chain(attribute_iter);
    Ok(thing_iter)
}
