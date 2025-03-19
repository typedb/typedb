/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{cmp::Ordering, collections::BTreeMap, fmt, iter, ops::Bound, sync::Arc, vec};

use answer::{variable_value::VariableValue, Thing, Type};
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
use lending_iterator::LendingIterator;
use resource::profile::StorageCounters;
use storage::snapshot::ReadableSnapshot;

use crate::{
    instruction::{
        isa_executor::{IsaFilterMapFn, EXTRACT_THING, EXTRACT_TYPE},
        iterator::{SortedTupleIterator, TupleIterator, TupleSeekable},
        tuple::{isa_to_tuple_thing_type, isa_to_tuple_type_thing, Tuple, TuplePositions, TupleResult},
        type_from_row_or_annotations, BinaryIterateMode, Checker, VariableModes,
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
        storage_counters: StorageCounters,
    ) -> Result<TupleIterator, Box<ConceptReadError>> {
        let check = self.checker.filter_for_row(context, &row, storage_counters.clone());
        let filter_for_row: Box<IsaFilterMapFn> = Box::new(move |item| match check(&item) {
            Ok(true) | Err(_) => Some(item),
            Ok(false) => None,
        });

        let range = self.checker.value_range_for(
            context,
            Some(row.as_reference()),
            self.isa.thing().as_variable().unwrap(),
            storage_counters.clone(),
        )?;

        let snapshot = &**context.snapshot();
        let thing_manager = context.thing_manager();
        match self.iterate_mode {
            BinaryIterateMode::Unbound => {
                let thing_iter = instances_of_types_chained(
                    snapshot,
                    thing_manager,
                    self.type_to_instance_types.keys().copied(),
                    self.type_to_instance_types.as_ref(),
                    self.isa.isa_kind(),
                    &range,
                    storage_counters,
                )?;
                Ok(TupleIterator::IsaReverseUnbounded(SortedTupleIterator::new(
                    IsaReverseUnboundedSortedType::new(thing_iter, filter_for_row),
                    self.tuple_positions.clone(),
                    &self.variable_modes,
                )))
            }
            BinaryIterateMode::UnboundInverted => unreachable!(),
            BinaryIterateMode::BoundFrom => {
                let type_ = type_from_row_or_annotations(self.isa.type_(), row, self.type_to_instance_types.keys());
                let iterator = instances_of_types_chained(
                    snapshot,
                    thing_manager,
                    iter::once(type_),
                    self.type_to_instance_types.as_ref(),
                    self.isa.isa_kind(),
                    &range,
                    storage_counters,
                )?;
                Ok(TupleIterator::IsaReverseBounded(SortedTupleIterator::new(
                    IsaReverseBoundedSortedThing::new(iterator, filter_for_row, type_),
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

pub(super) fn instances_of_types_chained(
    snapshot: &impl ReadableSnapshot,
    thing_manager: &ThingManager,
    types: impl Iterator<Item = Type>,
    type_to_instance_types: &BTreeMap<Type, Vec<Type>>,
    isa_kind: IsaKind,
    range: &(Bound<Value<'_>>, Bound<Value<'_>>),
    storage_counters: StorageCounters,
) -> Result<MultipleTypeIsaReverseIterator, Box<ConceptReadError>> {
    let (attribute_types, object_types) =
        types.into_iter().partition::<Vec<_>, _>(|type_| matches!(type_, Type::Attribute(_)));

    let object_iters = object_types
        .into_iter()
        .flat_map(|type_| {
            let returned_types = if matches!(isa_kind, IsaKind::Subtype) {
                type_to_instance_types.get(&type_).unwrap_or(const { &Vec::new() }).clone()
            } else {
                vec![type_]
            };
            returned_types.into_iter().map({
                let counters = storage_counters.clone();
                move |subtype| {
                    IsaReverseObjectIterator::new(
                        thing_manager.get_objects_in(snapshot, subtype.as_object_type(), counters.clone()),
                        type_,
                    )
                }
            })
        })
        .collect_vec();

    // TODO: don't unwrap inside the operators
    let attribute_iters = attribute_types
        .into_iter()
        .flat_map(|type_| {
            let returned_types = if matches!(isa_kind, IsaKind::Subtype) {
                type_to_instance_types.get(&type_).unwrap_or(const { &Vec::new() }).clone()
            } else {
                vec![type_]
            };
            returned_types.into_iter().map({
                let counters = storage_counters.clone();
                move |subtype| {
                    let iter = thing_manager.get_attributes_in_range(
                        snapshot,
                        subtype.as_attribute_type(),
                        range,
                        counters.clone(),
                    )?;
                    Ok::<_, Box<_>>(IsaReverseAttributeIterator::new(iter, type_))
                }
            })
        })
        .try_collect()?;

    let thing_iter = MultipleTypeIsaReverseIterator::new(object_iters, attribute_iters);
    Ok(thing_iter)
}

pub(crate) struct IsaReverseBoundedSortedThing {
    inner: MultipleTypeIsaReverseIterator,
    filter_map: Box<IsaFilterMapFn>,
    type_: Type,
}

impl IsaReverseBoundedSortedThing {
    pub(crate) fn new(inner: MultipleTypeIsaReverseIterator, filter_map: Box<IsaFilterMapFn>, type_: Type) -> Self {
        Self { inner, filter_map, type_ }
    }
}

impl LendingIterator for IsaReverseBoundedSortedThing {
    type Item<'a> = TupleResult<'static>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        // TODO: can this be simplified with something like `.by_ref()` on iterators?
        while let Some(next) = self.inner.next() {
            if let Some(filter_mapped) = (self.filter_map)(next) {
                return Some(isa_to_tuple_thing_type(filter_mapped));
            }
        }
        None
    }
}

impl TupleSeekable for IsaReverseBoundedSortedThing {
    fn seek(&mut self, target: &Tuple<'_>) -> Result<(), Box<ConceptReadError>> {
        let target_type = target.values().get(0);
        let target_thing = target.values().get(1);
        self.inner.seek(target_type, target_thing)?;
        Ok(())
    }
}

pub(crate) struct IsaReverseUnboundedSortedType {
    inner: MultipleTypeIsaReverseIterator,
    filter_map: Box<IsaFilterMapFn>,
}

impl IsaReverseUnboundedSortedType {
    pub(crate) fn new(inner: MultipleTypeIsaReverseIterator, filter_map: Box<IsaFilterMapFn>) -> Self {
        Self { inner, filter_map }
    }
}

impl LendingIterator for IsaReverseUnboundedSortedType {
    type Item<'a> = TupleResult<'static>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        // TODO: can this be simplified with something like `.by_ref()` on iterators?
        while let Some(next) = self.inner.next() {
            if let Some(filter_mapped) = (self.filter_map)(next) {
                return Some(isa_to_tuple_type_thing(filter_mapped));
            }
        }
        None
    }
}

impl TupleSeekable for IsaReverseUnboundedSortedType {
    fn seek(&mut self, target: &Tuple<'_>) -> Result<(), Box<ConceptReadError>> {
        let target_type = target.values().get(0);
        let target_thing = target.values().get(1);
        self.inner.seek(target_type, target_thing)?;
        Ok(())
    }
}

pub(super) struct MultipleTypeIsaReverseIterator {
    object_iters: Vec<IsaReverseObjectIterator>,
    attribute_iters: Vec<IsaReverseAttributeIterator>,
}

impl MultipleTypeIsaReverseIterator {
    pub(super) fn new(
        mut objects: Vec<IsaReverseObjectIterator>,
        mut attributes: Vec<IsaReverseAttributeIterator>,
    ) -> Self {
        objects.reverse();
        attributes.reverse();
        Self { object_iters: objects, attribute_iters: attributes }
    }

    fn seek(
        &mut self,
        target_type: Option<&VariableValue<'_>>,
        target_thing: Option<&VariableValue<'_>>,
    ) -> Result<(), Box<ConceptReadError>> {
        // TODO!!!!
        todo!()
        //
        // let Some(target_type) = target_type else { return Ok(Some(Ordering::Greater)) };
        // let &VariableValue::Type(target_type) = target_type else {
        //     unreachable!("seeking to type {:?} which is not a `Type`", target_type)
        // };
        // match target_type {
        //     Type::Entity(_) | Type::Relation(_) => {
        //         while let Some(object_iter) = self.object_iters.last_mut() {
        //             if object_iter.type_ < target_type {
        //                 self.object_iters.pop();
        //             } else {
        //                 return object_iter.seek(target_thing);
        //             }
        //         }
        //         if self.attribute_iters.is_empty() {
        //             Ok(None)
        //         } else {
        //             Ok(Some(Ordering::Greater))
        //         }
        //     }
        //     Type::Attribute(_) => {
        //         while let Some(attribute_iter) = self.attribute_iters.last_mut() {
        //             if attribute_iter.iterator_type < target_type {
        //                 self.attribute_iters.pop();
        //             } else {
        //                 return attribute_iter.seek(target_thing);
        //             }
        //         }
        //         Ok(None)
        //     }
        //     Type::RoleType(_) => unreachable!("encountered role type during isa reverse seek"),
        // }
    }
}

impl Iterator for MultipleTypeIsaReverseIterator {
    type Item = Result<(Thing, Type), Box<ConceptReadError>>;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(object_iter) = self.object_iters.last_mut() {
            if let Some(item) = object_iter.next() {
                return Some(item);
            } else {
                self.object_iters.pop();
            }
        }
        while let Some(attribute_iter) = self.attribute_iters.last_mut() {
            if let Some(item) = attribute_iter.next() {
                return Some(item);
            } else {
                self.attribute_iters.pop();
            }
        }
        None
    }
}

struct IsaReverseObjectIterator {
    objects: InstanceIterator<Object>,
    type_: Type,
}

impl IsaReverseObjectIterator {
    fn new(objects: InstanceIterator<Object>, iterator_type: Type) -> Self {
        Self { objects, type_: iterator_type }
    }

    fn seek(&mut self, target_thing: Option<&VariableValue<'_>>) -> Result<(), Box<ConceptReadError>> {
        // let Some(target_thing) = target_thing else { return Ok(Some(Ordering::Greater)) };
        // let VariableValue::Thing(target_thing) = target_thing else {
        //     unreachable!("seeking to thing {:?} which is not a `Thing`", target_thing)
        // };
        // match self.objects.seek(&target_thing.as_object())? {
        //     None => return Ok(None),
        //     Some(Ordering::Greater) => return Ok(Some(Ordering::Greater)),
        //     Some(Ordering::Equal) => (),
        //     Some(Ordering::Less) => unreachable!(),
        // }
        //
        // Ok(Some(Ordering::Equal))
        todo!()
    }
}

impl Iterator for IsaReverseObjectIterator {
    type Item = Result<(Thing, Type), Box<ConceptReadError>>;

    fn next(&mut self) -> Option<Self::Item> {
        let object = match Iterator::next(&mut self.objects)? {
            Ok(object) => object,
            Err(err) => return Some(Err(err)),
        };
        Some(Ok((Thing::from(object), self.type_)))
    }
}

struct IsaReverseAttributeIterator {
    attributes: AttributeIterator<InstanceIterator<Attribute>>,
    iterator_type: Type,
}

impl IsaReverseAttributeIterator {
    fn new(attributes: AttributeIterator<InstanceIterator<Attribute>>, iterator_type: Type) -> Self {
        Self { attributes, iterator_type }
    }

    fn seek(&mut self, target_thing: Option<&VariableValue<'_>>) -> Result<Option<Ordering>, Box<ConceptReadError>> {
        let Some(target_thing) = target_thing else { return Ok(Some(Ordering::Greater)) };
        let VariableValue::Thing(target_thing) = target_thing else {
            unreachable!("seeking to thing {:?} which is not a `Thing`", target_thing)
        };
        self.attributes.seek(&target_thing.as_attribute());
        let peek = self.attributes.peek().transpose()?;
        let peek = match peek {
            None => return Ok(None),
            Some(peek) => peek,
        };
        match peek.cmp(target_thing.as_attribute()) {
            Ordering::Greater => return Ok(Some(Ordering::Greater)),
            Ordering::Equal => (),
            Ordering::Less => unreachable!(),
        }

        Ok(Some(Ordering::Equal))
    }
}

impl Iterator for IsaReverseAttributeIterator {
    type Item = Result<(Thing, Type), Box<ConceptReadError>>;

    fn next(&mut self) -> Option<Self::Item> {
        let attribute = match self.attributes.next()? {
            Ok(attribute) => attribute,
            Err(err) => return Some(Err(err)),
        };
        Some(Ok((Thing::from(attribute), self.iterator_type)))
    }
}
