/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{cmp::Ordering, collections::BTreeMap, fmt, iter, ops::Bound, sync::Arc, vec};

use answer::{variable_value::VariableValue, Thing, Type};
use compiler::{executable::match_::instructions::thing::IsaInstruction, ExecutorVariable};
use concept::{
    error::ConceptReadError,
    iterator::InstanceIterator,
    thing::{
        attribute,
        attribute::{Attribute, AttributeIterator},
        object::Object,
        thing_manager::ThingManager,
        ThingAPI,
    },
};
use encoding::value::value::Value;
use ir::pattern::{
    constraint::{Isa, IsaKind},
    Vertex,
};
use itertools::Itertools;
use lending_iterator::{AsLendingIterator, LendingIterator, Peekable, Seekable};
use resource::profile::StorageCounters;
use storage::snapshot::ReadableSnapshot;
use typeql::token::Keyword::Or;

use crate::{
    instruction::{
        iterator::{NaiiveSeekable, SortedTupleIterator, TupleIterator, TupleSeekable},
        tuple::{isa_to_tuple_thing_type, isa_to_tuple_type_thing, IsaToTupleFn, Tuple, TuplePositions, TupleResult},
        BinaryIterateMode, Checker, FilterMapUnchangedFn, VariableModes, TYPES_EMPTY,
    },
    pipeline::stage::ExecutionContext,
    row::MaybeOwnedRow,
};

#[derive(Debug)]
pub(crate) struct IsaExecutor {
    isa: Isa<ExecutorVariable>,
    iterate_mode: BinaryIterateMode,
    variable_modes: VariableModes,
    tuple_positions: TuplePositions,
    instance_type_to_types: Arc<BTreeMap<Type, Vec<Type>>>,
    checker: Checker<(Thing, Type)>,
}

pub(super) type IsaTupleIterator<I> = iter::Map<iter::FilterMap<I, Box<IsaFilterMapFn>>, IsaToTupleFn>;
pub(super) type IsaBoundedSortedType = NaiiveSeekable<
    AsLendingIterator<
        IsaTupleIterator<
            iter::Map<
                iter::Zip<iter::Repeat<Thing>, vec::IntoIter<Type>>,
                fn((Thing, Type)) -> Result<(Thing, Type), Box<ConceptReadError>>,
            >,
        >,
    >,
>;

type ThingWithTypes<I> = iter::FlatMap<
    iter::Zip<I, iter::Repeat<Vec<Type>>>,
    Vec<Result<(Thing, Type), Box<ConceptReadError>>>,
    fn((Result<Thing, Box<ConceptReadError>>, Vec<Type>)) -> Vec<Result<(Thing, Type), Box<ConceptReadError>>>,
>;

pub(super) type IsaFilterMapFn = FilterMapUnchangedFn<(Thing, Type)>;

type IsaVariableValueExtractor = for<'a, 'b> fn(&'a (Thing, Type)) -> VariableValue<'a>;

pub(super) const EXTRACT_THING: IsaVariableValueExtractor = |(thing, _)| VariableValue::Thing(thing.clone());
pub(super) const EXTRACT_TYPE: IsaVariableValueExtractor = |&(_, type_)| VariableValue::Type(type_);

impl IsaExecutor {
    pub(crate) fn new(
        isa: IsaInstruction<ExecutorVariable>,
        variable_modes: VariableModes,
        sort_by: ExecutorVariable,
    ) -> Self {
        let IsaInstruction { isa, checks, instance_type_to_types, .. } = isa;
        debug_assert!(instance_type_to_types.len() > 0);
        let iterate_mode = BinaryIterateMode::new(isa.thing(), isa.type_(), &variable_modes, sort_by);

        let thing = isa.thing().as_variable();
        let type_ = isa.type_().as_variable();

        let output_tuple_positions = match iterate_mode {
            BinaryIterateMode::Unbound => TuplePositions::Pair([thing, type_]),
            _ => TuplePositions::Pair([type_, thing]),
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
            instance_type_to_types,
            checker,
        }
    }

    pub(crate) fn get_iterator(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: MaybeOwnedRow<'_>,
        storage_counters: StorageCounters,
    ) -> Result<TupleIterator, Box<ConceptReadError>> {
        let check = self.checker.filter_for_row(context, &row);
        let filter_for_row: Box<IsaFilterMapFn> = Box::new(move |item| match check(&item) {
            Ok(true) | Err(_) => Some(item),
            Ok(false) => None,
        });

        let snapshot = &**context.snapshot();
        let thing_manager = context.thing_manager();
        match self.iterate_mode {
            BinaryIterateMode::Unbound => {
                let instances_range = if let Vertex::Variable(thing_variable) = self.isa.thing() {
                    self.checker.value_range_for(context, Some(row), *thing_variable)?
                } else {
                    (Bound::Unbounded, Bound::Unbounded)
                };
                let thing_iter = instances_of_all_types_chained(
                    snapshot,
                    thing_manager,
                    self.instance_type_to_types.as_ref(),
                    self.isa.isa_kind(),
                    instances_range,
                    storage_counters,
                )?;
                let as_tuples = IsaUnboundedSortedThing { inner: thing_iter, filter_map: filter_for_row };
                Ok(TupleIterator::IsaUnbounded(SortedTupleIterator::new(
                    as_tuples,
                    self.tuple_positions.clone(),
                    &self.variable_modes,
                )))
            }
            BinaryIterateMode::UnboundInverted => unreachable!(),
            BinaryIterateMode::BoundFrom => {
                let thing = self.isa.thing().as_variable().unwrap().as_position().unwrap();
                debug_assert!(row.len() > thing.as_usize());
                let VariableValue::Thing(thing) = row.get(thing).to_owned() else {
                    unreachable!("Has thing must be an entity or relation.")
                };
                let type_ = thing.type_();
                let supertypes = self.instance_type_to_types.get(&type_).cloned().unwrap_or(TYPES_EMPTY);
                let as_tuples = iter::repeat(thing)
                    .zip(supertypes)
                    .map(Ok as _)
                    .filter_map(filter_for_row)
                    .map(isa_to_tuple_type_thing as _);
                let lending_tuples = NaiiveSeekable::new(AsLendingIterator::new(as_tuples));
                Ok(TupleIterator::IsaBounded(SortedTupleIterator::new(
                    lending_tuples,
                    self.tuple_positions.clone(),
                    &self.variable_modes,
                )))
            }
        }
    }
}

impl fmt::Display for IsaExecutor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "[{}], mode={}", &self.isa, &self.iterate_mode)
    }
}

pub(super) struct IsaUnboundedSortedThing {
    inner: MultipleTypeIsaIterator,
    filter_map: Box<IsaFilterMapFn>,
}

impl LendingIterator for IsaUnboundedSortedThing {
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

impl TupleSeekable for IsaUnboundedSortedThing {
    fn seek(&mut self, target: &Tuple<'_>) -> Result<(), Box<ConceptReadError>> {
        let target_thing = target
            .values()
            .get(0)
            .expect("Reverse tuple mapping missing thing")
            .get_thing()
            .unwrap_or_else(|| &answer::MIN_THING_STATIC);
        let target_type = target
            .values()
            .get(1)
            .expect("Reverse tuple mapping missing type")
            .get_type()
            .unwrap_or_else(|| answer::MIN_TYPE_STATIC);
        self.inner.seek(target_thing, target_type)
    }
}

pub(super) struct MultipleTypeIsaIterator {
    object_iters: Vec<IsaObjectIterator>,
    attribute_iters: Vec<IsaAttributeIterator>,
}

impl MultipleTypeIsaIterator {
    pub(super) fn new(mut objects: Vec<IsaObjectIterator>, mut attributes: Vec<IsaAttributeIterator>) -> Self {
        objects.reverse(); // will operate over the iterators in reverse, so we can pop in order while Seeking
        attributes.reverse();
        Self { object_iters: objects, attribute_iters: attributes }
    }

    fn seek(&mut self, target_thing: &Thing, target_type: Type) -> Result<(), Box<ConceptReadError>> {
        match target_thing {
            Thing::Entity(_) | Thing::Relation(_) => {
                let mut first_comparison = true;
                while let Some(object_iter) = self.object_iters.last_mut() {
                    let cmp_type = object_iter.iterator_type.cmp(&target_type);
                    match cmp_type {
                        Ordering::Less => {
                            self.object_iters.pop();
                        }
                        Ordering::Equal => return object_iter.seek(target_thing, target_type),
                        Ordering::Greater => {
                            if first_comparison {
                                unreachable!(
                                    "This iterator's next value is ahead of the target (seek target is behind)"
                                );
                            } else {
                                return Ok(());
                            }
                        }
                    };
                    first_comparison = false;
                }
                if self.attribute_iters.is_empty() {
                    Ok(())
                } else {
                    if first_comparison {
                        unreachable!("This iterator's next value is ahead of the target (seek target is behind)");
                    } else {
                        Ok(())
                    }
                }
            }
            Thing::Attribute(_) => {
                self.object_iters.clear();
                let mut first_comparison = true;
                while let Some(attribute_iter) = self.attribute_iters.last_mut() {
                    let cmp_type = attribute_iter.iterator_type.cmp(&target_type);
                    match cmp_type {
                        Ordering::Less => self.attribute_iters.pop(),
                        Ordering::Equal => {
                            attribute_iter.seek(target_thing, target_type)?;
                            return Ok(());
                        }
                        Ordering::Greater => {
                            if first_comparison {
                                unreachable!("Iterator is ahead of seek target")
                            } else {
                                return Ok(());
                            }
                        }
                    };
                    first_comparison = false;
                }
                Ok(())
            }
        }
    }
}

impl Iterator for MultipleTypeIsaIterator {
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

struct IsaObjectIterator {
    objects: InstanceIterator<Object>,
    iterator_type: Type,
    types: Vec<Type>,

    active_object: Option<Object>,
    next_type_index: usize,
}

impl IsaObjectIterator {
    fn new(objects: InstanceIterator<Object>, iterator_type: Type, types: Vec<Type>) -> Self {
        Self { objects, iterator_type, types, active_object: None, next_type_index: 0 }
    }

    fn seek(&mut self, target_thing: &Thing, target_type: Type) -> Result<(), Box<ConceptReadError>> {
        let target_object = target_thing.get_object().unwrap_or_else(|| Object::MIN);
        let mut must_seek_objects = true;
        if let Some(active) = self.active_object.as_ref() {
            match active.cmp(&target_object) {
                Ordering::Less => {
                    // reset the active object, and index in the types list, which must be re-found
                    let _ = self.active_object.take();
                    self.next_type_index = 0;
                }
                Ordering::Equal => {
                    // no need to seek objects, but must find new index in types list
                    must_seek_objects = false;
                }
                Ordering::Greater => {
                    unreachable!("Seek target is behind current iterator.")
                }
            }
        }

        if must_seek_objects {
            self.objects.seek(&target_object)?;
        }

        // must be at an equal object here. Find equal or greater type, otherwise it's an illegal state
        if let Some((idx, _)) = self.types.iter().enumerate().find(|&(_, &ty)| ty >= target_type) {
            self.next_type_index = idx;
            match self.types[idx].cmp(&target_type) {
                Ordering::Less => unreachable!("Impossible due to preceding search."),
                Ordering::Equal | Ordering::Greater => return Ok(()),
            }
        } else {
            return unreachable!("Post-seek comparison must be Equal or Greater, found Less.");
        }
    }
}

impl Iterator for IsaObjectIterator {
    type Item = Result<(Thing, Type), Box<ConceptReadError>>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(object) = self.active_object {
            if self.next_type_index < self.types.len() {
                let item = (Thing::from(object), self.types[self.next_type_index]);
                self.next_type_index += 1;
                return Some(Ok(item));
            } else {
                self.next_type_index = 0;
                self.active_object = None;
            }
        }
        let object = match Iterator::next(&mut self.objects)? {
            Ok(object) => object,
            Err(err) => return Some(Err(err)),
        };
        self.active_object = Some(object);
        let item = (Thing::from(object), self.types[self.next_type_index]);
        self.next_type_index += 1;
        Some(Ok(item))
    }
}

struct IsaAttributeIterator {
    attributes: AttributeIterator<InstanceIterator<Attribute>>,
    iterator_type: Type,
    types: Vec<Type>,

    active_attribute: Option<Attribute>,
    next_type_index: usize,
}

impl IsaAttributeIterator {
    fn new(attributes: AttributeIterator<InstanceIterator<Attribute>>, iterator_type: Type, types: Vec<Type>) -> Self {
        Self { attributes, iterator_type, types, active_attribute: None, next_type_index: 0 }
    }

    fn seek(&mut self, target_attribute: &Thing, target_type: Type) -> Result<Option<Ordering>, Box<ConceptReadError>> {
        let target_attribute = target_attribute.get_attribute().unwrap_or_else(|| &attribute::MIN_STATIC);
        let mut must_seek_attributes = true;
        if let Some(active) = self.active_attribute.as_ref() {
            match active.cmp(&target_attribute) {
                Ordering::Less => {
                    // reset the active attribute, and index in the types list, which must be re-found
                    let _ = self.active_attribute.take();
                    self.next_type_index = 0;
                }
                Ordering::Equal => {
                    // no need to seek objects, but must find new index in types list
                    must_seek_attributes = false;
                }
                Ordering::Greater => {
                    unreachable!("Seek target is behind current iterator.")
                }
            }
        }

        if must_seek_attributes {
            self.attributes.seek(target_attribute);
            let cmp_after_seek = self.attributes.peek().transpose()?.map(|peek| peek.cmp(target_attribute));
            match cmp_after_seek {
                None => return Ok(None),
                Some(Ordering::Less) => unreachable!("Post-seek comparison must be Equal or Greater, found Less."),
                Some(Ordering::Equal) => {}
                Some(Ordering::Greater) => return Ok(Some(Ordering::Greater)),
            }
        }

        // must be at an equal object here. Find equal or greater type, otherwise it's an illegal state
        if let Some((idx, _)) = self.types.iter().enumerate().find(|&(_, &ty)| ty >= target_type) {
            self.next_type_index = idx;
            match self.types[idx].cmp(&target_type) {
                Ordering::Less => unreachable!("Impossible due to preceding search."),
                Ordering::Equal => return Ok(Some(Ordering::Equal)),
                Ordering::Greater => return Ok(Some(Ordering::Greater)),
            }
        } else {
            return unreachable!("Post-seek comparison must be Equal or Greater, found Less.");
        }
    }
}

impl Iterator for IsaAttributeIterator {
    type Item = Result<(Thing, Type), Box<ConceptReadError>>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(attribute) = &self.active_attribute {
            if self.next_type_index < self.types.len() {
                let item = (Thing::from(attribute.clone()), self.types[self.next_type_index]);
                self.next_type_index += 1;
                return Some(Ok(item));
            } else {
                self.next_type_index = 0;
                self.active_attribute = None;
            }
        }
        let attribute = match self.attributes.next()? {
            Ok(attribute) => attribute,
            Err(err) => return Some(Err(err)),
        };
        self.active_attribute = Some(attribute.clone());
        let item = Ok((Thing::from(attribute), self.types[self.next_type_index]));
        self.next_type_index += 1;
        Some(item)
    }
}

pub(super) fn instances_of_all_types_chained(
    snapshot: &impl ReadableSnapshot,
    thing_manager: &ThingManager,
    instance_types_to_types: &BTreeMap<Type, Vec<Type>>,
    isa_kind: IsaKind,
    instance_values_range: (Bound<Value<'_>>, Bound<Value<'_>>),
    storage_counters: StorageCounters,
) -> Result<MultipleTypeIsaIterator, Box<ConceptReadError>> {
    // TODO: this method contains a lot of heap allocations - we clone the Vec<Type> each time!

    // object types and attribute types will continue to be sorted, based on their source in the BTreeMap
    let (attribute_types, object_types) =
        instance_types_to_types.iter().partition::<Vec<_>, _>(|(type_, _)| matches!(type_, Type::Attribute(_)));

    let object_iters = object_types
        .into_iter()
        .map(|(&type_, types)| {
            let returned_types = if matches!(isa_kind, IsaKind::Subtype) { types.clone() } else { vec![type_] };
            IsaObjectIterator::new(
                thing_manager.get_objects_in(snapshot, type_.as_object_type(), storage_counters.clone()),
                type_,
                returned_types,
            )
        })
        .collect();

    let type_manager = thing_manager.type_manager();
    let attribute_iters = attribute_types
        .into_iter()
        // TODO: we shouldn't really filter out errors here, but presumably a ConceptReadError will crop up elsewhere too if it happens here
        .filter(|(type_, _)| {
            type_.as_attribute_type().get_value_type(snapshot, type_manager).is_ok_and(|vt| vt.is_some())
        })
        .map(|(&type_, types)| {
            let returned_types = if matches!(isa_kind, IsaKind::Subtype) { types.clone() } else { vec![type_] };
            thing_manager
                .get_attributes_in_range(
                    snapshot,
                    type_.as_attribute_type(),
                    &instance_values_range,
                    storage_counters.clone(),
                )
                .map(|iterator| IsaAttributeIterator::new(iterator, type_, returned_types))
        })
        .try_collect()?;

    Ok(MultipleTypeIsaIterator::new(object_iters, attribute_iters))
}
