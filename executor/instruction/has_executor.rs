/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::{Arc, OnceLock},
};

use answer::{variable_value::VariableValue, Thing, Type};
use compiler::{executable::match_::instructions::thing::HasInstruction, ExecutorVariable};
use concept::{
    error::ConceptReadError,
    thing::{
        has::Has,
        object::{HasIterator, Object, ObjectAPI},
        thing_manager::ThingManager,
    },
};
use lending_iterator::{
    adaptors::{Map, TryFilter},
    kmerge::KMergeBy,
    AsHkt, LendingIterator, Peekable,
};
use resource::constants::traversal::CONSTANT_CONCEPT_LIMIT;
use storage::{
    key_range::{KeyRange, RangeEnd, RangeStart},
    snapshot::ReadableSnapshot,
};

use crate::{
    instruction::{
        iterator::{SortedTupleIterator, TupleIterator},
        tuple::{
            has_to_tuple_attribute_owner, has_to_tuple_owner_attribute, HasToTupleFn, Tuple, TuplePositions,
            TupleResult,
        },
        BinaryIterateMode, Checker, FilterFn, VariableModes,
    },
    pipeline::stage::ExecutionContext,
    row::MaybeOwnedRow,
};

pub(crate) struct HasExecutor {
    has: ir::pattern::constraint::Has<ExecutorVariable>,
    iterate_mode: BinaryIterateMode,
    variable_modes: VariableModes,
    tuple_positions: TuplePositions,
    owner_attribute_types: Arc<BTreeMap<Type, Vec<Type>>>,
    attribute_types: Arc<BTreeSet<Type>>,
    filter_fn: Arc<HasFilterFn>,
    owner_cache: Option<Vec<Object<'static>>>,
    checker: Checker<(AsHkt![Has<'_>], u64)>,
}

pub(super) type HasTupleIterator<I> = Map<
    TryFilter<I, Box<HasFilterFn>, (AsHkt![Has<'_>], u64), ConceptReadError>,
    HasToTupleFn,
    AsHkt![TupleResult<'_>],
>;

pub(crate) type HasUnboundedSortedOwner = HasTupleIterator<HasIterator>;
pub(crate) type HasUnboundedSortedAttributeMerged = HasTupleIterator<KMergeBy<HasIterator, HasOrderingFn>>;
pub(crate) type HasUnboundedSortedAttributeSingle = HasTupleIterator<HasIterator>;
pub(crate) type HasBoundedSortedAttribute = HasTupleIterator<HasIterator>;

pub(super) type HasFilterFn = FilterFn<(AsHkt![Has<'_>], u64)>;

type HasVariableValueExtractor = for<'a, 'b> fn(&'a (Has<'b>, u64)) -> VariableValue<'a>;
pub(super) const EXTRACT_OWNER: HasVariableValueExtractor = |(has, _)| VariableValue::Thing(Thing::from(has.owner()));
pub(super) const EXTRACT_ATTRIBUTE: HasVariableValueExtractor =
    |(has, _)| VariableValue::Thing(Thing::Attribute(has.attribute()));

pub(crate) type HasOrderingFn = for<'a, 'b> fn(
    (&'a Result<(Has<'a>, u64), ConceptReadError>, &'b Result<(Has<'b>, u64), ConceptReadError>),
) -> Ordering;

impl HasExecutor {
    pub(crate) fn new<Snapshot: ReadableSnapshot>(
        has: HasInstruction<ExecutorVariable>,
        variable_modes: VariableModes,
        sort_by: ExecutorVariable,
        snapshot: &Snapshot,
        thing_manager: &ThingManager,
    ) -> Result<Self, ConceptReadError> {
        debug_assert!(!variable_modes.all_inputs());
        let owner_attribute_types = has.owner_to_attribute_types().clone();
        debug_assert!(owner_attribute_types.len() > 0);
        let attribute_types = has.attribute_types().clone();
        let HasInstruction { has, checks, .. } = has;
        let iterate_mode = BinaryIterateMode::new(has.owner(), has.attribute(), &variable_modes, sort_by);
        let filter_fn = match iterate_mode {
            BinaryIterateMode::Unbound => create_has_filter_owners_attributes(owner_attribute_types.clone()),
            BinaryIterateMode::UnboundInverted | BinaryIterateMode::BoundFrom => {
                create_has_filter_attributes(attribute_types.clone())
            }
        };

        let owner = has.owner().as_variable().unwrap();
        let attribute = has.attribute().as_variable().unwrap();

        let output_tuple_positions = match iterate_mode {
            BinaryIterateMode::Unbound => TuplePositions::Pair([Some(owner), Some(attribute)]),
            _ => TuplePositions::Pair([Some(attribute), Some(owner)]),
        };

        let checker = Checker::<(Has<'_>, _)>::new(
            checks,
            HashMap::from([(owner, EXTRACT_OWNER), (attribute, EXTRACT_ATTRIBUTE)]),
        );

        let owner_cache = if iterate_mode == BinaryIterateMode::UnboundInverted {
            let mut cache = Vec::new();
            for type_ in owner_attribute_types.keys() {
                let instances: Vec<Object<'static>> = thing_manager
                    .get_objects_in(snapshot, type_.as_object_type())
                    .map_static(|result| Ok(result?.clone().into_owned()))
                    .try_collect()?;
                cache.extend(instances);
            }
            debug_assert!(cache.len() < CONSTANT_CONCEPT_LIMIT);
            Some(cache)
        } else {
            None
        };

        Ok(Self {
            has,
            iterate_mode,
            variable_modes,
            tuple_positions: output_tuple_positions,
            owner_attribute_types,
            attribute_types,
            filter_fn,
            owner_cache,
            checker,
        })
    }

    pub(crate) fn get_iterator(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: MaybeOwnedRow<'_>,
    ) -> Result<TupleIterator, ConceptReadError> {
        let filter = self.filter_fn.clone();
        let check = self.checker.filter_for_row(context, &row);
        let filter_for_row: Box<HasFilterFn> = Box::new(move |item| match filter(item) {
            Ok(true) => check(item),
            fail => fail,
        });

        let snapshot = &**context.snapshot();
        let thing_manager = context.thing_manager();

        match self.iterate_mode {
            BinaryIterateMode::Unbound => {
                let first_from_type = self.owner_attribute_types.first_key_value().unwrap().0;
                let last_key_from_type = self.owner_attribute_types.last_key_value().unwrap().0;
                let key_range = KeyRange::new_variable_width(
                    RangeStart::Inclusive(first_from_type.as_object_type()),
                    RangeEnd::EndPrefixInclusive(last_key_from_type.as_object_type()),
                );
                // TODO: we could cache the range byte arrays computed inside the thing_manager, for this case
                let as_tuples: HasUnboundedSortedOwner = thing_manager
                    .get_has_from_owner_type_range_unordered(snapshot, key_range)
                    .try_filter::<_, HasFilterFn, (Has<'_>, _), _>(filter_for_row)
                    .map::<Result<Tuple<'_>, _>, _>(has_to_tuple_owner_attribute);
                Ok(TupleIterator::HasUnbounded(SortedTupleIterator::new(
                    as_tuples,
                    self.tuple_positions.clone(),
                    &self.variable_modes,
                )))
            }
            BinaryIterateMode::UnboundInverted => {
                debug_assert!(self.owner_cache.is_some());

                if let Some([owner]) = self.owner_cache.as_deref() {
                    // no heap allocs needed if there is only 1 iterator
                    let iterator = owner.get_has_types_range_unordered(
                        snapshot,
                        thing_manager,
                        // TODO: this should be just the types owned by the one instance's type in the cache!
                        self.attribute_types.iter().map(|t| t.as_attribute_type()),
                    )?;
                    let as_tuples: HasUnboundedSortedAttributeSingle = iterator
                        .try_filter::<_, HasFilterFn, (Has<'_>, _), _>(filter_for_row)
                        .map::<Result<Tuple<'_>, _>, _>(has_to_tuple_attribute_owner);
                    Ok(TupleIterator::HasUnboundedInvertedSingle(SortedTupleIterator::new(
                        as_tuples,
                        self.tuple_positions.clone(),
                        &self.variable_modes,
                    )))
                } else {
                    // TODO: we could create a reusable space for these temporarily held iterators
                    //       so we don't have allocate again before the merging iterator
                    let owners = self.owner_cache.as_ref().unwrap().iter();
                    let iterators = owners
                        .map(|object| {
                            Ok(Peekable::new(object.get_has_types_range_unordered(
                                snapshot,
                                thing_manager,
                                self.attribute_types.iter().map(|ty| ty.as_attribute_type()),
                            )?))
                        })
                        .collect::<Result<Vec<_>, _>>()?;

                    // note: this will always have to heap alloc, if we use don't have a re-usable/small-vec'ed priority queue somewhere
                    let merged: KMergeBy<HasIterator, HasOrderingFn> =
                        KMergeBy::new(iterators, compare_has_by_attribute_then_owner);
                    let as_tuples: HasUnboundedSortedAttributeMerged = merged
                        .try_filter::<_, HasFilterFn, (Has<'_>, _), _>(filter_for_row)
                        .map::<Result<Tuple<'_>, _>, _>(has_to_tuple_attribute_owner);
                    Ok(TupleIterator::HasUnboundedInvertedMerged(SortedTupleIterator::new(
                        as_tuples,
                        self.tuple_positions.clone(),
                        &self.variable_modes,
                    )))
                }
            }
            BinaryIterateMode::BoundFrom => {
                let owner = self.has.owner().as_variable().unwrap().as_position().unwrap();
                debug_assert!(row.len() > owner.as_usize());
                let iterator = match row.get(owner) {
                    VariableValue::Thing(Thing::Entity(entity)) => entity.get_has_types_range_unordered(
                        snapshot,
                        thing_manager,
                        self.attribute_types.iter().map(|t| t.as_attribute_type()),
                    )?,
                    VariableValue::Thing(Thing::Relation(relation)) => relation.get_has_types_range_unordered(
                        snapshot,
                        thing_manager,
                        self.attribute_types.iter().map(|t| t.as_attribute_type()),
                    )?,
                    _ => unreachable!("Has owner must be an entity or relation."),
                };
                let as_tuples: HasBoundedSortedAttribute = iterator
                    .try_filter::<_, HasFilterFn, (Has<'_>, _), _>(filter_for_row)
                    .map::<Result<Tuple<'_>, _>, _>(has_to_tuple_attribute_owner);
                Ok(TupleIterator::HasBounded(SortedTupleIterator::new(
                    as_tuples,
                    self.tuple_positions.clone(),
                    &self.variable_modes,
                )))
            }
        }
    }
}

fn create_has_filter_owners_attributes(owner_attribute_types: Arc<BTreeMap<Type, Vec<Type>>>) -> Arc<HasFilterFn> {
    Arc::new(move |result| match result {
        Ok((has, _)) => match owner_attribute_types.get(&Type::from(has.owner().type_())) {
            Some(attribute_types) => Ok(attribute_types.contains(&Type::Attribute(has.attribute().type_()))),
            None => Ok(false),
        },
        Err(err) => Err(err.clone()),
    })
}

fn create_has_filter_attributes(attribute_types: Arc<BTreeSet<Type>>) -> Arc<HasFilterFn> {
    Arc::new(move |result| match result {
        Ok((has, _)) => Ok(attribute_types.contains(&Type::Attribute(has.attribute().type_()))),
        Err(err) => Err(err.clone()),
    })
}

fn compare_has_by_attribute_then_owner(
    pair: (&Result<(Has<'_>, u64), ConceptReadError>, &Result<(Has<'_>, u64), ConceptReadError>),
) -> Ordering {
    if let (Ok((has_1, _)), Ok((has_2, _))) = pair {
        (has_1.attribute(), has_2.owner()).cmp(&(has_2.attribute(), has_2.owner()))
    } else {
        Ordering::Equal
    }
}
