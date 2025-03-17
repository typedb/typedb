/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet, HashMap},
    fmt,
    iter::Iterator,
    ops::Bound,
    sync::Arc,
};

use answer::{variable_value::VariableValue, Thing, Type};
use compiler::{executable::match_::instructions::thing::HasInstruction, ExecutorVariable};
use concept::{
    error::ConceptReadError,
    thing::{
        attribute::Attribute,
        has::Has,
        object::{HasIterator, Object, ObjectAPI},
        thing_manager::ThingManager,
        ThingAPI,
    },
    type_::{attribute_type::AttributeType, object_type::ObjectType},
};
use encoding::{graph::type_::vertex::TypeVertexEncoding, value::value_type::ValueTypeCategory};
use itertools::Itertools;
use lending_iterator::{kmerge::KMergeBy, LendingIterator, Peekable};
use primitive::Bounds;
use resource::{constants::traversal::CONSTANT_CONCEPT_LIMIT, profile::StorageCounters};
use storage::snapshot::ReadableSnapshot;

use crate::{
    instruction::{
        iterator::{SortedTupleIterator, TupleIterator, TupleSeekable},
        min_max_types,
        tuple::{
            has_to_tuple_attribute_owner, has_to_tuple_owner_attribute, tuple_attribute_owner_to_has_canonical,
            tuple_owner_attribute_to_has_canonical, unsafe_compare_result_tuple, HasToTupleFn, Tuple, TupleOrderingFn,
            TuplePositions, TupleResult, TupleToHasFn,
        },
        BinaryIterateMode, Checker, FilterFn, FilterMapUnchangedFn, VariableModes,
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
    owner_type_range: Bounds<ObjectType>,
    attribute_type_range: Bounds<AttributeType>,
    ordered_value_type_categories: Vec<ValueTypeCategory>,
    filter_fn: Arc<HasFilterFn>,
    owner_cache: Option<Vec<Object>>,
    checker: Checker<(Has, u64)>,
}

impl fmt::Debug for HasExecutor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "HasExecutor")
    }
}

pub(crate) type HasTupleIteratorSingle = HasTupleIterator<HasIterator>;
pub(crate) type HasTupleIteratorMerged = KMergeBy<HasTupleIterator<HasIterator>, TupleOrderingFn>;

pub(super) type HasFilterFn = FilterFn<(Has, u64)>;
pub(super) type HasFilterMapFn = FilterMapUnchangedFn<(Has, u64)>;

type HasVariableValueExtractor = for<'a, 'b> fn(&'a (Has, u64)) -> VariableValue<'a>;
pub(super) const EXTRACT_OWNER: HasVariableValueExtractor = |(has, _)| VariableValue::Thing(Thing::from(has.owner()));
pub(super) const EXTRACT_ATTRIBUTE: HasVariableValueExtractor =
    |(has, _)| VariableValue::Thing(Thing::Attribute(has.attribute()));

impl HasExecutor {
    pub(crate) fn new<Snapshot: ReadableSnapshot>(
        has: HasInstruction<ExecutorVariable>,
        variable_modes: VariableModes,
        sort_by: ExecutorVariable,
        snapshot: &Snapshot,
        thing_manager: &ThingManager,
    ) -> Result<Self, Box<ConceptReadError>> {
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

        let checker =
            Checker::<(Has, _)>::new(checks, HashMap::from([(owner, EXTRACT_OWNER), (attribute, EXTRACT_ATTRIBUTE)]));

        let (min_attribute_type, max_attribute_type) = min_max_types(&*attribute_types);
        let attribute_type_range = (
            Bound::Included(min_attribute_type.as_attribute_type()),
            Bound::Included(max_attribute_type.as_attribute_type()),
        );

        let owner_type_range = (
            Bound::Included(owner_attribute_types.first_key_value().unwrap().0.as_object_type()),
            Bound::Included(owner_attribute_types.last_key_value().unwrap().0.as_object_type()),
        );

        let owner_cache = if iterate_mode == BinaryIterateMode::UnboundInverted {
            let mut cache = Vec::new();
            for type_ in owner_attribute_types.keys() {
                let instances: Vec<_> = Itertools::try_collect(thing_manager.get_objects_in(
                    snapshot,
                    type_.as_object_type(),
                    StorageCounters::DISABLED,
                ))?;
                cache.extend(instances);
            }
            #[cfg(debug_assertions)]
            if cache.len() < CONSTANT_CONCEPT_LIMIT {
                eprintln!("DEBUG_ASSERT_FAILURE: cache.len() > CONSTANT_CONCEPT_LIMIT");
            }
            Some(cache)
        } else {
            None
        };

        let possible_attribute_value_categories = thing_manager
            .type_manager()
            .get_attribute_types(snapshot)?
            .into_iter()
            .filter_map(|attribute_type| {
                attribute_type
                    .get_value_type_without_source(snapshot, thing_manager.type_manager())
                    .ok()?
                    .map(|value_type| value_type.category())
            })
            .sorted_by_key(|category| category.to_bytes())
            .collect_vec();

        Ok(Self {
            has,
            iterate_mode,
            variable_modes,
            tuple_positions: output_tuple_positions,
            owner_attribute_types,
            owner_type_range,
            attribute_type_range,
            ordered_value_type_categories: possible_attribute_value_categories,
            filter_fn,
            owner_cache,
            checker,
        })
    }

    pub(crate) fn get_iterator(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: MaybeOwnedRow<'_>,
        storage_counters: StorageCounters,
    ) -> Result<TupleIterator, Box<ConceptReadError>> {
        let filter = self.filter_fn.clone();
        let check = self.checker.filter_for_row(context, &row);
        let filter_for_row: Arc<HasFilterMapFn> = Arc::new(move |item| match filter(&item) {
            Ok(true) => match check(&item) {
                Ok(true) | Err(_) => Some(item),
                Ok(false) => None,
            },
            Ok(false) => None,
            Err(_) => Some(item),
        });
        let value_range = self.checker.value_range_for(
            context,
            Some(row.as_reference()),
            self.has.attribute().as_variable().unwrap(),
        )?;

        let snapshot = &**context.snapshot();
        let thing_manager = context.thing_manager();

        match self.iterate_mode {
            BinaryIterateMode::Unbound => {
                // TODO: we could cache the range byte arrays computed inside the thing_manager, for this case

                // TODO: in the HasReverse case, we look up N iterators (one per type) and link them - here we scan and post-filter
                //        we should determine which strategy we want long-term
                let has_iterator: HasIterator = thing_manager.get_has_from_owner_type_range_unordered(
                    snapshot,
                    &self.owner_type_range,
                    storage_counters,
                );
                let as_tuples = HasTupleIterator::new(
                    has_iterator,
                    filter_for_row,
                    has_to_tuple_owner_attribute,
                    tuple_owner_attribute_to_has_canonical,
                    FixedHasBounds::None,
                );
                Ok(TupleIterator::HasSingle(SortedTupleIterator::new(
                    as_tuples,
                    self.tuple_positions.clone(),
                    &self.variable_modes,
                )))
            }
            BinaryIterateMode::UnboundInverted => {
                debug_assert!(self.owner_cache.is_some());
                if let Some([owner]) = self.owner_cache.as_deref() {
                    // no heap allocs needed if there is only 1 iterator
                    let iterator = owner.get_has_types_range_unordered_in_value_types(
                        snapshot,
                        thing_manager,
                        // TODO: this should be just the types owned by the one instance's type in the cache!
                        &self.attribute_type_range,
                        &self.ordered_value_type_categories,
                        &value_range,
                        storage_counters,
                    )?;
                    let as_tuples = HasTupleIterator::new(
                        iterator,
                        filter_for_row,
                        has_to_tuple_attribute_owner,
                        tuple_attribute_owner_to_has_canonical,
                        FixedHasBounds::Owner(*owner),
                    );
                    Ok(TupleIterator::HasSingle(SortedTupleIterator::new(
                        as_tuples,
                        self.tuple_positions.clone(),
                        &self.variable_modes,
                    )))
                } else {
                    // TODO: we could create a reusable space for these temporarily held iterators
                    //       so we don't have allocate again before the merging iterator
                    let owners = self.owner_cache.as_ref().unwrap().iter();
                    let mut iterators = Vec::new();
                    for owner in owners {
                        let iterator = owner.get_has_types_range_unordered_in_value_types(
                            snapshot,
                            thing_manager,
                            &self.attribute_type_range,
                            &self.ordered_value_type_categories,
                            &value_range,
                            storage_counters.clone(),
                        )?;
                        let filter = filter_for_row.clone();
                        let iterator = HasTupleIterator::new(
                            iterator,
                            filter,
                            has_to_tuple_attribute_owner,
                            tuple_attribute_owner_to_has_canonical,
                            FixedHasBounds::Owner(*owner),
                        );
                        iterators.push(iterator);
                    }

                    // note: this will always have to heap alloc, if we use don't have a re-usable/small-vec'ed priority queue somewhere

                    // TODO: this will be brittle, since we're using an unsafe comparison and unwrapping tuple comparisons.
                    let merged_tuples: KMergeBy<HasTupleIterator<HasIterator>, TupleOrderingFn> =
                        KMergeBy::new(iterators, unsafe_compare_result_tuple);
                    Ok(TupleIterator::HasMerged(SortedTupleIterator::new(
                        merged_tuples,
                        self.tuple_positions.clone(),
                        &self.variable_modes,
                    )))
                }
            }
            BinaryIterateMode::BoundFrom => {
                let owner = self.has.owner().as_variable().unwrap().as_position().unwrap();
                debug_assert!(row.len() > owner.as_usize());
                let iterator = match row.get(owner) {
                    VariableValue::Thing(Thing::Entity(entity)) => entity
                        .get_has_types_range_unordered_in_value_types(
                            snapshot,
                            thing_manager,
                            &self.attribute_type_range,
                            &self.ordered_value_type_categories,
                            &value_range,
                            storage_counters,
                        )?,
                    VariableValue::Thing(Thing::Relation(relation)) => relation
                        .get_has_types_range_unordered_in_value_types(
                            snapshot,
                            thing_manager,
                            &self.attribute_type_range,
                            &self.ordered_value_type_categories,
                            &value_range,
                            storage_counters,
                        )?,
                    _ => unreachable!("Has owner must be an entity or relation."),
                };
                let as_tuples = HasTupleIterator::new(
                    iterator,
                    filter_for_row,
                    has_to_tuple_attribute_owner,
                    tuple_attribute_owner_to_has_canonical,
                    FixedHasBounds::Owner(row.get(owner).as_thing().as_object()),
                );
                Ok(TupleIterator::HasSingle(SortedTupleIterator::new(
                    as_tuples,
                    self.tuple_positions.clone(),
                    &self.variable_modes,
                )))
            }
        }
    }
}

impl fmt::Display for HasExecutor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "[{}], mode={}", &self.has, &self.iterate_mode)
    }
}

pub(crate) enum FixedHasBounds {
    None,
    Owner(Object),
    Attribute(Attribute),
}

pub(super) struct HasTupleIterator<Iter: LendingIterator> {
    inner: Peekable<Iter>,
    filter_map: Arc<HasFilterMapFn>,
    to_tuple_fn: HasToTupleFn,
    from_tuple_fn: TupleToHasFn,
    fixed_bounds: FixedHasBounds,
}

impl<Iter> HasTupleIterator<Iter>
where
    Iter: for<'a> lending_iterator::Seekable<Result<(Has, u64), Box<ConceptReadError>>>
        + for<'a> LendingIterator<Item<'a> = Result<(Has, u64), Box<ConceptReadError>>>,
{
    pub(super) fn new(
        inner: Iter,
        filter_map: Arc<HasFilterMapFn>,
        to_tuple_fn: HasToTupleFn,
        from_tuple_fn: TupleToHasFn,
        fixed_bounds: FixedHasBounds,
    ) -> Self {
        Self { inner: Peekable::new(inner), filter_map, to_tuple_fn, from_tuple_fn, fixed_bounds }
    }
}

impl<Iter> LendingIterator for HasTupleIterator<Iter>
where
    Iter: for<'a> lending_iterator::Seekable<Result<(Has, u64), Box<ConceptReadError>>>
        + for<'a> LendingIterator<Item<'a> = Result<(Has, u64), Box<ConceptReadError>>>,
{
    type Item<'a> = TupleResult<'static>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        // TODO: can this be simplified with something like `.by_ref()` on iterators?
        while let Some(next) = self.inner.next() {
            if let Some(filter_mapped) = (self.filter_map)(next) {
                return Some((self.to_tuple_fn)(filter_mapped));
            }
        }
        None
    }
}

impl<Iter> TupleSeekable for HasTupleIterator<Iter>
where
    Iter: for<'a> lending_iterator::Seekable<Result<(Has, u64), Box<ConceptReadError>>>
        + for<'a> LendingIterator<Item<'a> = Result<(Has, u64), Box<ConceptReadError>>>,
{
    fn seek(&mut self, target: &Tuple<'_>) -> Result<(), Box<ConceptReadError>> {
        let target_has = (self.from_tuple_fn)(&target, &self.fixed_bounds);
        let target_pair = (target_has, 0);
        lending_iterator::Seekable::seek(&mut self.inner, &Ok(target_pair.clone()));
        Ok(())
    }
}

fn create_has_filter_owners_attributes(owner_attribute_types: Arc<BTreeMap<Type, Vec<Type>>>) -> Arc<HasFilterFn> {
    Arc::new(move |result| match result {
        Ok((has, _)) => match owner_attribute_types.get(&Type::from(has.owner().type_())) {
            Some(attribute_types) => {
                let attribute_type = has.attribute().type_();
                println!(
                    "Checking if attribute type {:?} is within allowed types {:?}",
                    attribute_type, attribute_types
                );
                Ok(attribute_types.contains(&Type::Attribute(attribute_type)))
            }
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
    (left, right): (&Result<(Has, u64), Box<ConceptReadError>>, &Result<(Has, u64), Box<ConceptReadError>>),
) -> Ordering {
    if let (Ok((has_1, _)), Ok((has_2, _))) = (left, right) {
        (has_1.attribute(), has_1.owner()).cmp(&(has_2.attribute(), has_2.owner()))
    } else {
        // arbitrary
        Ordering::Equal
    }
}
