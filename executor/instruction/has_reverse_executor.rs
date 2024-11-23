/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet, Bound, HashMap},
    fmt,
    sync::{Arc, OnceLock},
    vec,
};

use answer::Type;
use compiler::{executable::match_::instructions::thing::HasReverseInstruction, ExecutorVariable};
use concept::{
    error::ConceptReadError,
    thing::{attribute::Attribute, has::Has, object::HasReverseIterator, thing_manager::ThingManager},
    type_::attribute_type::AttributeType,
};
use encoding::value::value::Value;
use itertools::{Itertools, MinMaxResult};
use concept::type_::object_type::ObjectType;
use lending_iterator::{adaptors::Flatten, kmerge::KMergeBy, AsHkt, AsLendingIterator, LendingIterator, Peekable};
use resource::constants::traversal::CONSTANT_CONCEPT_LIMIT;
use storage::{
    key_range::{KeyRange, RangeEnd, RangeStart},
    snapshot::ReadableSnapshot,
};

use crate::{
    instruction::{
        has_executor::{HasFilterFn, HasOrderingFn, HasTupleIterator, EXTRACT_ATTRIBUTE, EXTRACT_OWNER},
        iterator::{SortedTupleIterator, TupleIterator},
        tuple::{has_to_tuple_attribute_owner, has_to_tuple_owner_attribute, Tuple, TuplePositions},
        BinaryIterateMode, Checker, VariableModes,
    },
    pipeline::stage::ExecutionContext,
    row::MaybeOwnedRow,
};
use crate::instruction::min_max_types;

pub(crate) struct HasReverseExecutor {
    has: ir::pattern::constraint::Has<ExecutorVariable>,
    iterate_mode: BinaryIterateMode,
    variable_modes: VariableModes,
    tuple_positions: TuplePositions,
    attribute_owner_types: Arc<BTreeMap<Type, Vec<Type>>>,
    attribute_owner_types_range: BTreeMap<AttributeType<'static>, KeyRange<ObjectType<'static>>>,
    owner_type_range: KeyRange<ObjectType<'static>>,
    filter_fn: Arc<HasFilterFn>,
    attribute_cache: OnceLock<Vec<Attribute<'static>>>,
    checker: Checker<(AsHkt![Has<'_>], u64)>,
}

pub(crate) type HasReverseUnboundedSortedAttribute = HasTupleIterator<MultipleTypeHasReverseIterator>;
pub(crate) type HasReverseUnboundedSortedOwnerMerged = HasTupleIterator<KMergeBy<HasReverseIterator, HasOrderingFn>>;
pub(crate) type HasReverseUnboundedSortedOwnerSingle = HasTupleIterator<HasReverseIterator>;
pub(crate) type HasReverseBoundedSortedOwner = HasTupleIterator<HasReverseIterator>;
type MultipleTypeHasReverseIterator = Flatten<AsLendingIterator<vec::IntoIter<HasReverseIterator>>>;

impl HasReverseExecutor {
    pub(crate) fn new(
        has_reverse: HasReverseInstruction<ExecutorVariable>,
        variable_modes: VariableModes,
        sort_by: ExecutorVariable,
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
    ) -> Result<Self, Box<ConceptReadError>> {
        debug_assert!(!variable_modes.all_inputs());
        let attribute_owner_types = has_reverse.attribute_to_owner_types().clone();
        debug_assert!(!attribute_owner_types.is_empty());
        let owner_types = has_reverse.owner_types().clone();
        let HasReverseInstruction { has, checks, .. } = has_reverse;
        let iterate_mode = BinaryIterateMode::new(has.attribute(), has.owner(), &variable_modes, sort_by);
        let filter_fn = match iterate_mode {
            BinaryIterateMode::Unbound => create_has_filter_attributes_owners(attribute_owner_types.clone()),
            BinaryIterateMode::UnboundInverted | BinaryIterateMode::BoundFrom => {
                create_has_filter_owners(owner_types.clone())
            }
        };

        let owner = has.owner().as_variable().unwrap();
        let attribute = has.attribute().as_variable().unwrap();

        let attribute_owner_types_range: BTreeMap<AttributeType<'static>, KeyRange<ObjectType<'static>>> = attribute_owner_types
            .iter()
            .map(|(type_, owner_types)| {
                let (min_owner_type, max_owner_type) = min_max_types(&*owner_types);
                (
                    type_.as_attribute_type(),
                    KeyRange::new_variable_width(
                        RangeStart::Inclusive(min_owner_type.as_object_type()),
                        RangeEnd::EndPrefixInclusive(max_owner_type.as_object_type()),
                    )
                )
            })
            .collect();

        let (min_owner_type, max_owner_type) = min_max_types(&*owner_types);
        let owner_type_range = KeyRange::new_variable_width(
            RangeStart::Inclusive(min_owner_type.as_object_type()),
            RangeEnd::EndPrefixInclusive(max_owner_type.as_object_type()),
        );

        let output_tuple_positions = match iterate_mode {
            BinaryIterateMode::Unbound => TuplePositions::Pair([Some(attribute), Some(owner)]),
            _ => TuplePositions::Pair([Some(owner), Some(attribute)]),
        };

        let checker = Checker::<(Has<'_>, _)>::new(
            checks,
            HashMap::from([(owner, EXTRACT_OWNER), (attribute, EXTRACT_ATTRIBUTE)]),
        );

        Ok(Self {
            has,
            iterate_mode,
            variable_modes,
            tuple_positions: output_tuple_positions,
            attribute_owner_types,
            attribute_owner_types_range,
            owner_type_range,
            filter_fn,
            attribute_cache: OnceLock::new(),
            checker,
        })
    }

    pub(crate) fn get_iterator(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: MaybeOwnedRow<'_>,
    ) -> Result<TupleIterator, Box<ConceptReadError>> {
        if self.iterate_mode.is_unbound_inverted() && self.attribute_cache.get().is_none() {
            // one-off initialisation of the cache of constants as we require the Parameters
            let value_range =
                self.checker.value_range_for(context, None, self.has.attribute().as_variable().unwrap())?;
            let mut cache = Vec::new();
            for type_ in self.attribute_owner_types.keys() {
                let instances: Vec<Attribute<'static>> = context
                    .thing_manager
                    .get_attributes_in_range(context.snapshot.as_ref(), type_.as_attribute_type(), &value_range)?
                    .map_static(|result| Ok::<_, Box<_>>(result?.clone().into_owned()))
                    .try_collect()?;
                cache.extend(instances);
            }
            #[cfg(debug_assertions)]
            if cache.len() < CONSTANT_CONCEPT_LIMIT {
                eprintln!("DEBUG_ASSERT_FAILURE: cache.len() > CONSTANT_CONCEPT_LIMIT");
            }
            self.attribute_cache.get_or_init(|| cache);
        }

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
                let range = self.checker.value_range_for(
                    context,
                    Some(row.as_reference()),
                    self.has.attribute().as_variable().unwrap(),
                )?;
                let as_tuples: HasTupleIterator<MultipleTypeHasReverseIterator> = Self::all_has_reverse_chained(
                    snapshot,
                    thing_manager,
                    &self.attribute_owner_types_range,
                    range,
                )?
                .try_filter::<_, HasFilterFn, (Has<'_>, _), _>(filter_for_row)
                .map::<Result<Tuple<'_>, _>, _>(has_to_tuple_attribute_owner);
                Ok(TupleIterator::HasReverseUnbounded(SortedTupleIterator::new(
                    as_tuples,
                    self.tuple_positions.clone(),
                    &self.variable_modes,
                )))
            }
            BinaryIterateMode::UnboundInverted => {
                debug_assert!(self.attribute_cache.get().is_some());
                if self.attribute_cache.get().unwrap().len() == 1 {
                    let attr = &self.attribute_cache.get().unwrap()[0];
                    // no heap allocs needed if there is only 1 iterator
                    let iterator = thing_manager
                        .get_has_reverse_by_attribute_and_owner_type_range(
                            snapshot,
                            attr.as_reference(),
                            &self.owner_type_range,
                        )
                        .try_filter::<_, HasFilterFn, (Has<'_>, _), _>(filter_for_row);
                    let as_tuples: HasReverseUnboundedSortedOwnerSingle =
                        iterator.map::<Result<Tuple<'_>, _>, _>(has_to_tuple_owner_attribute);
                    Ok(TupleIterator::HasReverseUnboundedInvertedSingle(SortedTupleIterator::new(
                        as_tuples,
                        self.tuple_positions.clone(),
                        &self.variable_modes,
                    )))
                } else {
                    // // TODO: we could create a reusable space for these temporarily held iterators so we don't have allocate again before the merging iterator
                    let attributes = self.attribute_cache.get().unwrap().iter();
                    let iterators = attributes.map(|attribute| {
                        Peekable::new(thing_manager.get_has_reverse_by_attribute_and_owner_type_range(
                            snapshot,
                            attribute.as_reference(),
                            &self.owner_type_range,
                        ))
                    });

                    // note: this will always have to heap alloc, if we use don't have a re-usable/small-vec'ed priority queue somewhere
                    let merged: KMergeBy<HasReverseIterator, HasOrderingFn> =
                        KMergeBy::new(iterators, compare_has_by_owner_then_attribute);
                    let as_tuples: HasReverseUnboundedSortedOwnerMerged = merged
                        .try_filter::<_, HasFilterFn, (Has<'_>, _), _>(filter_for_row)
                        .map::<Result<Tuple<'_>, _>, _>(has_to_tuple_owner_attribute);
                    Ok(TupleIterator::HasReverseUnboundedInvertedMerged(SortedTupleIterator::new(
                        as_tuples,
                        self.tuple_positions.clone(),
                        &self.variable_modes,
                    )))
                }
            }
            BinaryIterateMode::BoundFrom => {
                let attribute = self.has.attribute().as_variable().unwrap().as_position().unwrap();
                debug_assert!(row.len() > attribute.as_usize());
                let attribute = row.get(attribute);
                let iterator = thing_manager.get_has_reverse_by_attribute_and_owner_type_range(
                    snapshot,
                    attribute.as_thing().as_attribute(),
                    &self.owner_type_range,
                );
                let filtered = iterator.try_filter::<_, HasFilterFn, (Has<'_>, _), _>(filter_for_row);
                let as_tuples: HasReverseBoundedSortedOwner =
                    filtered.map::<Result<Tuple<'_>, _>, _>(has_to_tuple_owner_attribute);
                Ok(TupleIterator::HasReverseBounded(SortedTupleIterator::new(
                    as_tuples,
                    self.tuple_positions.clone(),
                    &self.variable_modes,
                )))
            }
        }
    }

    fn all_has_reverse_chained<'a>(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        attribute_type_owner_range: &BTreeMap<AttributeType<'static>, KeyRange<ObjectType<'static>>>,
        attribute_values_range: (Bound<Value<'_>>, Bound<Value<'_>>),
    ) -> Result<MultipleTypeHasReverseIterator, Box<ConceptReadError>> {
        let type_manager = thing_manager.type_manager();
        let iterators: Vec<_> = attribute_type_owner_range
            .iter()
            // TODO: we shouldn't really filter out errors here, but presumably a ConceptReadError will crop up elsewhere too if it happens here
            .filter(|(attribute_type, owner_type_range)| {
                attribute_type.get_value_type(snapshot, type_manager).is_ok_and(|vt| vt.is_some())
            })
            .map(|(attribute_type, owner_types)| {
                thing_manager.get_has_reverse_in_range(snapshot, attribute_type.clone(), &attribute_values_range, owner_types)
            })
            .try_collect()?;
        Ok(AsLendingIterator::new(iterators).flatten())
    }
}
impl fmt::Display for HasReverseExecutor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Reverse[{}], mode={}", &self.has, &self.iterate_mode)
    }
}

fn create_has_filter_attributes_owners(attributes_owner_types: Arc<BTreeMap<Type, Vec<Type>>>) -> Arc<HasFilterFn> {
    Arc::new(move |result| match result {
        Ok((has, _)) => match attributes_owner_types.get(&Type::from(has.attribute().type_())) {
            Some(owner_types) => Ok(owner_types.contains(&Type::from(has.owner().type_()))),
            None => Ok(false),
        },
        Err(err) => Err(err.clone()),
    })
}

fn create_has_filter_owners(owner_types: Arc<BTreeSet<Type>>) -> Arc<HasFilterFn> {
    Arc::new(move |result| match result {
        Ok((has, _)) => Ok(owner_types.contains(&Type::from(has.owner().type_()))),
        Err(err) => Err(err.clone()),
    })
}

fn compare_has_by_owner_then_attribute(
    pair: (&Result<(Has<'_>, u64), Box<ConceptReadError>>, &Result<(Has<'_>, u64), Box<ConceptReadError>>),
) -> Ordering {
    if let (Ok((has_1, _)), Ok((has_2, _))) = pair {
        (has_2.owner(), has_1.attribute()).cmp(&(has_2.owner(), has_2.attribute()))
    } else {
        Ordering::Equal
    }
}
