/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{BTreeMap, BTreeSet, Bound, HashMap},
    fmt, iter,
    sync::{Arc, OnceLock},
    vec,
};

use answer::Type;
use compiler::{executable::match_::instructions::thing::HasReverseInstruction, ExecutorVariable};
use concept::{
    error::ConceptReadError,
    thing::{attribute::Attribute, has::Has, object::HasReverseIterator, thing_manager::ThingManager},
    type_::{attribute_type::AttributeType, object_type::ObjectType},
};
use encoding::value::value::Value;
use itertools::{kmerge_by, Itertools, KMergeBy};
use primitive::Bounds;
use resource::constants::traversal::CONSTANT_CONCEPT_LIMIT;
use storage::snapshot::ReadableSnapshot;

use super::has_executor::HasFilterMapFn;
use crate::{
    instruction::{
        has_executor::{HasFilterFn, HasOrderingFn, HasTupleIterator, EXTRACT_ATTRIBUTE, EXTRACT_OWNER},
        iid_executor::IidExecutor,
        iterator::{SortedTupleIterator, TupleIterator},
        min_max_types,
        tuple::{has_to_tuple_attribute_owner, has_to_tuple_owner_attribute, Tuple, TuplePositions},
        BinaryIterateMode, Checker, VariableModes,
    },
    pipeline::stage::ExecutionContext,
    row::MaybeOwnedRow,
};

pub(crate) struct HasReverseExecutor {
    has: ir::pattern::constraint::Has<ExecutorVariable>,
    iterate_mode: BinaryIterateMode,
    variable_modes: VariableModes,
    tuple_positions: TuplePositions,
    attribute_owner_types: Arc<BTreeMap<Type, Vec<Type>>>,
    attribute_owner_types_range: BTreeMap<AttributeType, Bounds<ObjectType>>,
    owner_type_range: Bounds<ObjectType>,
    filter_fn: Arc<HasFilterFn>,
    attribute_cache: OnceLock<Vec<Attribute>>,
    checker: Checker<(Has, u64)>,
}

impl fmt::Debug for HasReverseExecutor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "HasReverseExecutor")
    }
}

pub(crate) type HasReverseTupleIteratorSingle = HasTupleIterator<HasReverseIterator>;
pub(crate) type HasReverseTupleIteratorChained = HasTupleIterator<ChainedHasReverseIterator>;
type ChainedHasReverseIterator = iter::Flatten<vec::IntoIter<HasReverseIterator>>;
pub(crate) type HasReverseUnboundedSortedOwnerMerged = HasTupleIterator<KMergeBy<HasReverseIterator, HasOrderingFn>>;

impl HasReverseExecutor {
    pub(crate) fn new(
        has_reverse: HasReverseInstruction<ExecutorVariable>,
        variable_modes: VariableModes,
        sort_by: ExecutorVariable,
        _snapshot: &impl ReadableSnapshot,
        _thing_manager: &ThingManager,
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

        let attribute_owner_types_range: BTreeMap<AttributeType, Bounds<ObjectType>> = attribute_owner_types
            .iter()
            .map(|(type_, owner_types)| {
                let (min_owner_type, max_owner_type) = min_max_types(owner_types);
                (
                    type_.as_attribute_type(),
                    (
                        Bound::Included(min_owner_type.as_object_type()),
                        Bound::Included(max_owner_type.as_object_type()),
                    ),
                )
            })
            .collect();

        let (min_owner_type, max_owner_type) = min_max_types(&*owner_types);
        let owner_type_range =
            (Bound::Included(min_owner_type.as_object_type()), Bound::Included(max_owner_type.as_object_type()));

        let output_tuple_positions = match iterate_mode {
            BinaryIterateMode::Unbound => TuplePositions::Pair([Some(attribute), Some(owner)]),
            _ => TuplePositions::Pair([Some(owner), Some(attribute)]),
        };

        let checker =
            Checker::<(Has, _)>::new(checks, HashMap::from([(owner, EXTRACT_OWNER), (attribute, EXTRACT_ATTRIBUTE)]));

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
                let instances: Vec<Attribute> = context
                    .thing_manager
                    .get_attributes_in_range(context.snapshot.as_ref(), type_.as_attribute_type(), &value_range)?
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
        let filter_for_row: Box<HasFilterMapFn> = Box::new(move |item| match filter(&item) {
            Ok(true) => match check(&item) {
                Ok(true) | Err(_) => Some(item),
                Ok(false) => None,
            },
            Ok(false) => None,
            Err(_) => Some(item),
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
                let as_tuples: HasTupleIterator<ChainedHasReverseIterator> =
                    Self::all_has_reverse_chained(snapshot, thing_manager, &self.attribute_owner_types_range, range)?
                        .filter_map(filter_for_row)
                        .map::<Result<Tuple<'_>, _>, _>(has_to_tuple_attribute_owner);
                Ok(TupleIterator::HasReverseChained(SortedTupleIterator::new(
                    as_tuples,
                    self.tuple_positions.clone(),
                    &self.variable_modes,
                )))
            }
            BinaryIterateMode::UnboundInverted => {
                debug_assert!(self.attribute_cache.get().is_some());
                if self.attribute_cache.get().unwrap().len() == 1 {
                    let attribute = &self.attribute_cache.get().unwrap()[0];
                    // no heap allocs needed if there is only 1 iterator
                    let iterator = thing_manager.get_has_reverse_by_attribute_and_owner_type_range(
                        snapshot,
                        attribute,
                        &self.owner_type_range,
                    );
                    let as_tuples: HasReverseTupleIteratorSingle =
                        iterator.filter_map(filter_for_row).map(has_to_tuple_owner_attribute);
                    Ok(TupleIterator::HasReverseSingle(SortedTupleIterator::new(
                        as_tuples,
                        self.tuple_positions.clone(),
                        &self.variable_modes,
                    )))
                } else {
                    // TODO: we could create a reusable space for these temporarily held iterators so we don't have allocate again before the merging iterator
                    let attributes = self.attribute_cache.get().unwrap().iter();
                    let iterators = attributes.map(|attribute| {
                        thing_manager.get_has_reverse_by_attribute_and_owner_type_range(
                            snapshot,
                            attribute,
                            &self.owner_type_range,
                        )
                    });

                    // note: this will always have to heap alloc, if we use don't have a re-usable/small-vec'ed priority queue somewhere
                    let merged: KMergeBy<HasReverseIterator, HasOrderingFn> =
                        kmerge_by(iterators, compare_has_by_owner_then_attribute);
                    let as_tuples: HasReverseUnboundedSortedOwnerMerged =
                        merged.filter_map(filter_for_row).map(has_to_tuple_owner_attribute);
                    Ok(TupleIterator::HasReverseMerged(SortedTupleIterator::new(
                        as_tuples,
                        self.tuple_positions.clone(),
                        &self.variable_modes,
                    )))
                }
            }
            BinaryIterateMode::BoundFrom => {
                let attribute = self.has.attribute().as_variable().unwrap().as_position().unwrap();
                debug_assert!(row.len() > attribute.as_usize());
                let variable_value = row.get(attribute);
                let iterator = thing_manager.get_has_reverse_by_attribute_and_owner_type_range(
                    snapshot,
                    variable_value.as_thing().as_attribute(),
                    &self.owner_type_range,
                );
                let as_tuples: HasReverseTupleIteratorSingle =
                    iterator.filter_map(filter_for_row).map(has_to_tuple_owner_attribute);
                Ok(TupleIterator::HasReverseSingle(SortedTupleIterator::new(
                    as_tuples,
                    self.tuple_positions.clone(),
                    &self.variable_modes,
                )))
            }
        }
    }

    fn all_has_reverse_chained(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        attribute_type_owner_range: &BTreeMap<AttributeType, (Bound<ObjectType>, Bound<ObjectType>)>,
        attribute_values_range: (Bound<Value<'_>>, Bound<Value<'_>>),
    ) -> Result<ChainedHasReverseIterator, Box<ConceptReadError>> {
        let type_manager = thing_manager.type_manager();
        let iterators: Vec<_> = attribute_type_owner_range
            .iter()
            // TODO: we shouldn't really filter out errors here, but presumably a ConceptReadError will crop up elsewhere too if it happens here
            .filter(|(attribute_type, _owner_type_range)| {
                attribute_type.get_value_type(snapshot, type_manager).is_ok_and(|vt| vt.is_some())
            })
            .map(|(attribute_type, owner_types)| {
                thing_manager.get_has_reverse_in_range(snapshot, *attribute_type, &attribute_values_range, owner_types)
            })
            .try_collect()?;
        Ok(iterators.into_iter().flatten())
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
    left: &Result<(Has, u64), Box<ConceptReadError>>,
    right: &Result<(Has, u64), Box<ConceptReadError>>,
) -> bool {
    if let (Ok((has_1, _)), Ok((has_2, _))) = (left, right) {
        (has_1.owner(), has_1.attribute()) < (has_2.owner(), has_2.attribute())
    } else {
        false
    }
}
