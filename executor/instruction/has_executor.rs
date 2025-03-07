/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    fmt, iter,
    ops::Bound,
    sync::Arc,
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
    type_::{attribute_type::AttributeType, object_type::ObjectType},
};
use itertools::{kmerge_by, Itertools, KMergeBy};
use primitive::Bounds;
use resource::constants::traversal::CONSTANT_CONCEPT_LIMIT;
use storage::snapshot::ReadableSnapshot;

use crate::{
    instruction::{
        iterator::{SortedTupleIterator, TupleIterator},
        min_max_types,
        tuple::{has_to_tuple_attribute_owner, has_to_tuple_owner_attribute, HasToTupleFn, Tuple, TuplePositions},
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
    filter_fn: Arc<HasFilterFn>,
    owner_cache: Option<Vec<Object>>,
    checker: Checker<(Has, u64)>,
}

impl fmt::Debug for HasExecutor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "HasExecutor")
    }
}

pub(super) type HasTupleIterator<I> = iter::Map<iter::FilterMap<I, Box<HasFilterMapFn>>, HasToTupleFn>;

pub(crate) type HasUnboundedTupleIteratorSingle = HasTupleIterator<HasIterator>;
pub(crate) type HasUnboundedTupleIteratorMerged = HasTupleIterator<KMergeBy<HasIterator, HasOrderingFn>>;

pub(super) type HasFilterFn = FilterFn<(Has, u64)>;
pub(super) type HasFilterMapFn = FilterMapUnchangedFn<(Has, u64)>;

type HasVariableValueExtractor = for<'a, 'b> fn(&'a (Has, u64)) -> VariableValue<'a>;
pub(super) const EXTRACT_OWNER: HasVariableValueExtractor = |(has, _)| VariableValue::Thing(Thing::from(has.owner()));
pub(super) const EXTRACT_ATTRIBUTE: HasVariableValueExtractor =
    |(has, _)| VariableValue::Thing(Thing::Attribute(has.attribute()));

pub(crate) type HasOrderingFn = for<'a, 'b> fn(
    &'a Result<(Has, u64), Box<ConceptReadError>>,
    &'b Result<(Has, u64), Box<ConceptReadError>>,
) -> bool;

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

        let output_tuple_positions = TuplePositions::Pair([Some(owner), Some(attribute)]);

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
                let instances: Vec<_> = thing_manager.get_objects_in(snapshot, type_.as_object_type()).try_collect()?;
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
        Ok(Self {
            has,
            iterate_mode,
            variable_modes,
            tuple_positions: output_tuple_positions,
            owner_attribute_types,
            owner_type_range,
            attribute_type_range,
            filter_fn,
            owner_cache,
            checker,
        })
    }

    pub(crate) fn get_iterator(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: MaybeOwnedRow<'_>,
    ) -> Result<TupleIterator, Box<ConceptReadError>> {
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

        let attribute = self.has.attribute().as_variable().unwrap().as_position();
        let owner = self.has.owner().as_variable().unwrap().as_position();

        let attribute =
            attribute.and_then(|pos| (row.len() > pos.as_usize()).then(|| row.get(pos))).filter(|v| !v.is_empty());
        let owner = owner.and_then(|pos| (row.len() > pos.as_usize()).then(|| row.get(pos))).filter(|v| !v.is_empty());

        match (owner, attribute) {
            (None, None) => {
                // TODO: we could cache the range byte arrays computed inside the thing_manager, for this case

                // TODO: in the HasReverse case, we look up N iterators (one per type) and link them - here we scan and post-filter
                //        we should determine which strategy we want long-term
                let as_tuples: HasUnboundedTupleIteratorSingle = thing_manager
                    .get_has_from_owner_type_range_unordered(snapshot, &self.owner_type_range)
                    .filter_map(filter_for_row)
                    .map::<Result<Tuple<'_>, _>, _>(has_to_tuple_owner_attribute);
                Ok(TupleIterator::HasSingle(SortedTupleIterator::new(
                    as_tuples,
                    self.tuple_positions.clone(),
                    &self.variable_modes,
                )))
            }
            (None, Some(_attribute)) => todo!(),
            (Some(owner), None) => {
                // TODO: inject value ranges
                let iterator = match owner {
                    VariableValue::Thing(Thing::Entity(entity)) => {
                        entity.get_has_types_range_unordered(snapshot, thing_manager, &self.attribute_type_range)
                    }
                    VariableValue::Thing(Thing::Relation(relation)) => {
                        relation.get_has_types_range_unordered(snapshot, thing_manager, &self.attribute_type_range)
                    }
                    _ => unreachable!("Has owner must be an entity or relation."),
                };
                let as_tuples: HasUnboundedTupleIteratorSingle =
                    iterator.filter_map(filter_for_row).map(has_to_tuple_owner_attribute);
                Ok(TupleIterator::HasSingle(SortedTupleIterator::new(
                    as_tuples,
                    self.tuple_positions.clone(),
                    &self.variable_modes,
                )))
            }
            (Some(owner), Some(attribute)) => {
                let VariableValue::Thing(Thing::Attribute(attr)) = attribute else { panic!() };
                let VariableValue::Thing(owner_obj) = owner else { panic!() };
                Ok(TupleIterator::Check(SortedTupleIterator::new(
                    owner_obj
                        .as_object()
                        .has_attribute(snapshot, thing_manager, attr)?
                        .then(|| Ok(Tuple::Pair([owner.to_owned(), attribute.to_owned()])))
                        .into_iter(),
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
    left: &Result<(Has, u64), Box<ConceptReadError>>,
    right: &Result<(Has, u64), Box<ConceptReadError>>,
) -> bool {
    if let (Ok((has_1, _)), Ok((has_2, _))) = (left, right) {
        (has_1.attribute(), has_1.owner()) < (has_2.attribute(), has_2.owner())
    } else {
        false
    }
}
