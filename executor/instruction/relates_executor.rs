/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    iter,
    marker::PhantomData,
    sync::Arc,
    vec,
};

use answer::{variable_value::VariableValue, Type};
use compiler::match_::instructions::type_::RelatesInstruction;
use concept::{error::ConceptReadError, thing::thing_manager::ThingManager, type_::relates::Relates};
use itertools::Itertools;
use lending_iterator::{
    adaptors::{Map, TryFilter},
    AsHkt, AsNarrowingIterator, LendingIterator,
};
use storage::snapshot::ReadableSnapshot;

use crate::{
    instruction::{
        iterator::{SortedTupleIterator, TupleIterator},
        tuple::{relates_to_tuple_relation_role, RelatesToTupleFn, TuplePositions, TupleResult},
        BinaryIterateMode, Checker, FilterFn, VariableModes,
    },
    row::MaybeOwnedRow,
    VariablePosition,
};

pub(crate) struct RelatesExecutor {
    relates: ir::pattern::constraint::Relates<VariablePosition>,
    iterate_mode: BinaryIterateMode,
    variable_modes: VariableModes,
    tuple_positions: TuplePositions,
    relation_role_types: Arc<BTreeMap<Type, Vec<Type>>>,
    role_types: Arc<HashSet<Type>>,
    filter_fn: Arc<RelatesFilterFn>,
    checker: Checker<AsHkt![Relates<'_>]>,
}

pub(super) type RelatesTupleIterator<I> = Map<
    TryFilter<I, Box<RelatesFilterFn>, AsHkt![Relates<'_>], ConceptReadError>,
    RelatesToTupleFn,
    AsHkt![TupleResult<'_>],
>;

pub(super) type RelatesUnboundedSortedRelation = RelatesTupleIterator<
    AsNarrowingIterator<
        iter::Map<
            iter::Flatten<vec::IntoIter<HashSet<Relates<'static>>>>,
            fn(Relates<'static>) -> Result<Relates<'static>, ConceptReadError>,
        >,
        Result<AsHkt![Relates<'_>], ConceptReadError>,
    >,
>;
pub(super) type RelatesBoundedSortedRole = RelatesTupleIterator<
    AsNarrowingIterator<
        iter::Map<vec::IntoIter<Relates<'static>>, fn(Relates<'static>) -> Result<Relates<'static>, ConceptReadError>>,
        Result<AsHkt![Relates<'_>], ConceptReadError>,
    >,
>;

pub(super) type RelatesFilterFn = FilterFn<AsHkt![Relates<'_>]>;

type RelatesVariableValueExtractor = for<'a> fn(&'a Relates<'_>) -> VariableValue<'a>;
pub(super) const EXTRACT_RELATION: RelatesVariableValueExtractor =
    |relates| VariableValue::Type(Type::Relation(relates.relation().into_owned()));
pub(super) const EXTRACT_ROLE: RelatesVariableValueExtractor =
    |relates| VariableValue::Type(Type::RoleType(relates.role().into_owned()));

impl RelatesExecutor {
    pub(crate) fn new(
        relates: RelatesInstruction<VariablePosition>,
        variable_modes: VariableModes,
        sort_by: Option<VariablePosition>,
    ) -> Self {
        let role_types = relates.role_types().clone();
        let relation_role_types = relates.relation_role_types().clone();
        debug_assert!(role_types.len() > 0);

        let RelatesInstruction { relates, checks, .. } = relates;

        let iterate_mode = BinaryIterateMode::new(relates.relation(), relates.role_type(), &variable_modes, sort_by);
        let filter_fn = match iterate_mode {
            BinaryIterateMode::Unbound => create_relates_filter_relation_role_type(relation_role_types.clone()),
            BinaryIterateMode::UnboundInverted | BinaryIterateMode::BoundFrom => {
                create_relates_filter_role_type(role_types.clone())
            }
        };
        let output_tuple_positions = if iterate_mode.is_inverted() {
            TuplePositions::Pair([relates.role_type(), relates.relation()])
        } else {
            TuplePositions::Pair([relates.relation(), relates.role_type()])
        };

        let checker = Checker::<AsHkt![Relates<'_>]> {
            checks,
            extractors: HashMap::from([(relates.relation(), EXTRACT_RELATION), (relates.role_type(), EXTRACT_ROLE)]),
            _phantom_data: PhantomData,
        };

        Self {
            relates,
            iterate_mode,
            variable_modes,
            tuple_positions: output_tuple_positions,
            relation_role_types,
            role_types,
            filter_fn,
            checker,
        }
    }

    pub(crate) fn get_iterator(
        &self,
        snapshot: &Arc<impl ReadableSnapshot + 'static>,
        thing_manager: &Arc<ThingManager>,
        row: MaybeOwnedRow<'_>,
    ) -> Result<TupleIterator, ConceptReadError> {
        let filter = self.filter_fn.clone();
        let check = self.checker.filter_for_row(snapshot, thing_manager, &row);
        let filter_for_row: Box<RelatesFilterFn> = Box::new(move |item| match filter(item) {
            Ok(true) => check(item),
            fail => fail,
        });

        match self.iterate_mode {
            BinaryIterateMode::Unbound => {
                let type_manager = thing_manager.type_manager();
                let relates: Vec<_> = self
                    .relation_role_types
                    .keys()
                    .map(|relation| match relation {
                        Type::Relation(relation) => relation.get_relates(&**snapshot, type_manager),
                        _ => unreachable!("relation types must be relation types"),
                    })
                    .map_ok(|set| set.to_owned())
                    .try_collect()?;
                let iterator = relates.into_iter().flatten().map(Ok as _);
                let as_tuples: RelatesUnboundedSortedRelation =
                    AsNarrowingIterator::<_, Result<Relates<'_>, _>>::new(iterator)
                        .try_filter::<_, RelatesFilterFn, Relates<'_>, _>(filter_for_row)
                        .map(relates_to_tuple_relation_role);
                Ok(TupleIterator::RelatesUnbounded(SortedTupleIterator::new(
                    as_tuples,
                    self.tuple_positions.clone(),
                    &self.variable_modes,
                )))
            }

            BinaryIterateMode::UnboundInverted => {
                todo!() // is this ever relevant?
            }

            BinaryIterateMode::BoundFrom => {
                debug_assert!(row.len() > self.relates.relation().as_usize());
                let VariableValue::Type(Type::Relation(relation)) = row.get(self.relates.relation()).to_owned() else {
                    unreachable!("Relation in `relates` must be a relation type")
                };

                let type_manager = thing_manager.type_manager();
                let relates = relation.get_relates(&**snapshot, type_manager)?;

                let iterator =
                    relates.iter().cloned().sorted_by_key(|relates| (relates.role(), relates.relation())).map(Ok as _);
                let as_tuples: RelatesBoundedSortedRole =
                    AsNarrowingIterator::<_, Result<Relates<'_>, _>>::new(iterator)
                        .try_filter::<_, RelatesFilterFn, Relates<'_>, _>(filter_for_row)
                        .map(relates_to_tuple_relation_role);
                Ok(TupleIterator::RelatesBounded(SortedTupleIterator::new(
                    as_tuples,
                    self.tuple_positions.clone(),
                    &self.variable_modes,
                )))
            }
        }
    }
}

fn create_relates_filter_relation_role_type(
    relation_role_types: Arc<BTreeMap<Type, Vec<Type>>>,
) -> Arc<RelatesFilterFn> {
    Arc::new(move |result| match result {
        Ok(relates) => match relation_role_types.get(&Type::from(relates.relation().into_owned())) {
            Some(role_types) => Ok(role_types.contains(&Type::RoleType(relates.role().into_owned()))),
            None => Ok(false),
        },
        Err(err) => Err(err.clone()),
    })
}

fn create_relates_filter_role_type(role_types: Arc<HashSet<Type>>) -> Arc<RelatesFilterFn> {
    Arc::new(move |result| match result {
        Ok(relates) => Ok(role_types.contains(&Type::RoleType(relates.role().into_owned()))),
        Err(err) => Err(err.clone()),
    })
}
