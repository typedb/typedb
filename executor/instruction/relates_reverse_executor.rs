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
use compiler::match_::instructions::type_::RelatesReverseInstruction;
use concept::{error::ConceptReadError, thing::thing_manager::ThingManager, type_::relates::Relates};
use itertools::Itertools;
use lending_iterator::{higher_order::AdHocHkt, AsLendingIterator, LendingIterator};
use storage::snapshot::ReadableSnapshot;

use crate::{
    instruction::{
        iterator::{SortedTupleIterator, TupleIterator},
        relates_executor::{RelatesFilterFn, RelatesTupleIterator, EXTRACT_RELATION, EXTRACT_ROLE},
        sub_executor::NarrowingTupleIterator,
        tuple::{relates_to_tuple_role_relation, TuplePositions},
        BinaryIterateMode, Checker, VariableModes,
    },
    row::MaybeOwnedRow,
    VariablePosition,
};

pub(crate) struct RelatesReverseExecutor {
    relates: ir::pattern::constraint::Relates<VariablePosition>,
    iterate_mode: BinaryIterateMode,
    variable_modes: VariableModes,
    tuple_positions: TuplePositions,
    role_relation_types: Arc<BTreeMap<Type, Vec<Type>>>,
    relation_types: Arc<HashSet<Type>>,
    filter_fn: Arc<RelatesFilterFn>,
    checker: Checker<AdHocHkt<Relates<'static>>>,
}

pub(super) type RelatesReverseUnboundedSortedRole = RelatesTupleIterator<
    AsLendingIterator<
        iter::Map<vec::IntoIter<Relates<'static>>, fn(Relates<'static>) -> Result<Relates<'static>, ConceptReadError>>,
    >,
>;
pub(super) type RelatesReverseBoundedSortedRelation =
    RelatesTupleIterator<lending_iterator::Once<Result<AdHocHkt<Relates<'static>>, ConceptReadError>>>;

impl RelatesReverseExecutor {
    pub(crate) fn new(
        relates: RelatesReverseInstruction<VariablePosition>,
        variable_modes: VariableModes,
        sort_by: Option<VariablePosition>,
    ) -> Self {
        let relation_types = relates.relation_types().clone();
        let role_relation_types = relates.role_relation_types().clone();
        debug_assert!(relation_types.len() > 0);

        let RelatesReverseInstruction { relates, checks, .. } = relates;

        let iterate_mode = BinaryIterateMode::new(relates.role_type(), relates.relation(), &variable_modes, sort_by);
        let filter_fn = match iterate_mode {
            BinaryIterateMode::Unbound => create_relates_filter_relation_role(role_relation_types.clone()),
            BinaryIterateMode::UnboundInverted | BinaryIterateMode::BoundFrom => {
                create_relates_filter_role(relation_types.clone())
            }
        };
        let output_tuple_positions = if iterate_mode.is_inverted() {
            TuplePositions::Pair([relates.relation(), relates.role_type()])
        } else {
            TuplePositions::Pair([relates.role_type(), relates.relation()])
        };

        let checker = Checker::<AdHocHkt<Relates<'static>>> {
            checks,
            extractors: HashMap::from([(relates.relation(), EXTRACT_RELATION), (relates.role_type(), EXTRACT_ROLE)]),
            _phantom_data: PhantomData,
        };

        Self {
            relates,
            iterate_mode,
            variable_modes,
            tuple_positions: output_tuple_positions,
            role_relation_types,
            relation_types,
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
                    .role_relation_types
                    .keys()
                    .map(|role| role.as_role_type().get_relates_root(&**snapshot, type_manager))
                    .try_collect()?;
                let iterator = relates.into_iter().map(Ok as _);
                let as_tuples: RelatesReverseUnboundedSortedRole = NarrowingTupleIterator(
                    AsLendingIterator::new(iterator)
                        .try_filter::<_, RelatesFilterFn, AdHocHkt<Relates<'_>>, _>(filter_for_row)
                        .map(relates_to_tuple_role_relation),
                );
                Ok(TupleIterator::RelatesReverseUnbounded(SortedTupleIterator::new(
                    as_tuples,
                    self.tuple_positions.clone(),
                    &self.variable_modes,
                )))
            }

            BinaryIterateMode::UnboundInverted => {
                todo!() // is this ever relevant?
            }

            BinaryIterateMode::BoundFrom => {
                debug_assert!(row.len() > self.relates.role_type().as_usize());
                let VariableValue::Type(Type::RoleType(role)) = row.get(self.relates.role_type()).to_owned() else {
                    unreachable!("Role in `relates` must be an role type")
                };

                let type_manager = thing_manager.type_manager();
                let relates = role.get_relates_root(&**snapshot, type_manager)?;

                let as_tuples: RelatesReverseBoundedSortedRelation = NarrowingTupleIterator(
                    lending_iterator::once(Ok(relates))
                        .try_filter::<_, RelatesFilterFn, AdHocHkt<Relates<'_>>, _>(filter_for_row)
                        .map(relates_to_tuple_role_relation),
                );
                Ok(TupleIterator::RelatesReverseBounded(SortedTupleIterator::new(
                    as_tuples,
                    self.tuple_positions.clone(),
                    &self.variable_modes,
                )))
            }
        }
    }
}

fn create_relates_filter_relation_role(role_relation_types: Arc<BTreeMap<Type, Vec<Type>>>) -> Arc<RelatesFilterFn> {
    Arc::new(move |result| match result {
        Ok(relates) => match role_relation_types.get(&Type::RoleType(relates.role())) {
            Some(relation_types) => Ok(relation_types.contains(&Type::from(relates.relation()))),
            None => Ok(false),
        },
        Err(err) => Err(err.clone()),
    })
}

fn create_relates_filter_role(relation_types: Arc<HashSet<Type>>) -> Arc<RelatesFilterFn> {
    Arc::new(move |result| match result {
        Ok(relates) => Ok(relation_types.contains(&Type::from(relates.relation()))),
        Err(err) => Err(err.clone()),
    })
}
