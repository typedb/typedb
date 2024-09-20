/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{BTreeMap, BTreeSet},
    iter,
    marker::PhantomData,
    sync::Arc,
    vec,
};

use answer::Type;
use compiler::match_::instructions::type_::RelatesReverseInstruction;
use concept::{error::ConceptReadError, type_::relates::Relates};
use itertools::Itertools;
use lending_iterator::{AsHkt, AsNarrowingIterator, LendingIterator};
use storage::snapshot::ReadableSnapshot;

use crate::{
    instruction::{
        iterator::{SortedTupleIterator, TupleIterator},
        relates_executor::{RelatesFilterFn, RelatesTupleIterator, EXTRACT_RELATION, EXTRACT_ROLE},
        tuple::{relates_to_tuple_role_relation, TuplePositions},
        type_from_row_or_annotations, BinaryIterateMode, Checker, VariableModes,
    },
    pipeline::stage::ExecutionContext,
    row::MaybeOwnedRow,
    VariablePosition,
};

pub(crate) struct RelatesReverseExecutor {
    relates: ir::pattern::constraint::Relates<VariablePosition>,
    iterate_mode: BinaryIterateMode,
    variable_modes: VariableModes,
    tuple_positions: TuplePositions,
    role_relation_types: Arc<BTreeMap<Type, Vec<Type>>>,
    relation_types: Arc<BTreeSet<Type>>,
    filter_fn: Arc<RelatesFilterFn>,
    checker: Checker<AsHkt![Relates<'_>]>,
}

pub(super) type RelatesReverseUnboundedSortedRole = RelatesTupleIterator<
    AsNarrowingIterator<
        iter::Map<vec::IntoIter<Relates<'static>>, fn(Relates<'static>) -> Result<Relates<'static>, ConceptReadError>>,
        Result<Relates<'static>, ConceptReadError>,
    >,
>;
pub(super) type RelatesReverseBoundedSortedRelation =
    RelatesTupleIterator<lending_iterator::Once<Result<AsHkt![Relates<'_>], ConceptReadError>>>;

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

        let relation = relates.relation().as_variable();
        let role_type = relates.role_type().as_variable();

        let output_tuple_positions = if iterate_mode.is_inverted() {
            TuplePositions::Pair([relation, role_type])
        } else {
            TuplePositions::Pair([role_type, relation])
        };

        let checker = Checker::<AsHkt![Relates<'_>]> {
            checks,
            extractors: [(relation, EXTRACT_RELATION), (role_type, EXTRACT_ROLE)]
                .into_iter()
                .filter_map(|(var, ex)| Some((var?, ex)))
                .collect(),
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
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: MaybeOwnedRow<'_>,
    ) -> Result<TupleIterator, ConceptReadError> {
        let filter = self.filter_fn.clone();
        let check = self.checker.filter_for_row(context, &row);
        let filter_for_row: Box<RelatesFilterFn> = Box::new(move |item| match filter(item) {
            Ok(true) => check(item),
            fail => fail,
        });

        let snapshot = &**context.snapshot();

        match self.iterate_mode {
            BinaryIterateMode::Unbound => {
                let type_manager = context.type_manager();
                let relates: Vec<_> = self
                    .role_relation_types
                    .keys()
                    .map(|role| role.as_role_type().get_relates_root(snapshot, type_manager))
                    .try_collect()?;
                let iterator = relates.into_iter().map(Ok as _);
                let as_tuples: RelatesReverseUnboundedSortedRole =
                    AsNarrowingIterator::<_, Result<Relates<'_>, _>>::new(iterator)
                        .try_filter::<_, RelatesFilterFn, Relates<'_>, _>(filter_for_row)
                        .map(relates_to_tuple_role_relation);
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
                let role_type =
                    type_from_row_or_annotations(self.relates.role_type(), row, self.role_relation_types.keys());
                let Type::RoleType(role) = role_type else { unreachable!("Role in `relates` must be an role type") };

                let relates = role.get_relates_root(snapshot, context.type_manager())?;

                let as_tuples: RelatesReverseBoundedSortedRelation =
                    lending_iterator::once::<Result<Relates<'_>, _>>(Ok(relates))
                        .try_filter::<_, RelatesFilterFn, Relates<'_>, _>(filter_for_row)
                        .map(relates_to_tuple_role_relation);
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
        Ok(relates) => match role_relation_types.get(&Type::RoleType(relates.role().into_owned())) {
            Some(relation_types) => Ok(relation_types.contains(&Type::from(relates.relation().into_owned()))),
            None => Ok(false),
        },
        Err(err) => Err(err.clone()),
    })
}

fn create_relates_filter_role(relation_types: Arc<BTreeSet<Type>>) -> Arc<RelatesFilterFn> {
    Arc::new(move |result| match result {
        Ok(relates) => Ok(relation_types.contains(&Type::from(relates.relation().into_owned()))),
        Err(err) => Err(err.clone()),
    })
}
