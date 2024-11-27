/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    fmt, iter,
    sync::Arc,
    vec,
};

use answer::Type;
use compiler::{executable::match_::instructions::type_::RelatesReverseInstruction, ExecutorVariable};
use concept::{
    error::ConceptReadError,
    type_::{relation_type::RelationType, role_type::RoleType},
};
use itertools::Itertools;
use lending_iterator::{AsHkt, AsNarrowingIterator, LendingIterator};
use storage::snapshot::ReadableSnapshot;

use crate::{
    instruction::{
        iterator::{SortedTupleIterator, TupleIterator},
        relates_executor::{
            RelatesFilterFn, RelatesFilterMapFn, RelatesTupleIterator, RelatesVariableValueExtractor, EXTRACT_RELATION,
            EXTRACT_ROLE,
        },
        tuple::{relates_to_tuple_relation_role, relates_to_tuple_role_relation, TuplePositions, TupleResult},
        type_from_row_or_annotations, BinaryIterateMode, Checker, VariableModes,
    },
    pipeline::stage::ExecutionContext,
    row::MaybeOwnedRow,
};

pub(crate) struct RelatesReverseExecutor {
    relates: ir::pattern::constraint::Relates<ExecutorVariable>,
    iterate_mode: BinaryIterateMode,
    variable_modes: VariableModes,
    tuple_positions: TuplePositions,
    role_relation_types: Arc<BTreeMap<Type, Vec<Type>>>,
    relation_types: Arc<BTreeSet<Type>>,
    filter_fn: Arc<RelatesFilterFn>,
    checker: Checker<(RelationType, RoleType)>,
}

pub(super) type RelatesReverseUnboundedSortedRole = AsNarrowingIterator<
    RelatesTupleIterator<
        iter::Map<
            iter::Flatten<vec::IntoIter<BTreeSet<(RelationType, RoleType)>>>,
            fn((RelationType, RoleType)) -> Result<(RelationType, RoleType), Box<ConceptReadError>>,
        >,
    >,
    AsHkt![TupleResult<'_>],
>;
pub(super) type RelatesReverseBoundedSortedRelation = AsNarrowingIterator<
    RelatesTupleIterator<
        iter::Map<
            vec::IntoIter<(RelationType, RoleType)>,
            fn((RelationType, RoleType)) -> Result<(RelationType, RoleType), Box<ConceptReadError>>,
        >,
    >,
    AsHkt![TupleResult<'_>],
>;

impl RelatesReverseExecutor {
    pub(crate) fn new(
        relates: RelatesReverseInstruction<ExecutorVariable>,
        variable_modes: VariableModes,
        sort_by: ExecutorVariable,
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

        let output_tuple_positions = match iterate_mode {
            BinaryIterateMode::Unbound => TuplePositions::Pair([role_type, relation]),
            _ => TuplePositions::Pair([relation, role_type]),
        };

        let checker = Checker::<(RelationType, RoleType)>::new(
            checks,
            [(relation, EXTRACT_RELATION), (role_type, EXTRACT_ROLE)]
                .into_iter()
                .filter_map(|(var, ex)| Some((var?, ex)))
                .collect::<HashMap<ExecutorVariable, RelatesVariableValueExtractor>>(),
        );

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
    ) -> Result<TupleIterator, Box<ConceptReadError>> {
        let filter = self.filter_fn.clone();
        let check = self.checker.filter_for_row(context, &row);
        let filter_for_row: Box<RelatesFilterMapFn> = Box::new(move |item| match filter(&item) {
            Ok(true) => match check(&item) {
                Ok(true) | Err(_) => Some(item),
                Ok(false) => None,
            },
            Ok(false) => None,
            Err(_) => Some(item),
        });

        let snapshot = &**context.snapshot();

        match self.iterate_mode {
            BinaryIterateMode::Unbound => {
                let type_manager = context.type_manager();
                let relates: Vec<_> = self
                    .role_relation_types
                    .keys()
                    .map(|role| {
                        let role_type = role.as_role_type();
                        role_type
                            .get_relation_types(snapshot, type_manager)
                            .map(|res| res.to_owned().keys().map(|relation_type| (*relation_type, role_type)).collect())
                    })
                    .try_collect()?;
                let iterator = relates.into_iter().flatten().map(Ok as _);
                let as_tuples: RelatesReverseUnboundedSortedRole = AsNarrowingIterator::new(
                    iterator.filter_map(filter_for_row).map(relates_to_tuple_role_relation as _),
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
                let role_type =
                    type_from_row_or_annotations(self.relates.role_type(), row, self.role_relation_types.keys())
                        .as_role_type();
                let relates = role_type
                    .get_relation_types(snapshot, context.type_manager())?
                    .to_owned()
                    .into_keys()
                    .map(|relation_type| (relation_type, role_type));

                let iterator = relates.into_iter().sorted_by_key(|(relation, _)| *relation).map(Ok as _);
                let as_tuples: RelatesReverseBoundedSortedRelation = AsNarrowingIterator::new(
                    iterator.filter_map(filter_for_row).map(relates_to_tuple_relation_role as _),
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

impl fmt::Display for RelatesReverseExecutor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Reverse[{}], mode={}", &self.relates, &self.iterate_mode)
    }
}

fn create_relates_filter_relation_role(role_relation_types: Arc<BTreeMap<Type, Vec<Type>>>) -> Arc<RelatesFilterFn> {
    Arc::new(move |result| match result {
        Ok((relation, role)) => match role_relation_types.get(&Type::RoleType((*role).into_owned())) {
            Some(relation_types) => Ok(relation_types.contains(&Type::from((*relation).into_owned()))),
            None => Ok(false),
        },
        Err(err) => Err(err.clone()),
    })
}

fn create_relates_filter_role(relation_types: Arc<BTreeSet<Type>>) -> Arc<RelatesFilterFn> {
    Arc::new(move |result| match result {
        Ok((relation, _)) => Ok(relation_types.contains(&Type::from((*relation).into_owned()))),
        Err(err) => Err(err.clone()),
    })
}
