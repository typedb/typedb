/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    iter,
    marker::PhantomData,
    sync::Arc,
    vec,
};

use answer::{variable_value::VariableValue, Type};
use compiler::match_::instructions::type_::PlaysReverseInstruction;
use concept::{error::ConceptReadError, thing::thing_manager::ThingManager, type_::plays::Plays};
use concept::{
    error::ConceptReadError,
    type_::{object_type::ObjectType, plays::Plays},
};
use itertools::Itertools;
use lending_iterator::{AsHkt, AsNarrowingIterator, LendingIterator};
use storage::snapshot::ReadableSnapshot;

use crate::{
    instruction::{
        iterator::{SortedTupleIterator, TupleIterator},
        plays_executor::{PlaysFilterFn, PlaysTupleIterator, EXTRACT_PLAYER, EXTRACT_ROLE},
        tuple::{plays_to_tuple_role_player, TuplePositions},
        BinaryIterateMode, Checker, VariableModes,
    },
    pipeline::stage::ExecutionContext,
    row::MaybeOwnedRow,
    VariablePosition,
};

pub(crate) struct PlaysReverseExecutor {
    plays: ir::pattern::constraint::Plays<VariablePosition>,
    iterate_mode: BinaryIterateMode,
    variable_modes: VariableModes,
    tuple_positions: TuplePositions,
    role_player_types: Arc<BTreeMap<Type, Vec<Type>>>,
    player_types: Arc<BTreeSet<Type>>,
    filter_fn: Arc<PlaysFilterFn>,
    checker: Checker<AsHkt![Plays<'_>]>,
}

pub(super) type PlaysReverseUnboundedSortedRole = PlaysTupleIterator<
    AsNarrowingIterator<
        iter::Map<
            iter::Flatten<vec::IntoIter<HashSet<Plays<'static>>>>,
            fn(Plays<'static>) -> Result<Plays<'static>, ConceptReadError>,
        >,
        Result<AsHkt![Plays<'_>], ConceptReadError>,
    >,
>;
pub(super) type PlaysReverseBoundedSortedPlayer = PlaysTupleIterator<
    AsNarrowingIterator<
        iter::Map<vec::IntoIter<Plays<'static>>, fn(Plays<'static>) -> Result<Plays<'static>, ConceptReadError>>,
        Result<AsHkt![Plays<'_>], ConceptReadError>,
    >,
>;

impl PlaysReverseExecutor {
    pub(crate) fn new(
        plays: PlaysReverseInstruction<VariablePosition>,
        variable_modes: VariableModes,
        sort_by: Option<VariablePosition>,
    ) -> Self {
        let arc = plays.player_types().clone();
        let player_types = arc;
        let role_player_types = plays.role_player_types().clone();
        debug_assert!(player_types.len() > 0);

        let PlaysReverseInstruction { plays, checks, .. } = plays;

        let iterate_mode = BinaryIterateMode::new(plays.role_type(), plays.player(), &variable_modes, sort_by);
        let filter_fn = match iterate_mode {
            BinaryIterateMode::Unbound => create_plays_filter_player_role(role_player_types.clone()),
            BinaryIterateMode::UnboundInverted | BinaryIterateMode::BoundFrom => {
                create_plays_filter_role(player_types.clone())
            }
        };

        let player = plays.player().as_variable();
        let role_type = plays.role_type().as_variable();

        let output_tuple_positions = if iterate_mode.is_inverted() {
            TuplePositions::Pair([player, role_type])
        } else {
            TuplePositions::Pair([role_type, player])
        };

        let checker = Checker::<AsHkt![Plays<'_>]> {
            checks,
            extractors: [(player, EXTRACT_PLAYER), (role_type, EXTRACT_ROLE)]
                .into_iter()
                .filter_map(|(var, ex)| Some((var?, ex)))
                .collect(),
            _phantom_data: PhantomData,
        };

        Self {
            plays,
            iterate_mode,
            variable_modes,
            tuple_positions: output_tuple_positions,
            role_player_types,
            player_types,
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
        let filter_for_row: Box<PlaysFilterFn> = Box::new(move |item| match filter(item) {
            Ok(true) => check(item),
            fail => fail,
        });

        let snapshot = &**context.snapshot();

        match self.iterate_mode {
            BinaryIterateMode::Unbound => {
                let type_manager = context.type_manager();
                let plays: Vec<_> = self
                    .role_player_types
                    .keys()
                    .map(|role| role.as_role_type().get_plays(snapshot, type_manager))
                    .map_ok(|set| set.to_owned())
                    .try_collect()?;
                let iterator = plays.into_iter().flatten().map(Ok as _);
                let as_tuples: PlaysReverseUnboundedSortedRole =
                    AsNarrowingIterator::<_, Result<Plays<'_>, _>>::new(iterator)
                        .try_filter::<_, PlaysFilterFn, Plays<'_>, _>(filter_for_row)
                        .map(plays_to_tuple_role_player);
                Ok(TupleIterator::PlaysReverseUnbounded(SortedTupleIterator::new(
                    as_tuples,
                    self.tuple_positions.clone(),
                    &self.variable_modes,
                )))
            }

            BinaryIterateMode::UnboundInverted => {
                todo!() // is this ever relevant?
            }

            BinaryIterateMode::BoundFrom => {
                let role_type = self.plays.role_type().as_variable().unwrap();
                debug_assert!(row.len() > role_type.as_usize());
                let VariableValue::Type(Type::RoleType(role)) = row.get(role_type).to_owned() else {
                    unreachable!("Role in `plays` must be an role type")
                };

                let type_manager = context.type_manager();
                let plays = role.get_plays(snapshot, type_manager)?.to_owned();

                let iterator = plays.into_iter().sorted_by_key(|plays| plays.player()).map(Ok as _);
                let as_tuples: PlaysReverseBoundedSortedPlayer =
                    AsNarrowingIterator::<_, Result<Plays<'_>, _>>::new(iterator)
                        .try_filter::<_, PlaysFilterFn, Plays<'_>, _>(filter_for_row)
                        .map(plays_to_tuple_role_player);
                Ok(TupleIterator::PlaysReverseBounded(SortedTupleIterator::new(
                    as_tuples,
                    self.tuple_positions.clone(),
                    &self.variable_modes,
                )))
            }
        }
    }
}

fn create_plays_filter_player_role(role_player_types: Arc<BTreeMap<Type, Vec<Type>>>) -> Arc<PlaysFilterFn> {
    Arc::new(move |result| match result {
        Ok(plays) => match role_player_types.get(&Type::RoleType(plays.role().into_owned())) {
            Some(player_types) => Ok(player_types.contains(&Type::from(plays.player().into_owned()))),
            None => Ok(false),
        },
        Err(err) => Err(err.clone()),
    })
}

fn create_plays_filter_role(player_types: Arc<BTreeSet<Type>>) -> Arc<PlaysFilterFn> {
    Arc::new(move |result| match result {
        Ok(plays) => Ok(player_types.contains(&Type::from(plays.player().into_owned()))),
        Err(err) => Err(err.clone()),
    })
}
