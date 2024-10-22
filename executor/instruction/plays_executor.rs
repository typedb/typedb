/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    iter,
    sync::Arc,
    vec,
};

use answer::{variable_value::VariableValue, Type};
use compiler::{executable::match_::instructions::type_::PlaysInstruction, ExecutorVariable};
use concept::{
    error::ConceptReadError,
    type_::{plays::Plays, PlayerAPI},
};
use itertools::Itertools;
use lending_iterator::{
    adaptors::{Map, TryFilter},
    AsHkt, AsNarrowingIterator, LendingIterator,
};
use storage::snapshot::ReadableSnapshot;

use crate::{
    instruction::{
        iterator::{SortedTupleIterator, TupleIterator},
        tuple::{plays_to_tuple_player_role, PlaysToTupleFn, TuplePositions, TupleResult},
        type_from_row_or_annotations, BinaryIterateMode, Checker, FilterFn, VariableModes,
    },
    pipeline::stage::ExecutionContext,
    row::MaybeOwnedRow,
};

pub(crate) struct PlaysExecutor {
    plays: ir::pattern::constraint::Plays<ExecutorVariable>,
    iterate_mode: BinaryIterateMode,
    variable_modes: VariableModes,
    tuple_positions: TuplePositions,
    player_role_types: Arc<BTreeMap<Type, Vec<Type>>>,
    role_types: Arc<BTreeSet<Type>>,
    filter_fn: Arc<PlaysFilterFn>,
    checker: Checker<AsHkt![Plays<'_>]>,
}

pub(super) type PlaysTupleIterator<I> =
    Map<TryFilter<I, Box<PlaysFilterFn>, AsHkt![Plays<'_>], ConceptReadError>, PlaysToTupleFn, AsHkt![TupleResult<'_>]>;

pub(super) type PlaysUnboundedSortedPlayer = PlaysTupleIterator<
    AsNarrowingIterator<
        iter::Map<
            iter::Flatten<vec::IntoIter<HashSet<Plays<'static>>>>,
            fn(Plays<'static>) -> Result<Plays<'static>, ConceptReadError>,
        >,
        Result<AsHkt![Plays<'_>], ConceptReadError>,
    >,
>;
pub(super) type PlaysBoundedSortedRole = PlaysTupleIterator<
    AsNarrowingIterator<
        iter::Map<vec::IntoIter<Plays<'static>>, fn(Plays<'static>) -> Result<Plays<'static>, ConceptReadError>>,
        Result<AsHkt![Plays<'_>], ConceptReadError>,
    >,
>;

pub(super) type PlaysFilterFn = FilterFn<AsHkt![Plays<'_>]>;

type PlaysVariableValueExtractor = for<'a> fn(&'a Plays<'_>) -> VariableValue<'a>;
pub(super) const EXTRACT_PLAYER: PlaysVariableValueExtractor =
    |plays| VariableValue::Type(Type::from(plays.player().into_owned()));
pub(super) const EXTRACT_ROLE: PlaysVariableValueExtractor =
    |plays| VariableValue::Type(Type::RoleType(plays.role().into_owned()));

impl PlaysExecutor {
    pub(crate) fn new(
        plays: PlaysInstruction<ExecutorVariable>,
        variable_modes: VariableModes,
        sort_by: ExecutorVariable,
    ) -> Self {
        let role_types = plays.role_types().clone();
        let player_role_types = plays.player_role_types().clone();
        debug_assert!(role_types.len() > 0);

        let PlaysInstruction { plays, checks, .. } = plays;

        let iterate_mode = BinaryIterateMode::new(plays.player(), plays.role_type(), &variable_modes, sort_by);
        let filter_fn = match iterate_mode {
            BinaryIterateMode::Unbound => create_plays_filter_player_role_type(player_role_types.clone()),
            BinaryIterateMode::UnboundInverted | BinaryIterateMode::BoundFrom => {
                create_plays_filter_role_type(role_types.clone())
            }
        };

        let player = plays.player().as_variable();
        let role_type = plays.role_type().as_variable();

        let output_tuple_positions = if iterate_mode.is_inverted() {
            TuplePositions::Pair([role_type, player])
        } else {
            TuplePositions::Pair([player, role_type])
        };

        let checker = Checker::<AsHkt![Plays<'_>]>::new(
            checks,
            [(player, EXTRACT_PLAYER), (role_type, EXTRACT_ROLE)]
                .into_iter()
                .filter_map(|(var, ex)| Some((var?, ex)))
                .collect(),
        );

        Self {
            plays,
            iterate_mode,
            variable_modes,
            tuple_positions: output_tuple_positions,
            player_role_types,
            role_types,
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
                    .player_role_types
                    .keys()
                    .map(|player| match player {
                        Type::Entity(player) => player.get_plays(snapshot, type_manager),
                        Type::Relation(player) => player.get_plays(snapshot, type_manager),
                        _ => unreachable!("player types must be entity or relation types"),
                    })
                    .map_ok(|set| set.to_owned())
                    .try_collect()?;
                let iterator = plays.into_iter().flatten().map(Ok as _);
                let as_tuples: PlaysUnboundedSortedPlayer =
                    AsNarrowingIterator::<_, Result<Plays<'_>, _>>::new(iterator)
                        .try_filter::<_, PlaysFilterFn, Plays<'_>, _>(filter_for_row)
                        .map(plays_to_tuple_player_role);
                Ok(TupleIterator::PlaysUnbounded(SortedTupleIterator::new(
                    as_tuples,
                    self.tuple_positions.clone(),
                    &self.variable_modes,
                )))
            }

            BinaryIterateMode::UnboundInverted => {
                todo!() // is this ever relevant?
            }

            BinaryIterateMode::BoundFrom => {
                let player = type_from_row_or_annotations(self.plays.player(), row, self.player_role_types.keys());
                let type_manager = context.type_manager();
                let plays = match player {
                    Type::Entity(player) => player.get_plays(snapshot, type_manager)?,
                    Type::Relation(player) => player.get_plays(snapshot, type_manager)?,
                    _ => unreachable!("player types must be entity or relation types"),
                };

                let iterator = plays.iter().cloned().sorted_by_key(|plays| (plays.role(), plays.player())).map(Ok as _);
                let as_tuples: PlaysBoundedSortedRole = AsNarrowingIterator::<_, Result<Plays<'_>, _>>::new(iterator)
                    .try_filter::<_, PlaysFilterFn, Plays<'_>, _>(filter_for_row)
                    .map(plays_to_tuple_player_role);
                Ok(TupleIterator::PlaysBounded(SortedTupleIterator::new(
                    as_tuples,
                    self.tuple_positions.clone(),
                    &self.variable_modes,
                )))
            }
        }
    }
}

fn create_plays_filter_player_role_type(player_role_types: Arc<BTreeMap<Type, Vec<Type>>>) -> Arc<PlaysFilterFn> {
    Arc::new(move |result| match result {
        Ok(plays) => match player_role_types.get(&Type::from(plays.player().into_owned())) {
            Some(role_types) => Ok(role_types.contains(&Type::RoleType(plays.role().into_owned()))),
            None => Ok(false),
        },
        Err(err) => Err(err.clone()),
    })
}

fn create_plays_filter_role_type(role_types: Arc<BTreeSet<Type>>) -> Arc<PlaysFilterFn> {
    Arc::new(move |result| match result {
        Ok(plays) => Ok(role_types.contains(&Type::RoleType(plays.role().into_owned()))),
        Err(err) => Err(err.clone()),
    })
}
