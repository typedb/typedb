/*
 * This Source Code Form is playsject to the terms of the Mozilla Public
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
use compiler::match_::instructions::type_::PlaysInstruction;
use concept::{
    error::ConceptReadError,
    thing::thing_manager::ThingManager,
    type_::{plays::Plays, PlayerAPI},
};
use itertools::Itertools;
use lending_iterator::{
    adaptors::{Map, TryFilter},
    higher_order::AdHocHkt,
    AsLendingIterator, LendingIterator,
};
use storage::snapshot::ReadableSnapshot;

use crate::{
    instruction::{
        iterator::{SortedTupleIterator, TupleIterator},
        sub_executor::NarrowingTupleIterator,
        tuple::{plays_to_tuple_player_role, PlaysToTupleFn, TuplePositions, TupleResult},
        BinaryIterateMode, Checker, FilterFn, VariableModes,
    },
    row::MaybeOwnedRow,
    VariablePosition,
};

pub(crate) struct PlaysExecutor {
    plays: ir::pattern::constraint::Plays<VariablePosition>,
    iterate_mode: BinaryIterateMode,
    variable_modes: VariableModes,
    tuple_positions: TuplePositions,
    player_role_types: Arc<BTreeMap<Type, Vec<Type>>>,
    role_types: Arc<HashSet<Type>>,
    filter_fn: Arc<PlaysFilterFn>,
    checker: Checker<AdHocHkt<Plays<'static>>>,
}

pub(super) type PlaysTupleIterator<I> = NarrowingTupleIterator<
    Map<
        TryFilter<I, Box<PlaysFilterFn>, AdHocHkt<Plays<'static>>, ConceptReadError>,
        PlaysToTupleFn,
        AdHocHkt<TupleResult<'static>>,
    >,
>;

pub(super) type PlaysUnboundedSortedPlayer = PlaysTupleIterator<
    AsLendingIterator<
        iter::Map<
            iter::Flatten<vec::IntoIter<HashSet<Plays<'static>>>>,
            fn(Plays<'static>) -> Result<Plays<'static>, ConceptReadError>,
        >,
    >,
>;
pub(super) type PlaysBoundedSortedRole = PlaysTupleIterator<
    AsLendingIterator<
        iter::Map<vec::IntoIter<Plays<'static>>, fn(Plays<'static>) -> Result<Plays<'static>, ConceptReadError>>,
    >,
>;

pub(super) type PlaysFilterFn = FilterFn<AdHocHkt<Plays<'static>>>;

type PlaysVariableValueExtractor = fn(&Plays<'static>) -> VariableValue<'static>;
pub(super) const EXTRACT_PLAYER: PlaysVariableValueExtractor =
    |plays| VariableValue::Type(Type::from(plays.player().into_owned()));
pub(super) const EXTRACT_ROLE: PlaysVariableValueExtractor =
    |plays| VariableValue::Type(Type::RoleType(plays.role().into_owned()));

impl PlaysExecutor {
    pub(crate) fn new(
        plays: PlaysInstruction<VariablePosition>,
        variable_modes: VariableModes,
        sort_by: Option<VariablePosition>,
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
        let output_tuple_positions = if iterate_mode.is_inverted() {
            TuplePositions::Pair([plays.role_type(), plays.player()])
        } else {
            TuplePositions::Pair([plays.player(), plays.role_type()])
        };

        let checker = Checker::<AdHocHkt<Plays<'static>>> {
            checks,
            extractors: HashMap::from([(plays.player(), EXTRACT_PLAYER), (plays.role_type(), EXTRACT_ROLE)]),
            _phantom_data: PhantomData,
        };

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
        snapshot: &Arc<impl ReadableSnapshot + 'static>,
        thing_manager: &Arc<ThingManager>,
        row: MaybeOwnedRow<'_>,
    ) -> Result<TupleIterator, ConceptReadError> {
        let filter = self.filter_fn.clone();
        let check = self.checker.filter_for_row(snapshot, thing_manager, &row);
        let filter_for_row: Box<PlaysFilterFn> = Box::new(move |item| match filter(item) {
            Ok(true) => check(item),
            fail => fail,
        });

        match self.iterate_mode {
            BinaryIterateMode::Unbound => {
                let type_manager = thing_manager.type_manager();
                let plays: Vec<_> = self
                    .player_role_types
                    .keys()
                    .map(|player| match player {
                        Type::Entity(player) => player.get_plays(&**snapshot, type_manager),
                        Type::Relation(player) => player.get_plays(&**snapshot, type_manager),
                        _ => unreachable!("player types must be entity or relation types"),
                    })
                    .map_ok(|set| set.to_owned())
                    .try_collect()?;
                let iterator = plays.into_iter().flatten().map(Ok as _);
                let as_tuples: PlaysUnboundedSortedPlayer = NarrowingTupleIterator(
                    AsLendingIterator::new(iterator)
                        .try_filter::<_, PlaysFilterFn, AdHocHkt<Plays<'_>>, _>(filter_for_row)
                        .map(plays_to_tuple_player_role),
                );
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
                debug_assert!(row.len() > self.plays.player().as_usize());
                let VariableValue::Type(player) = row.get(self.plays.player()).to_owned() else {
                    unreachable!("Player in `plays` must be a type")
                };

                let type_manager = thing_manager.type_manager();
                let plays = match player {
                    Type::Entity(player) => player.get_plays(&**snapshot, type_manager)?,
                    Type::Relation(player) => player.get_plays(&**snapshot, type_manager)?,
                    _ => unreachable!("player types must be entity or relation types"),
                };

                let iterator = plays.iter().cloned().sorted_by_key(|plays| (plays.role(), plays.player())).map(Ok as _);
                let as_tuples: PlaysBoundedSortedRole = NarrowingTupleIterator(
                    AsLendingIterator::new(iterator)
                        .try_filter::<_, PlaysFilterFn, AdHocHkt<Plays<'_>>, _>(filter_for_row)
                        .map(plays_to_tuple_player_role),
                );
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
        Ok(plays) => match player_role_types.get(&Type::from(plays.player())) {
            Some(role_types) => Ok(role_types.contains(&Type::RoleType(plays.role()))),
            None => Ok(false),
        },
        Err(err) => Err(err.clone()),
    })
}

fn create_plays_filter_role_type(role_types: Arc<HashSet<Type>>) -> Arc<PlaysFilterFn> {
    Arc::new(move |result| match result {
        Ok(plays) => Ok(role_types.contains(&Type::RoleType(plays.role()))),
        Err(err) => Err(err.clone()),
    })
}
