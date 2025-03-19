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

use answer::{variable_value::VariableValue, Type};
use compiler::{executable::match_::instructions::type_::PlaysInstruction, ExecutorVariable};
use concept::{
    error::ConceptReadError,
    type_::{object_type::ObjectType, role_type::RoleType, type_manager::TypeManager, ObjectTypeAPI, PlayerAPI},
};
use itertools::Itertools;
use lending_iterator::{AsLendingIterator, Peekable};
use resource::profile::StorageCounters;
use storage::snapshot::ReadableSnapshot;

use crate::{
    instruction::{
        iterator::{NaiiveSeekable, SortedTupleIterator, TupleIterator},
        plays_reverse_executor::PlaysReverseExecutor,
        tuple::{plays_to_tuple_player_role, plays_to_tuple_role_player, PlaysToTupleFn, TuplePositions},
        type_from_row_or_annotations, BinaryIterateMode, Checker, FilterFn, FilterMapUnchangedFn, VariableModes,
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
    checker: Checker<(ObjectType, RoleType)>,
}

impl fmt::Debug for PlaysExecutor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PlaysExecutor")
    }
}

pub(super) type PlaysTupleIterator<I> =
    NaiiveSeekable<AsLendingIterator<iter::Map<iter::FilterMap<I, Box<PlaysFilterMapFn>>, PlaysToTupleFn>>>;

pub(super) type PlaysUnboundedSortedPlayer = PlaysTupleIterator<
    iter::Map<
        iter::Flatten<vec::IntoIter<BTreeSet<(ObjectType, RoleType)>>>,
        fn((ObjectType, RoleType)) -> Result<(ObjectType, RoleType), Box<ConceptReadError>>,
    >,
>;
pub(super) type PlaysBoundedSortedRole = PlaysTupleIterator<
    iter::Map<
        vec::IntoIter<(ObjectType, RoleType)>,
        fn((ObjectType, RoleType)) -> Result<(ObjectType, RoleType), Box<ConceptReadError>>,
    >,
>;

pub(super) type PlaysFilterFn = FilterFn<(ObjectType, RoleType)>;
pub(super) type PlaysFilterMapFn = FilterMapUnchangedFn<(ObjectType, RoleType)>;

pub(super) type PlaysVariableValueExtractor = for<'a> fn(&'a (ObjectType, RoleType)) -> VariableValue<'a>;
pub(super) const EXTRACT_PLAYER: PlaysVariableValueExtractor = |(player, _)| VariableValue::Type(Type::from(*player));
pub(super) const EXTRACT_ROLE: PlaysVariableValueExtractor = |(_, role)| VariableValue::Type(Type::RoleType(*role));

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

        let output_tuple_positions = match iterate_mode {
            BinaryIterateMode::Unbound => TuplePositions::Pair([player, role_type]),
            _ => TuplePositions::Pair([role_type, player]),
        };

        let checker = Checker::<(ObjectType, RoleType)>::new(
            checks,
            [(player, EXTRACT_PLAYER), (role_type, EXTRACT_ROLE)]
                .into_iter()
                .filter_map(|(var, ex)| Some((var?, ex)))
                .collect::<HashMap<ExecutorVariable, PlaysVariableValueExtractor>>(),
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
        storage_counters: StorageCounters,
    ) -> Result<TupleIterator, Box<ConceptReadError>> {
        let filter = self.filter_fn.clone();
        let check = self.checker.filter_for_row(context, &row, storage_counters);
        let filter_for_row: Box<PlaysFilterMapFn> = Box::new(move |item| match filter(&item) {
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
                let plays: Vec<_> = self
                    .player_role_types
                    .keys()
                    .map(|player| self.get_plays_for_player(snapshot, type_manager, *player))
                    .try_collect()?;
                let iterator = plays.into_iter().flatten().map(Ok as _);
                let as_tuples = iterator.filter_map(filter_for_row).map(plays_to_tuple_player_role as _);
                let lending_tuples = NaiiveSeekable::new(AsLendingIterator::new(as_tuples));
                Ok(TupleIterator::PlaysUnbounded(SortedTupleIterator::new(
                    lending_tuples,
                    self.tuple_positions.clone(),
                    &self.variable_modes,
                )))
            }

            BinaryIterateMode::UnboundInverted => {
                // is this ever relevant?
                return Err(Box::new(ConceptReadError::UnimplementedFunctionality {
                    functionality: error::UnimplementedFeature::IrrelevantUnboundInvertedMode(file!()),
                }));
            }

            BinaryIterateMode::BoundFrom => {
                let player = type_from_row_or_annotations(self.plays.player(), row, self.player_role_types.keys());
                let type_manager = context.type_manager();
                let plays = self.get_plays_for_player(snapshot, type_manager, player)?;

                let iterator = plays.into_iter().sorted_by_key(|&(player, role)| (role, player)).map(Ok as _);
                let as_tuples = iterator.filter_map(filter_for_row).map(plays_to_tuple_role_player as _);
                let lending_tuples = NaiiveSeekable::new(AsLendingIterator::new(as_tuples));
                Ok(TupleIterator::PlaysBounded(SortedTupleIterator::new(
                    lending_tuples,
                    self.tuple_positions.clone(),
                    &self.variable_modes,
                )))
            }
        }
    }

    fn get_plays_for_player(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        player: Type,
    ) -> Result<BTreeSet<(ObjectType, RoleType)>, Box<ConceptReadError>> {
        let object_type = match player {
            Type::Entity(entity) => entity.into_object_type(),
            Type::Relation(relation) => relation.into_object_type(),
            _ => unreachable!("player types must be relation or entity types"),
        };

        Ok(object_type
            .get_played_role_types(snapshot, type_manager)?
            .into_iter()
            .map(|role_type| (object_type, role_type))
            .collect())
    }
}

impl fmt::Display for PlaysExecutor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "[{}], mode={}", &self.plays, &self.iterate_mode)
    }
}

fn create_plays_filter_player_role_type(player_role_types: Arc<BTreeMap<Type, Vec<Type>>>) -> Arc<PlaysFilterFn> {
    Arc::new(move |result| match result {
        Ok((player, role)) => match player_role_types.get(&Type::from(*player)) {
            Some(role_types) => Ok(role_types.contains(&Type::RoleType(*role))),
            None => Ok(false),
        },
        Err(err) => Err(err.clone()),
    })
}

fn create_plays_filter_role_type(role_types: Arc<BTreeSet<Type>>) -> Arc<PlaysFilterFn> {
    Arc::new(move |result| match result {
        Ok((_, role)) => Ok(role_types.contains(&Type::RoleType(*role))),
        Err(err) => Err(err.clone()),
    })
}
