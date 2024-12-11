/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */


use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::{fmt, iter};
use std::sync::Arc;

use itertools::{Itertools, kmerge_by, KMergeBy};
use answer::{Thing, Type};
use answer::variable_value::VariableValue;
use compiler::executable::match_::instructions::thing::{IndexedRelationInstruction, LinksInstruction};

use compiler::executable::match_::instructions::{VariableMode, VariableModes};
use compiler::ExecutorVariable;
use concept::error::ConceptReadError;
use concept::thing::object::ObjectAPI;
use concept::thing::relation::{IndexedRelationsIterator, IndexedRelationPlayers, Relation, RolePlayer, LinksIterator};
use concept::thing::thing_manager::ThingManager;
use concept::type_::role_type::RoleType;
use ir::pattern::constraint::IndexedRelation;
use ir::pattern::Vertex;
use storage::snapshot::ReadableSnapshot;

use crate::instruction::iterator::{SortedTupleIterator, TupleIterator};
use crate::instruction::{Checker, FilterFn, FilterMapFn, TernaryIterateMode};
use crate::instruction::links_executor::{LinksFilterMapFn, LinksOrderingFn};
use crate::instruction::tuple::{indexed_relation_to_tuple_end_start_relation_startrole_endrole, indexed_relation_to_tuple_endrole_start_end_relation_relation_startrole, indexed_relation_to_tuple_relation_start_end_startrole_endrole, indexed_relation_to_tuple_start_end_relation_startrole_endrole, indexed_relation_to_tuple_startrole_start_end_relation_endrole, IndexedRelationToTupleFn, LinksToTupleFn, TuplePositions};
use crate::pipeline::stage::ExecutionContext;
use crate::row::MaybeOwnedRow;

pub(super) type IndexedRelationTupleIterator<I> = iter::Map<iter::FilterMap<I, Box<IndexedRelationFilterMapFn>>, IndexedRelationToTupleFn>;

pub(crate) type IndexedRelationSortedTupleIterator = IndexedRelationTupleIterator<IndexedRelationsIterator>;
pub(crate) type IndexedRelationUnboundedSortedStartMerged = IndexedRelationTupleIterator<KMergeBy<IndexedRelationsIterator, IndexedRelationOrderingFn>>;
// pub(crate) type LinksBoundedRelationSortedPlayer = crate::instruction::links_executor::LinksTupleIterator<LinksIterator>;
// pub(crate) type LinksBoundedRelationPlayer = crate::instruction::links_executor::LinksTupleIterator<LinksIterator>;

type IndexedRelationValueExtractor = fn(&(IndexedRelationPlayers, u64)) -> VariableValue<'static>;

pub(super) const EXTRACT_PLAYER_START: IndexedRelationValueExtractor =
    |(indexed, _)| VariableValue::Thing(Thing::from(indexed.0));
pub(super) const EXTRACT_PLAYER_END: IndexedRelationValueExtractor =
    |(indexed, _)| VariableValue::Thing(Thing::from(indexed.1));
pub(super) const EXTRACT_RELATION: IndexedRelationValueExtractor =
    |(indexed, _)| VariableValue::Thing(Thing::Relation(indexed.2));
pub(super) const EXTRACT_ROLE_START: IndexedRelationValueExtractor =
    |(indexed, _)| VariableValue::Type(Type::RoleType(indexed.3));
pub(super) const EXTRACT_ROLE_END: IndexedRelationValueExtractor =
    |(indexed, _)| VariableValue::Type(Type::RoleType(indexed.4));

pub(super) type IndexedRelationFilterFn = FilterFn<(IndexedRelationPlayers, u64)>;
pub(super) type IndexedRelationFilterMapFn = FilterMapFn<(IndexedRelationPlayers, u64)>;
pub(crate) type IndexedRelationOrderingFn = for<'a, 'b> fn(
    &'a Result<(IndexedRelationPlayers, u64), Box<ConceptReadError>>,
    &'b Result<(IndexedRelationPlayers, u64), Box<ConceptReadError>>,
) -> bool;

pub(crate) struct IndexedRelationExecutor {
    pub(crate) player_start: ExecutorVariable,
    pub(crate) player_end: ExecutorVariable,
    pub(crate) relation: ExecutorVariable,
    pub(crate) role_start: ExecutorVariable,
    pub(crate) role_end: ExecutorVariable,
    
    iterate_mode: IndexedRelationIterateIterateMode,
    variable_modes: VariableModes,

    tuple_positions: TuplePositions,

    pub(crate) relation_to_player_start_types: Arc<BTreeMap<Type, Vec<Type>>>,
    pub(crate) player_start_to_player_end_types: Arc<BTreeMap<Type, BTreeSet<Type>>>,
    pub(crate) role_start_types: Arc<BTreeSet<RoleType>>,
    pub(crate) role_end_types: Arc<BTreeSet<RoleType>>,

    filter_fn: Arc<IndexedRelationFilterFn>,
    // relation_cache: Option<Vec<Relation>>,

    checker: Checker<(IndexedRelationPlayers, u64)>,
}

impl IndexedRelationExecutor {
    pub(crate) fn new(
        indexed_relation: IndexedRelationInstruction<ExecutorVariable>,
        variable_modes: VariableModes,
        sort_by: ExecutorVariable,
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
    ) -> Result<Self, Box<ConceptReadError>> {
        debug_assert!(!variable_modes.all_inputs());

        let IndexedRelationInstruction {
            checks,
            player_start,
            player_end,
            relation,
            role_start,
            role_end,
            inputs: _inputs,
            relation_to_player_start_types,
            player_start_to_player_end_types,
            role_start_types,
            role_end_types,
        } = indexed_relation;
        let iterate_mode = IndexedRelationIterateIterateMode::new(
            player_start,
            player_end,
            relation,
            role_start,
            &variable_modes,
            sort_by
        );
        let filter_fn = create_indexed_players_filter(player_start_to_player_end_types.clone(), role_start_types.clone(), role_end_types.clone());

        // produce a lexicographical ordering where the Sorted component comes first: [sort][bound 1][bound 2]...[unbound 1][unbound 2]...
        // where the Sorted and then Unbound components are lexicographically ordered and unbound
        let output_tuple_positions = match iterate_mode {
            IndexedRelationIterateIterateMode::Unbound  => {
                TuplePositions::Quintuple([Some(player_start), Some(player_end), Some(relation), Some(role_start), Some(role_end)])
            },
            IndexedRelationIterateIterateMode::UnboundInvertedToPlayer => {
                TuplePositions::Quintuple([Some(player_end), Some(player_start), Some(relation), Some(role_start), Some(role_end)])
            }
            IndexedRelationIterateIterateMode::BoundStart => {
                TuplePositions::Quintuple([Some(player_end), Some(player_start), Some(relation), Some(role_start), Some(role_end)])
            }
            IndexedRelationIterateIterateMode::BoundStartBoundEnd => {
                TuplePositions::Quintuple([Some(relation), Some(player_start), Some(player_end), Some(role_start), Some(role_end)])
            }
            IndexedRelationIterateIterateMode::BoundStartBoundEndBoundRelation => {
                TuplePositions::Quintuple([Some(role_start), Some(player_start), Some(player_end), Some(relation), Some(role_end)])
            }
            IndexedRelationIterateIterateMode::BoundStartBoundEndBoundRelationBoundStartRole => {
                TuplePositions::Quintuple([Some(role_end), Some(player_start), Some(player_end), Some(relation), Some(role_start)])
            }
        };

        let checker = Checker::<(IndexedRelationPlayers, u64)>::new(
            checks,
            HashMap::from([
                (player_start, EXTRACT_PLAYER_START),
                (player_end, EXTRACT_PLAYER_END),
                (relation, EXTRACT_RELATION),
                (role_start, EXTRACT_ROLE_START),
                (role_end, EXTRACT_ROLE_END),
            ])
        );

        // let relation_type_range = (
        //     Bound::Included(relation_player_types.first_key_value().unwrap().0.as_relation_type()),
        //     Bound::Included(relation_player_types.last_key_value().unwrap().0.as_relation_type()),
        // );
        // let (min_player_type, max_player_type) = min_max_types(player_types.iter());
        // let player_type_range =
        //     (Bound::Included(min_player_type.as_object_type()), Bound::Included(max_player_type.as_object_type()));
        // let relation_cache = if iterate_mode == TernaryIterateMode::UnboundInverted {
        //     let mut cache = Vec::new();
        //     for type_ in relation_player_types.keys() {
        //         let instances: Vec<Relation> =
        //             thing_manager.get_relations_in(snapshot, type_.as_relation_type()).try_collect()?;
        //         cache.extend(instances);
        //     }
        //     #[cfg(debug_assertions)]
        //     if cache.len() < CONSTANT_CONCEPT_LIMIT {
        //         eprintln!("DEBUG_ASSERT_FAILURE: cache.len() > CONSTANT_CONCEPT_LIMIT");
        //     }
        //     Some(cache)
        // } else {
        //     None
        // };
        //
        Ok(Self {
            player_start,
            player_end,
            relation,
            role_start,
            role_end,

            iterate_mode,
            variable_modes,
            tuple_positions: output_tuple_positions,
            relation_to_player_start_types,
            player_start_to_player_end_types,
            role_start_types,
            role_end_types,
            filter_fn,
            // relation_cache,
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
        let filter_for_row: Box<IndexedRelationFilterMapFn> = Box::new(move |item| match filter(&item) {
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
            IndexedRelationIterateIterateMode::Unbound => {
                // want it sorted by start player, so we must merge an iterator per relation type
                if self.relation_to_player_start_types.len() == 1 {
                    let &relation_type = self.relation_to_player_start_types.keys().next().unwrap();
                    let as_tuples: IndexedRelationSortedTupleIterator = thing_manager.get_indexed_relations_in(snapshot, relation_type.as_relation_type())
                        .filter_map(filter_for_row)
                        .map(indexed_relation_to_tuple_start_end_relation_startrole_endrole);
                    Ok(TupleIterator::IndexedRelations(SortedTupleIterator::new(
                        as_tuples,
                        self.tuple_positions.clone(),
                        &self.variable_modes
                    )))
                } else {
                    let iterators = self.relation_to_player_start_types.keys()
                        .map(|relation_type| thing_manager.get_indexed_relations_in(snapshot, relation_type.as_relation_type()))
                        .collect_vec();
                    let merged: KMergeBy<IndexedRelationsIterator, IndexedRelationOrderingFn> =
                        kmerge_by(iterators, compare_indexed_players);
                    let as_tuples: IndexedRelationUnboundedSortedStartMerged = merged
                        .filter_map(filter_for_row)
                        .map(indexed_relation_to_tuple_start_end_relation_startrole_endrole);
                    Ok(TupleIterator::IndexedRelationsMerged(SortedTupleIterator::new(
                        as_tuples,
                        self.tuple_positions.clone(),
                        &self.variable_modes,
                    )))
                }
            }
            IndexedRelationIterateIterateMode::UnboundInvertedToPlayer => {
                todo!()
            }
            IndexedRelationIterateIterateMode::BoundStart => {
                let start_player = match row.get(self.player_start.as_position().unwrap()) {
                    VariableValue::Thing(thing) => thing.as_object(),
                    _ => unreachable!("Start player just be a thing object")
                };
                if self.relation_to_player_start_types.len() == 1 {
                    let relation_type = self.relation_to_player_start_types.keys().next().unwrap().as_relation_type();
                    let as_tuples: IndexedRelationSortedTupleIterator = start_player
                        .get_indexed_relation_players(snapshot, thing_manager, relation_type)
                        .filter_map(filter_for_row)
                        .map(indexed_relation_to_tuple_end_start_relation_startrole_endrole);
                    Ok(TupleIterator::IndexedRelations(SortedTupleIterator::new(
                        as_tuples,
                        self.tuple_positions.clone(),
                        &self.variable_modes,
                    )))
                } else {
                    let iterators = self.relation_to_player_start_types.keys()
                        .map(|relation_type| start_player.get_indexed_relation_players(snapshot, thing_manager, relation_type.as_relation_type()))
                        .collect_vec();
                    let merged: KMergeBy<IndexedRelationsIterator, IndexedRelationOrderingFn> =
                        kmerge_by(iterators, compare_indexed_players);
                    let as_tuples: IndexedRelationUnboundedSortedStartMerged = merged
                        .filter_map(filter_for_row)
                        .map(indexed_relation_to_tuple_end_start_relation_startrole_endrole);
                    Ok(TupleIterator::IndexedRelationsMerged(SortedTupleIterator::new(
                        as_tuples,
                        self.tuple_positions.clone(),
                        &self.variable_modes,
                    )))
                }
            }
            IndexedRelationIterateIterateMode::BoundStartBoundEnd => {
                let start_player = match row.get(self.player_start.as_position().unwrap()) {
                    VariableValue::Thing(thing) => thing.as_object(),
                    _ => unreachable!("Start player just be a thing object")
                };
                let end_player = match row.get(self.player_end.as_position().unwrap()) {
                    VariableValue::Thing(thing) => thing.as_object(),
                    _ => unreachable!("End player just be a thing object")
                };
                if self.relation_to_player_start_types.len() == 1 {
                    let relation_type = self.relation_to_player_start_types.keys().next().unwrap().as_relation_type();
                    let as_tuples: IndexedRelationSortedTupleIterator = start_player
                        .get_indexed_relations(snapshot, thing_manager, end_player, relation_type)
                        .filter_map(filter_for_row)
                        .map(indexed_relation_to_tuple_relation_start_end_startrole_endrole);
                    Ok(TupleIterator::IndexedRelations(SortedTupleIterator::new(
                        as_tuples,
                        self.tuple_positions.clone(),
                        &self.variable_modes,
                    )))
                } else {
                    let iterators = self.relation_to_player_start_types.keys()
                        .map(|relation_type| start_player.get_indexed_relations(snapshot, thing_manager, end_player, relation_type.as_relation_type()))
                        .collect_vec();
                    let merged: KMergeBy<IndexedRelationsIterator, IndexedRelationOrderingFn> =
                        kmerge_by(iterators, compare_indexed_players);
                    let as_tuples: IndexedRelationUnboundedSortedStartMerged = merged
                        .filter_map(filter_for_row)
                        .map(indexed_relation_to_tuple_relation_start_end_startrole_endrole);
                    Ok(TupleIterator::IndexedRelationsMerged(SortedTupleIterator::new(
                        as_tuples,
                        self.tuple_positions.clone(),
                        &self.variable_modes,
                    )))
                }
            }
            IndexedRelationIterateIterateMode::BoundStartBoundEndBoundRelation => {
                let start_player = match row.get(self.player_start.as_position().unwrap()) {
                    VariableValue::Thing(thing) => thing.as_object(),
                    _ => unreachable!("Start player just be a thing object")
                };
                let end_player = match row.get(self.player_end.as_position().unwrap()) {
                    VariableValue::Thing(thing) => thing.as_object(),
                    _ => unreachable!("End player just be a thing object")
                };
                let relation = match row.get(self.relation.as_position().unwrap()) {
                    VariableValue::Thing(thing) => thing.as_relation(),
                    _ => unreachable!("Indexed relation must be a thing relation")
                };
                let as_tuples: IndexedRelationSortedTupleIterator = start_player
                    .get_indexed_relation_roles(snapshot, thing_manager, end_player, relation)
                    .filter_map(filter_for_row)
                    .map(indexed_relation_to_tuple_startrole_start_end_relation_endrole);
                Ok(TupleIterator::IndexedRelations(SortedTupleIterator::new(
                    as_tuples,
                    self.tuple_positions.clone(),
                    &self.variable_modes,
                )))
            }
            IndexedRelationIterateIterateMode::BoundStartBoundEndBoundRelationBoundStartRole => {
                let start_player = match row.get(self.player_start.as_position().unwrap()) {
                    VariableValue::Thing(thing) => thing.as_object(),
                    _ => unreachable!("Start player just be a thing object")
                };
                let end_player = match row.get(self.player_end.as_position().unwrap()) {
                    VariableValue::Thing(thing) => thing.as_object(),
                    _ => unreachable!("End player just be a thing object")
                };
                let relation = match row.get(self.relation.as_position().unwrap()) {
                    VariableValue::Thing(thing) => thing.as_relation(),
                    _ => unreachable!("Indexed relation must be a thing relation")
                };
                let start_role = match row.get(self.role_start.as_position().unwrap()) {
                    VariableValue::Type(type_) => type_.as_role_type(),
                    _ => unreachable!("Indexed startrole must be a role type")
                };
                let as_tuples: IndexedRelationSortedTupleIterator = start_player
                    .get_indexed_relation_end_roles(snapshot, thing_manager, end_player, relation, start_role)
                    .filter_map(filter_for_row)
                    .map(indexed_relation_to_tuple_endrole_start_end_relation_relation_startrole);
                Ok(TupleIterator::IndexedRelations(SortedTupleIterator::new(
                    as_tuples,
                    self.tuple_positions.clone(),
                    &self.variable_modes,
                )))
            }
        }
    }
}

impl fmt::Display for IndexedRelationExecutor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "[{} -(role: {}, relation: {}, role: {})-> {}], mode={}",
            self.player_start,
            self.role_start,
            self.relation,
            self.role_end,
            self.player_end,
            &self.iterate_mode
        )
    }
}

/// Note: we should never have to filter Relation type, since it must always be specified in the prefix
fn create_indexed_players_filter(
    start_player_to_end_player_types: Arc<BTreeMap<Type, BTreeSet<Type>>>,
    start_role_types: Arc<BTreeSet<RoleType>>,
    end_role_types: Arc<BTreeSet<RoleType>>,
) -> Arc<IndexedRelationFilterFn> {
    Arc::new(move |result| {
        let (player_start, player_end, role_start, role_end) = match result {
            Ok(((player_start, player_end, relation, role_start, role_end), _)) => (player_start, player_end, role_start, role_end),
            Err(err) => return Err(err.clone()),
        };
        let Some(end_player_types) = start_player_to_end_player_types.get(&Type::from(player_start.type_())) else {
            return Ok(false);
        };
        Ok(end_player_types.contains(&Type::from(player_end.type_()))
            && start_role_types.contains(&role_start)
            && end_role_types.contains(&role_end)
        )
    })
}

fn compare_indexed_players(
    left: &Result<(IndexedRelationPlayers, u64), Box<ConceptReadError>>,
    right: &Result<(IndexedRelationPlayers, u64), Box<ConceptReadError>>,
) -> bool {
    if let (Ok((indexed_players_1, _)), Ok((indexed_players_2, _))) = (left, right) {
        indexed_players_1 < indexed_players_2
    } else {
        false
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(crate) enum IndexedRelationIterateIterateMode {
    // [x, y, r, a, b] = standard sort order
    Unbound,
    // [y, x, r, a, b] sort order
    UnboundInvertedToPlayer,
    // [X, y, r, a, b] sort order
    BoundStart,
    // [X, Y, r, a, b]
    BoundStartBoundEnd,
    // [X, Y, R, a, b]
    BoundStartBoundEndBoundRelation,
    // [X, Y, R, A, b]
    BoundStartBoundEndBoundRelationBoundStartRole,
    // cannot have all bound - would be a check!
}

impl IndexedRelationIterateIterateMode {
    pub(crate) fn new(
        player_start: ExecutorVariable,
        player_end: ExecutorVariable,
        relation: ExecutorVariable,
        player_role_start: ExecutorVariable,
        var_modes: &VariableModes,
        sort_by: ExecutorVariable,
    ) -> Self {
        debug_assert!(var_modes.len() == 5);
        debug_assert!(!var_modes.all_inputs());
        let is_start_bound = var_modes.get(player_start) == Some(VariableMode::Input);
        let is_end_bound = var_modes.get(player_end) == Some(VariableMode::Input);
        let is_rel_bound = var_modes.get(relation) == Some(VariableMode::Input);
        let is_role_start_bound = var_modes.get(player_role_start) == Some(VariableMode::Input);
        if is_role_start_bound {
            assert!(is_start_bound && is_end_bound && is_rel_bound);
            Self::BoundStartBoundEndBoundRelationBoundStartRole
        } else if is_rel_bound {
            assert!(is_start_bound && is_end_bound);
            Self::BoundStartBoundEndBoundRelation
        } else if is_end_bound {
            assert!(is_start_bound);
            Self::BoundStartBoundEnd
        } else if is_start_bound {
            Self::BoundStart
        } else if sort_by == player_end {
            Self::UnboundInvertedToPlayer
        } else {
            Self::Unbound
        }
    }
}

impl fmt::Display for IndexedRelationIterateIterateMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}
