/*
 * This Source Code Form is subject to the terms of the Mozilla Public)
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    cmp::Ordering,
    collections::{BTreeMap, HashSet},
    sync::Arc,
};

use answer::{variable_value::VariableValue, Thing, Type};
use concept::{
    error::ConceptReadError,
    thing::{
        relation::{Relation, RelationRolePlayerIterator, RolePlayer},
        thing_manager::ThingManager,
    },
};
use itertools::{Itertools, MinMaxResult};
use lending_iterator::{
    adaptors::{Filter, Map},
    higher_order::FnHktHelper,
    kmerge::KMergeBy,
    AsHkt, LendingIterator, Peekable,
};
use storage::{key_range::KeyRange, snapshot::ReadableSnapshot};

use super::{
    iterator::TupleIterator,
    tuple::{relation_role_player_to_tuple_relation_player_role, TupleResult},
};
use crate::{
    batch::ImmutableRow,
    instruction::{
        iterator::{inverted_instances_cache, SortedTupleIterator},
        tuple::{
            relation_role_player_to_tuple_player_relation_role, RelationRolePlayerToTupleFn, Tuple, TuplePositions,
        },
        VariableModes,
    },
    VariablePosition,
};

pub(crate) struct RolePlayerExecutor {
    role_player: ir::pattern::constraint::RolePlayer<VariablePosition>,

    iterate_mode: TernaryIterateMode,
    variable_modes: VariableModes,

    tuple_positions: TuplePositions,

    relation_player_types: Arc<BTreeMap<Type, Vec<Type>>>, // vecs are in sorted order
    player_types: Arc<HashSet<Type>>,

    filter_fn: Arc<RolePlayerFilterFn>,
    relation_cache: Option<Vec<Relation<'static>>>,
}

pub(crate) type RolePlayerUnboundedSortedRelation = Map<
    Filter<RelationRolePlayerIterator, Arc<RolePlayerFilterFn>>,
    RelationRolePlayerToTupleFn,
    AsHkt![TupleResult<'_>],
>;
pub(crate) type RolePlayerUnboundedSortedPlayerSingle = Map<
    Filter<RelationRolePlayerIterator, Arc<RolePlayerFilterFn>>,
    RelationRolePlayerToTupleFn,
    AsHkt![TupleResult<'_>],
>;
pub(crate) type RolePlayerUnboundedSortedPlayerMerged = Map<
    Filter<KMergeBy<RelationRolePlayerIterator, RelationRolePlayerOrderingFn>, Arc<RolePlayerFilterFn>>,
    RelationRolePlayerToTupleFn,
    AsHkt![TupleResult<'_>],
>;
pub(crate) type RolePlayerBoundedRelationSortedPlayer = Map<
    Filter<RelationRolePlayerIterator, Arc<RolePlayerFilterFn>>,
    RelationRolePlayerToTupleFn,
    AsHkt![TupleResult<'_>],
>;

pub(crate) type RolePlayerFilterFn =
    dyn for<'a, 'b> FnHktHelper<&'a Result<(Relation<'b>, RolePlayer<'b>, u64), ConceptReadError>, bool>;

pub(crate) type RelationRolePlayerOrderingFn = for<'a, 'b> fn(
    (
        &'a Result<(Relation<'a>, RolePlayer<'a>, u64), ConceptReadError>,
        &'b Result<(Relation<'b>, RolePlayer<'b>, u64), ConceptReadError>,
    ),
) -> Ordering;

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum TernaryIterateMode {
    // [x, y, z] = standard sort order
    Unbound,
    // [y, x, z] sort order
    UnboundInverted,
    // [X, y, z] sort order
    BoundFrom,
    // [X, Y, z]
    BoundFromBoundTo,
}

impl TernaryIterateMode {
    fn new(
        role_player: ir::pattern::constraint::RolePlayer<VariablePosition>,
        in_reverse_direction: bool,
        var_modes: &VariableModes,
        sort_by: Option<VariablePosition>,
    ) -> TernaryIterateMode {
        debug_assert!(!var_modes.fully_bound());
        let is_relation_bound = var_modes.get(role_player.relation()).is_some_and(|mode| mode.is_bound());
        let is_player_bound = var_modes.get(role_player.player()).is_some_and(|mode| mode.is_bound());
        if !is_relation_bound && !is_player_bound {
            match sort_by {
                None => {
                    // arbitrarily pick from sorted
                    TernaryIterateMode::Unbound
                }
                Some(variable) => {
                    if role_player.relation() == variable {
                        TernaryIterateMode::Unbound
                    } else {
                        TernaryIterateMode::UnboundInverted
                    }
                }
            }
        } else if is_relation_bound && !is_player_bound {
            TernaryIterateMode::BoundFrom
        } else {
            debug_assert!(is_relation_bound && is_player_bound);
            TernaryIterateMode::BoundFromBoundTo
        }
    }
}

impl RolePlayerExecutor {
    pub(crate) fn new(
        role_player: ir::pattern::constraint::RolePlayer<VariablePosition>,
        variable_modes: VariableModes,
        sort_by: Option<VariablePosition>,
        relation_player_types: Arc<BTreeMap<Type, Vec<Type>>>, // vecs are in sorted order
        player_role_types: Arc<BTreeMap<Type, HashSet<Type>>>, // vecs are in sorted order
        player_types: Arc<HashSet<Type>>,
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
    ) -> Result<Self, ConceptReadError> {
        debug_assert!(!relation_player_types.is_empty());
        debug_assert!(!variable_modes.fully_bound());
        let iterate_mode = TernaryIterateMode::new(role_player.clone(), false, &variable_modes, sort_by); // TODO
        let filter_fn = Self::create_role_player_filter_relations_players_roles(
            relation_player_types.clone(),
            player_role_types.clone(),
        );
        let output_tuple_positions = if iterate_mode == TernaryIterateMode::UnboundInverted {
            TuplePositions::Triple([role_player.player(), role_player.relation(), role_player.role_type()])
        } else {
            TuplePositions::Triple([role_player.relation(), role_player.player(), role_player.role_type()])
        };

        let relation_cache = if iterate_mode == TernaryIterateMode::UnboundInverted {
            Some(inverted_instances_cache(
                relation_player_types.keys().map(|t| t.as_relation_type()),
                snapshot,
                thing_manager,
            )?)
        } else {
            None
        };

        Ok(Self {
            role_player,
            iterate_mode,
            variable_modes,
            tuple_positions: output_tuple_positions,
            relation_player_types,
            player_types,
            filter_fn,
            relation_cache,
        })
    }

    pub(crate) fn get_iterator(
        &self,
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        row: ImmutableRow<'_>,
    ) -> Result<TupleIterator, ConceptReadError> {
        match self.iterate_mode {
            TernaryIterateMode::Unbound => {
                let first_from_type = self.relation_player_types.first_key_value().unwrap().0;
                let last_key_from_type = self.relation_player_types.last_key_value().unwrap().0;
                let key_range =
                    KeyRange::new_inclusive(first_from_type.as_relation_type(), last_key_from_type.as_relation_type());
                let filter_fn = self.filter_fn.clone();
                // TODO: we could cache the range byte arrays computed inside the thing_manager, for this case
                let iterator: Filter<RelationRolePlayerIterator, Arc<RolePlayerFilterFn>> = thing_manager
                    .get_relation_role_players_from_relation_type_range(snapshot, key_range)
                    .filter::<_, RolePlayerFilterFn>(filter_fn);
                let as_tuples: RolePlayerUnboundedSortedRelation =
                    iterator.map(relation_role_player_to_tuple_relation_player_role);
                Ok(TupleIterator::RolePlayerUnbounded(SortedTupleIterator::new(
                    as_tuples,
                    self.tuple_positions.clone(),
                    &self.variable_modes,
                )))
            }
            TernaryIterateMode::UnboundInverted => {
                debug_assert!(self.relation_cache.is_some());

                let (min_player_type, max_player_type) = min_max_types(&*self.player_types);
                let player_type_range =
                    KeyRange::new_inclusive(min_player_type.as_object_type(), max_player_type.as_object_type());

                if let Some([relation]) = self.relation_cache.as_deref() {
                    // no heap allocs needed if there is only 1 iterator
                    let iterator = thing_manager
                        .get_relation_role_players_from_relation_and_player_type_range(
                            snapshot,
                            relation.as_reference(),
                            // TODO: this should be just the types owned by the one instance's type in the cache!
                            player_type_range,
                        )
                        .filter::<_, RolePlayerFilterFn>(self.filter_fn.clone());
                    let as_tuples: RolePlayerUnboundedSortedPlayerSingle =
                        iterator.map::<Result<Tuple<'_>, _>, _>(relation_role_player_to_tuple_player_relation_role);
                    Ok(TupleIterator::RolePlayerUnboundedInvertedSingle(SortedTupleIterator::new(
                        as_tuples,
                        self.tuple_positions.clone(),
                        &self.variable_modes,
                    )))
                } else {
                    // TODO: we could create a reusable space for these temporarily held iterators
                    //       so we don't have allocate again before the merging iterator
                    let relations = self.relation_cache.as_ref().unwrap().iter();
                    let iterators = relations
                        .map(|relation| {
                            Ok(Peekable::new(
                                thing_manager.get_relation_role_players_from_relation_and_player_type_range(
                                    snapshot,
                                    relation.as_reference(),
                                    player_type_range.clone(),
                                ),
                            ))
                        })
                        .collect::<Result<Vec<_>, _>>()?;

                    // note: this will always have to heap alloc, if we use don't have a re-usable/small-vec'ed priority queue somewhere
                    let merged: KMergeBy<RelationRolePlayerIterator, RelationRolePlayerOrderingFn> =
                        KMergeBy::new(iterators, Self::compare_by_player_then_relation);
                    let filtered: Filter<
                        KMergeBy<RelationRolePlayerIterator, RelationRolePlayerOrderingFn>,
                        Arc<RolePlayerFilterFn>,
                    > = merged.filter::<_, RolePlayerFilterFn>(self.filter_fn.clone());
                    let as_tuples: RolePlayerUnboundedSortedPlayerMerged =
                        filtered.map::<Result<Tuple<'_>, _>, _>(relation_role_player_to_tuple_player_relation_role);
                    Ok(TupleIterator::RolePlayerUnboundedInvertedMerged(SortedTupleIterator::new(
                        as_tuples,
                        self.tuple_positions.clone(),
                        &self.variable_modes,
                    )))
                }
            }

            TernaryIterateMode::BoundFrom => {
                debug_assert!(row.width() > self.role_player.relation().as_usize());
                let (min_player_type, max_player_type) = min_max_types(&*self.player_types);
                let player_type_range =
                    KeyRange::new_inclusive(min_player_type.as_object_type(), max_player_type.as_object_type());

                let iterator = match row.get(self.role_player.relation()) {
                    VariableValue::Thing(Thing::Relation(relation)) => thing_manager
                        .get_relation_role_players_from_relation_and_player_type_range(
                            snapshot,
                            relation.as_reference(),
                            player_type_range,
                        ),
                    _ => unreachable!("RolePlayer relation must be a relation."),
                };
                let filtered = iterator.filter::<_, RolePlayerFilterFn>(self.filter_fn.clone());
                let as_tuples: RolePlayerBoundedRelationSortedPlayer =
                    filtered.map::<Result<Tuple<'_>, _>, _>(relation_role_player_to_tuple_relation_player_role);
                Ok(TupleIterator::RolePlayerBoundedRelation(SortedTupleIterator::new(
                    as_tuples,
                    self.tuple_positions.clone(),
                    &self.variable_modes,
                )))
            }
            TernaryIterateMode::BoundFromBoundTo => {
                debug_assert!(row.width() > self.role_player.relation().as_usize());
                debug_assert!(row.width() > self.role_player.player().as_usize());
                let relation = row.get(self.role_player.relation()).as_thing().as_relation();
                let player = row.get(self.role_player.player()).as_thing().as_object();
                let iterator =
                    thing_manager.get_relation_role_players_from_relation_and_player(snapshot, relation, player);
                let filtered = iterator.filter::<_, RolePlayerFilterFn>(self.filter_fn.clone());
                let as_tuples: RolePlayerBoundedRelationSortedPlayer =
                    filtered.map::<Result<Tuple<'_>, _>, _>(relation_role_player_to_tuple_relation_player_role);
                Ok(TupleIterator::RolePlayerBoundedRelation(SortedTupleIterator::new(
                    as_tuples,
                    self.tuple_positions.clone(),
                    &self.variable_modes,
                )))
            }
        }
    }

    fn create_role_player_filter_relations_players_roles(
        relation_to_player: Arc<BTreeMap<Type, Vec<Type>>>,
        player_to_role: Arc<BTreeMap<Type, HashSet<Type>>>,
    ) -> Arc<RolePlayerFilterFn> {
        Arc::new(move |result| {
            let Ok((rel, rp, _)) = result else {
                return true;
            };
            let Some(player_types) = relation_to_player.get(&Type::from(rel.type_())) else {
                return false;
            };
            let player_type = Type::from(rp.player().type_());
            let role_type = Type::from(rp.role_type());
            player_types.contains(&player_type)
                && player_to_role.get(&player_type).is_some_and(|role_types| role_types.contains(&role_type))
        })
    }

    fn create_noop_filter() -> Arc<RolePlayerFilterFn> {
        Arc::new(|_| true)
    }

    fn compare_by_player_then_relation(
        pair: (
            &Result<(Relation<'_>, RolePlayer<'_>, u64), ConceptReadError>,
            &Result<(Relation<'_>, RolePlayer<'_>, u64), ConceptReadError>,
        ),
    ) -> Ordering {
        if let (Ok((rel_1, rp_1, _)), Ok((rel_2, rp_2, _))) = pair {
            (rp_1.player(), rel_1).cmp(&(rp_2.player(), rel_2))
        } else {
            Ordering::Equal
        }
    }
}

fn min_max_types<'a>(types: impl IntoIterator<Item = &'a Type>) -> (Type, Type) {
    match types.into_iter().minmax() {
        MinMaxResult::NoElements => unreachable!("Empty type iterator"),
        MinMaxResult::OneElement(item) => (item.clone(), item.clone()),
        MinMaxResult::MinMax(min, max) => (min.clone(), max.clone()),
    }
}

impl RolePlayerExecutor {}
