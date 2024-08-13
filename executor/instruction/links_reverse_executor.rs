/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    cmp::Ordering,
    collections::{BTreeMap, HashSet},
    sync::Arc,
};

use answer::Type;
use compiler::instruction::constraint::instructions::LinksReverseInstruction;
use concept::{
    error::ConceptReadError,
    thing::{
        object::Object,
        relation::{LinksIterator, Relation, RolePlayer},
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

use crate::{
    batch::ImmutableRow,
    instruction::{
        iterator::{inverted_instances_cache, SortedTupleIterator, TupleIterator},
        tuple::{
            links_to_tuple_player_relation_role, links_to_tuple_relation_player_role, LinksToTupleFn, TuplePositions,
            TupleResult,
        },
        TernaryIterateMode, VariableModes,
    },
    VariablePosition,
};

pub(crate) struct LinksReverseExecutor {
    links: ir::pattern::constraint::Links<VariablePosition>,

    iterate_mode: TernaryIterateMode,
    variable_modes: VariableModes,

    tuple_positions: TuplePositions,

    player_relation_types: Arc<BTreeMap<Type, Vec<Type>>>, // vecs are in sorted order
    relation_types: Arc<HashSet<Type>>,

    filter_fn: Arc<LinksFilterFn>,
    player_cache: Option<Vec<Object<'static>>>,
}

pub(crate) type LinksReverseUnboundedSortedPlayer =
    Map<Filter<LinksIterator, Arc<LinksFilterFn>>, LinksToTupleFn, AsHkt![TupleResult<'_>]>;
pub(crate) type LinksReverseUnboundedSortedRelationSingle =
    Map<Filter<LinksIterator, Arc<LinksFilterFn>>, LinksToTupleFn, AsHkt![TupleResult<'_>]>;
pub(crate) type LinksReverseUnboundedSortedRelationMerged =
    Map<Filter<KMergeBy<LinksIterator, LinksOrderingFn>, Arc<LinksFilterFn>>, LinksToTupleFn, AsHkt![TupleResult<'_>]>;
pub(crate) type LinksReverseBoundedPlayerSortedRelation =
    Map<Filter<LinksIterator, Arc<LinksFilterFn>>, LinksToTupleFn, AsHkt![TupleResult<'_>]>;
pub(crate) type LinksReverseBoundedPlayerRelation =
    Map<Filter<LinksIterator, Arc<LinksFilterFn>>, LinksToTupleFn, AsHkt![TupleResult<'_>]>;

pub(crate) type LinksFilterFn =
    dyn for<'a, 'b> FnHktHelper<&'a Result<(Relation<'b>, RolePlayer<'b>, u64), ConceptReadError>, bool>;

pub(crate) type LinksOrderingFn = for<'a, 'b> fn(
    (
        &'a Result<(Relation<'a>, RolePlayer<'a>, u64), ConceptReadError>,
        &'b Result<(Relation<'b>, RolePlayer<'b>, u64), ConceptReadError>,
    ),
) -> Ordering;

impl LinksReverseExecutor {
    pub(crate) fn new(
        links_reverse: LinksReverseInstruction<VariablePosition>,
        variable_modes: VariableModes,
        sort_by: Option<VariablePosition>,
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
    ) -> Result<Self, ConceptReadError> {
        debug_assert!(!variable_modes.all_inputs());
        let player_relation_types = links_reverse.player_to_relation_types().clone();
        debug_assert!(!player_relation_types.is_empty());
        let relation_types = links_reverse.relation_types().clone();
        let relation_role_types = links_reverse.relation_to_role_types().clone();
        let links = links_reverse.links;
        let iterate_mode = TernaryIterateMode::new(links.player(), links.relation(), &variable_modes, sort_by);
        let filter_fn = create_links_filter_relations_players_roles(player_relation_types.clone(), relation_role_types);
        let output_tuple_positions = if iterate_mode == TernaryIterateMode::UnboundInverted {
            TuplePositions::Triple([links.relation(), links.player(), links.role_type()])
        } else {
            TuplePositions::Triple([links.player(), links.relation(), links.role_type()])
        };

        let player_cache = if iterate_mode == TernaryIterateMode::UnboundInverted {
            Some(inverted_instances_cache(
                player_relation_types.keys().map(|t| t.as_object_type()),
                snapshot,
                thing_manager,
            )?)
        } else {
            None
        };

        Ok(Self {
            links,
            iterate_mode,
            variable_modes,
            tuple_positions: output_tuple_positions,
            player_relation_types,
            relation_types,
            filter_fn,
            player_cache,
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
                let min_player_type = self.player_relation_types.first_key_value().unwrap().0;
                let max_player_type = self.player_relation_types.last_key_value().unwrap().0;
                let key_range =
                    KeyRange::new_inclusive(min_player_type.as_object_type(), max_player_type.as_object_type());
                let filter_fn = self.filter_fn.clone();
                // TODO: we could cache the range byte arrays computed inside the thing_manager, for this case
                let iterator: Filter<LinksIterator, Arc<LinksFilterFn>> = thing_manager
                    .get_links_reverse_by_player_type_range(snapshot, key_range)
                    .filter::<_, LinksFilterFn>(filter_fn);
                let as_tuples: LinksReverseUnboundedSortedPlayer = iterator.map(links_to_tuple_player_relation_role);
                Ok(TupleIterator::LinksReverseUnbounded(SortedTupleIterator::new(
                    as_tuples,
                    self.tuple_positions.clone(),
                    &self.variable_modes,
                )))
            }

            TernaryIterateMode::UnboundInverted => {
                debug_assert!(self.player_cache.is_some());

                let (min_relation_type, max_relation_type) = min_max_types(&*self.relation_types);
                let relation_type_range =
                    KeyRange::new_inclusive(min_relation_type.as_relation_type(), max_relation_type.as_relation_type());

                if let Some([player]) = self.player_cache.as_deref() {
                    // no heap allocs needed if there is only 1 iterator
                    let filtered = thing_manager
                        .get_links_reverse_by_player_and_relation_type_range(
                            snapshot,
                            player.as_reference(),
                            // TODO: this should be just the types owned by the one instance's type in the cache!
                            relation_type_range,
                        )
                        .filter::<_, LinksFilterFn>(self.filter_fn.clone());
                    let as_tuples: LinksReverseUnboundedSortedRelationSingle =
                        filtered.map(links_to_tuple_relation_player_role);
                    Ok(TupleIterator::LinksReverseUnboundedInvertedSingle(SortedTupleIterator::new(
                        as_tuples,
                        self.tuple_positions.clone(),
                        &self.variable_modes,
                    )))
                } else {
                    // TODO: we could create a reusable space for these temporarily held iterators
                    //       so we don't have allocate again before the merging iterator
                    let relations = self.player_cache.as_ref().unwrap().iter();
                    let iterators = relations
                        .map(|relation| {
                            Ok(Peekable::new(thing_manager.get_links_reverse_by_player_and_relation_type_range(
                                snapshot,
                                relation.as_reference(),
                                relation_type_range.clone(),
                            )))
                        })
                        .collect::<Result<Vec<_>, _>>()?;

                    // note: this will always have to heap alloc, if we use don't have a re-usable/small-vec'ed priority queue somewhere
                    let merged: KMergeBy<LinksIterator, LinksOrderingFn> =
                        KMergeBy::new(iterators, compare_by_relation_then_player);
                    let filtered: Filter<KMergeBy<LinksIterator, LinksOrderingFn>, Arc<LinksFilterFn>> =
                        merged.filter::<_, LinksFilterFn>(self.filter_fn.clone());
                    let as_tuples: LinksReverseUnboundedSortedRelationMerged =
                        filtered.map(links_to_tuple_relation_player_role);
                    Ok(TupleIterator::LinksReverseUnboundedInvertedMerged(SortedTupleIterator::new(
                        as_tuples,
                        self.tuple_positions.clone(),
                        &self.variable_modes,
                    )))
                }
            }

            TernaryIterateMode::BoundFrom => {
                debug_assert!(row.width() > self.links.player().as_usize());
                let (min_relation_type, max_relation_type) = min_max_types(&*self.relation_types);
                let relation_type_range =
                    KeyRange::new_inclusive(min_relation_type.as_relation_type(), max_relation_type.as_relation_type());

                let iterator = thing_manager.get_links_reverse_by_player_and_relation_type_range(
                    snapshot,
                    row.get(self.links.player()).as_thing().as_object().as_reference(),
                    relation_type_range,
                );
                let filtered = iterator.filter::<_, LinksFilterFn>(self.filter_fn.clone());
                let as_tuples: LinksReverseBoundedPlayerSortedRelation =
                    filtered.map(links_to_tuple_player_relation_role);
                Ok(TupleIterator::LinksReverseBoundedPlayer(SortedTupleIterator::new(
                    as_tuples,
                    self.tuple_positions.clone(),
                    &self.variable_modes,
                )))
            }

            TernaryIterateMode::BoundFromBoundTo => {
                debug_assert!(row.width() > self.links.player().as_usize());
                debug_assert!(row.width() > self.links.relation().as_usize());
                let relation = row.get(self.links.relation()).as_thing().as_relation();
                let player = row.get(self.links.player()).as_thing().as_object();
                let iterator = thing_manager.get_links_by_relation_and_player(snapshot, relation, player); // NOTE: not reverse, no difference
                let filtered = iterator.filter::<_, LinksFilterFn>(self.filter_fn.clone());
                let as_tuples: LinksReverseBoundedPlayerSortedRelation =
                    filtered.map(links_to_tuple_player_relation_role);
                Ok(TupleIterator::LinksReverseBoundedPlayerRelation(SortedTupleIterator::new(
                    as_tuples,
                    self.tuple_positions.clone(),
                    &self.variable_modes,
                )))
            }
        }
    }
}

fn create_links_filter_relations_players_roles(
    player_to_relation: Arc<BTreeMap<Type, Vec<Type>>>,
    relation_to_role: Arc<BTreeMap<Type, HashSet<Type>>>,
) -> Arc<LinksFilterFn> {
    Arc::new(move |result| {
        let Ok((rel, rp, _)) = result else {
            return true;
        };
        let Some(relation_types) = player_to_relation.get(&Type::from(rp.player().type_())) else {
            return false;
        };
        let relation_type = Type::from(rel.type_());
        let role_type = Type::from(rp.role_type());
        relation_types.contains(&relation_type)
            && relation_to_role.get(&relation_type).is_some_and(|role_types| role_types.contains(&role_type))
    })
}

fn compare_by_relation_then_player(
    pair: (
        &Result<(Relation<'_>, RolePlayer<'_>, u64), ConceptReadError>,
        &Result<(Relation<'_>, RolePlayer<'_>, u64), ConceptReadError>,
    ),
) -> Ordering {
    if let (Ok((rel_1, rp_1, _)), Ok((rel_2, rp_2, _))) = pair {
        (rel_1, rp_1.player()).cmp(&(rel_2, rp_2.player()))
    } else {
        Ordering::Equal
    }
}

fn min_max_types<'a>(types: impl IntoIterator<Item = &'a Type>) -> (Type, Type) {
    match types.into_iter().minmax() {
        MinMaxResult::NoElements => unreachable!("Empty type iterator"),
        MinMaxResult::OneElement(item) => (item.clone(), item.clone()),
        MinMaxResult::MinMax(min, max) => (min.clone(), max.clone()),
    }
}
