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

use answer::{variable_value::VariableValue, Thing, Type};
use compiler::match_::instructions::LinksInstruction;
use concept::{
    error::ConceptReadError,
    thing::{
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
use resource::constants::traversal::CONSTANT_CONCEPT_LIMIT;
use storage::{key_range::KeyRange, snapshot::ReadableSnapshot};

use crate::{
    batch::ImmutableRow,
    instruction::{
        iterator::{SortedTupleIterator, TupleIterator},
        tuple::{
            links_to_tuple_player_relation_role, links_to_tuple_relation_player_role, LinksToTupleFn, Tuple,
            TuplePositions, TupleResult,
        },
        TernaryIterateMode, VariableModes,
    },
    VariablePosition,
};

pub(crate) struct LinksExecutor {
    links: ir::pattern::constraint::Links<VariablePosition>,

    iterate_mode: TernaryIterateMode,
    variable_modes: VariableModes,

    tuple_positions: TuplePositions,

    relation_player_types: Arc<BTreeMap<Type, Vec<Type>>>, // vecs are in sorted order
    player_types: Arc<HashSet<Type>>,

    filter_fn: Arc<LinksFilterFn>,
    relation_cache: Option<Vec<Relation<'static>>>,
}

pub(crate) type LinksUnboundedSortedRelation =
    Map<Filter<LinksIterator, Arc<LinksFilterFn>>, LinksToTupleFn, AsHkt![TupleResult<'_>]>;
pub(crate) type LinksUnboundedSortedPlayerSingle =
    Map<Filter<LinksIterator, Arc<LinksFilterFn>>, LinksToTupleFn, AsHkt![TupleResult<'_>]>;
pub(crate) type LinksUnboundedSortedPlayerMerged =
    Map<Filter<KMergeBy<LinksIterator, LinksOrderingFn>, Arc<LinksFilterFn>>, LinksToTupleFn, AsHkt![TupleResult<'_>]>;
pub(crate) type LinksBoundedRelationSortedPlayer =
    Map<Filter<LinksIterator, Arc<LinksFilterFn>>, LinksToTupleFn, AsHkt![TupleResult<'_>]>;
pub(crate) type LinksBoundedRelationPlayer =
    Map<Filter<LinksIterator, Arc<LinksFilterFn>>, LinksToTupleFn, AsHkt![TupleResult<'_>]>;

pub(crate) type LinksFilterFn =
    dyn for<'a, 'b> FnHktHelper<&'a Result<(Relation<'b>, RolePlayer<'b>, u64), ConceptReadError>, bool>;

pub(crate) type LinksOrderingFn = for<'a, 'b> fn(
    (
        &'a Result<(Relation<'a>, RolePlayer<'a>, u64), ConceptReadError>,
        &'b Result<(Relation<'b>, RolePlayer<'b>, u64), ConceptReadError>,
    ),
) -> Ordering;

impl LinksExecutor {
    pub(crate) fn new(
        links: LinksInstruction<VariablePosition>,
        variable_modes: VariableModes,
        sort_by: Option<VariablePosition>,
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
    ) -> Result<Self, ConceptReadError> {
        debug_assert!(!variable_modes.all_inputs());
        let relation_player_types = links.relation_to_player_types().clone();
        debug_assert!(!relation_player_types.is_empty());
        let player_types = links.player_types().clone();
        let player_role_types = links.relation_to_role_types().clone();
        let links = links.links;
        let iterate_mode = TernaryIterateMode::new(links.relation(), links.player(), &variable_modes, sort_by);
        let filter_fn = create_links_filter_relations_players_roles(relation_player_types.clone(), player_role_types);
        let output_tuple_positions = if iterate_mode == TernaryIterateMode::UnboundInverted {
            TuplePositions::Triple([links.player(), links.relation(), links.role_type()])
        } else {
            TuplePositions::Triple([links.relation(), links.player(), links.role_type()])
        };

        let relation_cache = if iterate_mode == TernaryIterateMode::UnboundInverted {
            let mut cache = Vec::new();
            for type_ in relation_player_types.keys() {
                let instances: Vec<Relation<'static>> = thing_manager
                    .get_relations_in(snapshot, type_.as_relation_type())
                    .map_static(|result| Ok(result?.clone().into_owned()))
                    .try_collect()?;
                cache.extend(instances);
            }
            debug_assert!(cache.len() < CONSTANT_CONCEPT_LIMIT);
            Some(cache)
        } else {
            None
        };

        Ok(Self {
            links,
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
                let iterator: Filter<LinksIterator, Arc<LinksFilterFn>> = thing_manager
                    .get_links_by_relation_type_range(snapshot, key_range)
                    .filter::<_, LinksFilterFn>(filter_fn);
                let as_tuples: LinksUnboundedSortedRelation = iterator.map(links_to_tuple_relation_player_role);
                Ok(TupleIterator::LinksUnbounded(SortedTupleIterator::new(
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
                        .get_links_by_relation_and_player_type_range(
                            snapshot,
                            relation.as_reference(),
                            // TODO: this should be just the types owned by the one instance's type in the cache!
                            player_type_range,
                        )
                        .filter::<_, LinksFilterFn>(self.filter_fn.clone());
                    let as_tuples: LinksUnboundedSortedPlayerSingle =
                        iterator.map::<Result<Tuple<'_>, _>, _>(links_to_tuple_player_relation_role);
                    Ok(TupleIterator::LinksUnboundedInvertedSingle(SortedTupleIterator::new(
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
                            Ok(Peekable::new(thing_manager.get_links_by_relation_and_player_type_range(
                                snapshot,
                                relation.as_reference(),
                                player_type_range.clone(),
                            )))
                        })
                        .collect::<Result<Vec<_>, _>>()?;

                    // note: this will always have to heap alloc, if we use don't have a re-usable/small-vec'ed priority queue somewhere
                    let merged: KMergeBy<LinksIterator, LinksOrderingFn> =
                        KMergeBy::new(iterators, compare_by_player_then_relation);
                    let filtered: Filter<KMergeBy<LinksIterator, LinksOrderingFn>, Arc<LinksFilterFn>> =
                        merged.filter::<_, LinksFilterFn>(self.filter_fn.clone());
                    let as_tuples: LinksUnboundedSortedPlayerMerged =
                        filtered.map::<Result<Tuple<'_>, _>, _>(links_to_tuple_player_relation_role);
                    Ok(TupleIterator::LinksUnboundedInvertedMerged(SortedTupleIterator::new(
                        as_tuples,
                        self.tuple_positions.clone(),
                        &self.variable_modes,
                    )))
                }
            }

            TernaryIterateMode::BoundFrom => {
                debug_assert!(row.width() > self.links.relation().as_usize());
                let (min_player_type, max_player_type) = min_max_types(&*self.player_types);
                let player_type_range =
                    KeyRange::new_inclusive(min_player_type.as_object_type(), max_player_type.as_object_type());

                let iterator = match row.get(self.links.relation()) {
                    VariableValue::Thing(Thing::Relation(relation)) => thing_manager
                        .get_links_by_relation_and_player_type_range(
                            snapshot,
                            relation.as_reference(),
                            player_type_range,
                        ),
                    _ => unreachable!("Links relation must be a relation."),
                };
                let filtered = iterator.filter::<_, LinksFilterFn>(self.filter_fn.clone());
                let as_tuples: LinksBoundedRelationSortedPlayer = filtered.map(links_to_tuple_relation_player_role);
                Ok(TupleIterator::LinksBoundedRelation(SortedTupleIterator::new(
                    as_tuples,
                    self.tuple_positions.clone(),
                    &self.variable_modes,
                )))
            }

            TernaryIterateMode::BoundFromBoundTo => {
                debug_assert!(row.width() > self.links.relation().as_usize());
                debug_assert!(row.width() > self.links.player().as_usize());
                let relation = row.get(self.links.relation()).as_thing().as_relation();
                let player = row.get(self.links.player()).as_thing().as_object();
                let iterator = thing_manager.get_links_by_relation_and_player(snapshot, relation, player);
                let filtered = iterator.filter::<_, LinksFilterFn>(self.filter_fn.clone());
                let as_tuples: LinksBoundedRelationSortedPlayer = filtered.map(links_to_tuple_relation_player_role);
                Ok(TupleIterator::LinksBoundedRelationPlayer(SortedTupleIterator::new(
                    as_tuples,
                    self.tuple_positions.clone(),
                    &self.variable_modes,
                )))
            }
        }
    }
}

fn create_links_filter_relations_players_roles(
    relation_to_player: Arc<BTreeMap<Type, Vec<Type>>>,
    player_to_role: Arc<BTreeMap<Type, HashSet<Type>>>,
) -> Arc<LinksFilterFn> {
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

fn min_max_types<'a>(types: impl IntoIterator<Item = &'a Type>) -> (Type, Type) {
    match types.into_iter().minmax() {
        MinMaxResult::NoElements => unreachable!("Empty type iterator"),
        MinMaxResult::OneElement(item) => (item.clone(), item.clone()),
        MinMaxResult::MinMax(min, max) => (min.clone(), max.clone()),
    }
}
