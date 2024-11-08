/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::Arc,
};

use answer::Type;
use compiler::{executable::match_::instructions::thing::LinksReverseInstruction, ExecutorVariable};
use concept::{
    error::ConceptReadError,
    thing::{
        object::Object,
        relation::{LinksIterator, Relation, RolePlayer},
        thing_manager::ThingManager,
    },
};
use itertools::{Itertools, MinMaxResult};
use lending_iterator::{kmerge::KMergeBy, AsHkt, LendingIterator, Peekable};
use resource::constants::traversal::CONSTANT_CONCEPT_LIMIT;
use storage::{
    key_range::{KeyRange, RangeEnd, RangeStart},
    snapshot::ReadableSnapshot,
};

use crate::{
    instruction::{
        iterator::{SortedTupleIterator, TupleIterator},
        links_executor::{
            LinksFilterFn, LinksOrderingFn, LinksTupleIterator, EXTRACT_PLAYER, EXTRACT_RELATION, EXTRACT_ROLE,
        },
        tuple::{
            links_to_tuple_player_relation_role, links_to_tuple_relation_player_role,
            links_to_tuple_role_relation_player, TuplePositions,
        },
        Checker, TernaryIterateMode, VariableModes,
    },
    pipeline::stage::ExecutionContext,
    row::MaybeOwnedRow,
};

pub(crate) struct LinksReverseExecutor {
    links: ir::pattern::constraint::Links<ExecutorVariable>,

    iterate_mode: TernaryIterateMode,
    variable_modes: VariableModes,

    tuple_positions: TuplePositions,

    player_relation_types: Arc<BTreeMap<Type, Vec<Type>>>, // vecs are in sorted order
    relation_types: Arc<BTreeSet<Type>>,

    filter_fn: Arc<LinksFilterFn>,
    player_cache: Option<Vec<Object<'static>>>,

    checker: Checker<(AsHkt![Relation<'_>], AsHkt![RolePlayer<'_>], u64)>,
}

pub(crate) type LinksReverseUnboundedSortedPlayer = LinksTupleIterator<LinksIterator>;
pub(crate) type LinksReverseUnboundedSortedRelationSingle = LinksTupleIterator<LinksIterator>;
pub(crate) type LinksReverseUnboundedSortedRelationMerged =
    LinksTupleIterator<KMergeBy<LinksIterator, LinksOrderingFn>>;
pub(crate) type LinksReverseBoundedPlayerSortedRelation = LinksTupleIterator<LinksIterator>;
pub(crate) type LinksReverseBoundedPlayerRelation = LinksTupleIterator<LinksIterator>;

impl LinksReverseExecutor {
    pub(crate) fn new(
        links_reverse: LinksReverseInstruction<ExecutorVariable>,
        variable_modes: VariableModes,
        sort_by: ExecutorVariable,
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
    ) -> Result<Self, ConceptReadError> {
        debug_assert!(!variable_modes.all_inputs());
        let player_relation_types = links_reverse.player_to_relation_types().clone();
        debug_assert!(!player_relation_types.is_empty());
        let relation_types = links_reverse.relation_types().clone();
        let relation_role_types = links_reverse.relation_to_role_types().clone();
        let LinksReverseInstruction { links, checks, .. } = links_reverse;
        let iterate_mode = TernaryIterateMode::new(links.player(), links.relation(), &variable_modes, sort_by);
        let filter_fn = create_links_filter_relations_players_roles(player_relation_types.clone(), relation_role_types);

        let relation = links.relation().as_variable().unwrap();
        let player = links.player().as_variable().unwrap();
        let role_type = links.role_type().as_variable().unwrap();

        let output_tuple_positions = match iterate_mode {
            TernaryIterateMode::Unbound => TuplePositions::Triple([Some(player), Some(relation), Some(role_type)]),
            TernaryIterateMode::UnboundInverted => {
                TuplePositions::Triple([Some(relation), Some(player), Some(role_type)])
            }
            TernaryIterateMode::BoundFrom => TuplePositions::Triple([Some(relation), Some(player), Some(role_type)]),
            TernaryIterateMode::BoundFromBoundTo => {
                TuplePositions::Triple([Some(role_type), Some(relation), Some(player)])
            }
        };

        let checker = Checker::<(Relation<'_>, RolePlayer<'_>, _)>::new(
            checks,
            HashMap::from([(relation, EXTRACT_RELATION), (player, EXTRACT_PLAYER), (role_type, EXTRACT_ROLE)]),
        );

        let player_cache = if iterate_mode == TernaryIterateMode::UnboundInverted {
            let mut cache = Vec::new();
            for type_ in player_relation_types.keys() {
                let instances: Vec<Object<'static>> = thing_manager
                    .get_objects_in(snapshot, type_.as_object_type())
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
            player_relation_types,
            relation_types,
            filter_fn,
            player_cache,
            checker,
        })
    }

    pub(crate) fn get_iterator(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: MaybeOwnedRow<'_>,
    ) -> Result<TupleIterator, ConceptReadError> {
        let filter = self.filter_fn.clone();
        let check = self.checker.filter_for_row(context, &row);
        let filter_for_row: Box<LinksFilterFn> = Box::new(move |item| match filter(item) {
            Ok(true) => check(item),
            fail => fail,
        });

        let snapshot = &**context.snapshot();
        let thing_manager = context.thing_manager();

        match self.iterate_mode {
            TernaryIterateMode::Unbound => {
                let min_player_type = self.player_relation_types.first_key_value().unwrap().0;
                let max_player_type = self.player_relation_types.last_key_value().unwrap().0;
                let key_range = KeyRange::new_variable_width(
                    RangeStart::Inclusive(min_player_type.as_object_type()),
                    RangeEnd::EndPrefixInclusive(max_player_type.as_object_type()),
                );
                // TODO: we could cache the range byte arrays computed inside the thing_manager, for this case
                let iterator = thing_manager.get_links_reverse_by_player_type_range(snapshot, key_range);
                let as_tuples: LinksReverseUnboundedSortedPlayer = iterator
                    .try_filter::<_, LinksFilterFn, (Relation<'_>, RolePlayer<'_>, _), _>(filter_for_row)
                    .map(links_to_tuple_player_relation_role);
                Ok(TupleIterator::LinksReverseUnbounded(SortedTupleIterator::new(
                    as_tuples,
                    self.tuple_positions.clone(),
                    &self.variable_modes,
                )))
            }

            TernaryIterateMode::UnboundInverted => {
                debug_assert!(self.player_cache.is_some());

                let (min_relation_type, max_relation_type) = min_max_types(&*self.relation_types);
                let relation_type_range = KeyRange::new_variable_width(
                    RangeStart::Inclusive(min_relation_type.as_relation_type()),
                    RangeEnd::EndPrefixInclusive(max_relation_type.as_relation_type()),
                );

                if let Some([player]) = self.player_cache.as_deref() {
                    // no heap allocs needed if there is only 1 iterator
                    let filtered = thing_manager.get_links_reverse_by_player_and_relation_type_range(
                        snapshot,
                        player.as_reference(),
                        // TODO: this should be just the types owned by the one instance's type in the cache!
                        relation_type_range,
                    );
                    let as_tuples: LinksReverseUnboundedSortedRelationSingle = filtered
                        .try_filter::<_, LinksFilterFn, (Relation<'_>, RolePlayer<'_>, _), _>(filter_for_row)
                        .map(links_to_tuple_relation_player_role);
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
                    let as_tuples: LinksReverseUnboundedSortedRelationMerged = merged
                        .try_filter::<_, LinksFilterFn, (Relation<'_>, RolePlayer<'_>, _), _>(filter_for_row)
                        .map(links_to_tuple_relation_player_role);
                    Ok(TupleIterator::LinksReverseUnboundedInvertedMerged(SortedTupleIterator::new(
                        as_tuples,
                        self.tuple_positions.clone(),
                        &self.variable_modes,
                    )))
                }
            }

            TernaryIterateMode::BoundFrom => {
                let player = self.links.player().as_variable().unwrap().as_position().unwrap();
                debug_assert!(row.len() > player.as_usize());
                let (min_relation_type, max_relation_type) = min_max_types(&*self.relation_types);
                let relation_type_range = KeyRange::new_variable_width(
                    RangeStart::Inclusive(min_relation_type.as_relation_type()),
                    RangeEnd::EndPrefixInclusive(max_relation_type.as_relation_type()),
                );

                let mut iterator = thing_manager.get_links_reverse_by_player_and_relation_type_range(
                    snapshot,
                    row.get(player).as_thing().as_object().as_reference(),
                    relation_type_range,
                );
                let has_next = iterator.peek().is_some();
                let as_tuples: LinksReverseBoundedPlayerSortedRelation = iterator
                    .try_filter::<_, LinksFilterFn, (Relation<'_>, RolePlayer<'_>, _), _>(filter_for_row)
                    .map(links_to_tuple_relation_player_role);
                Ok(TupleIterator::LinksReverseBoundedPlayer(SortedTupleIterator::new(
                    as_tuples,
                    self.tuple_positions.clone(),
                    &self.variable_modes,
                )))
            }

            TernaryIterateMode::BoundFromBoundTo => {
                let relation = self.links.relation().as_variable().unwrap().as_position().unwrap();
                let player = self.links.player().as_variable().unwrap().as_position().unwrap();
                debug_assert!(row.len() > player.as_usize());
                debug_assert!(row.len() > relation.as_usize());
                let relation = row.get(relation).as_thing().as_relation();
                let player = row.get(player).as_thing().as_object();
                let iterator = thing_manager.get_links_by_relation_and_player(snapshot, relation, player); // NOTE: not reverse, no difference
                let as_tuples: LinksReverseBoundedPlayerSortedRelation = iterator
                    .try_filter::<_, LinksFilterFn, (Relation<'_>, RolePlayer<'_>, _), _>(filter_for_row)
                    .map(links_to_tuple_role_relation_player);
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
    relation_to_role: Arc<BTreeMap<Type, BTreeSet<Type>>>,
) -> Arc<LinksFilterFn> {
    Arc::new(move |result| {
        let (rel, rp) = match result {
            Ok((rel, rp, _)) => (rel, rp),
            Err(err) => return Err(err.clone()),
        };
        let Some(relation_types) = player_to_relation.get(&Type::from(rp.player().type_())) else {
            return Ok(false);
        };
        let relation_type = Type::from(rel.type_());
        let role_type = Type::from(rp.role_type());
        Ok(relation_types.contains(&relation_type)
            && relation_to_role.get(&relation_type).is_some_and(|role_types| role_types.contains(&role_type)))
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
