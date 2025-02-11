/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    fmt,
    ops::Bound,
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
    type_::{object_type::ObjectType, relation_type::RelationType},
};
use itertools::{kmerge_by, Itertools, KMergeBy};
use primitive::Bounds;
use resource::constants::traversal::CONSTANT_CONCEPT_LIMIT;
use storage::snapshot::ReadableSnapshot;

use crate::{
    instruction::{
        iterator::{SortedTupleIterator, TupleIterator},
        links_executor::{
            may_get_role, verify_role, LinksFilterFn, LinksFilterMapFn, LinksOrderingFn, LinksTupleIteratorMerged,
            LinksTupleIteratorSingle, EXTRACT_PLAYER, EXTRACT_RELATION, EXTRACT_ROLE,
        },
        min_max_types,
        owns_executor::OwnsExecutor,
        tuple::{
            links_to_tuple_player_relation_role, links_to_tuple_relation_player_role,
            links_to_tuple_role_relation_player, TuplePositions,
        },
        Checker, LinksIterateMode, VariableModes,
    },
    pipeline::stage::ExecutionContext,
    row::MaybeOwnedRow,
};

pub(crate) struct LinksReverseExecutor {
    links: ir::pattern::constraint::Links<ExecutorVariable>,

    iterate_mode: LinksIterateMode,
    variable_modes: VariableModes,

    tuple_positions: TuplePositions,

    player_relation_types: Arc<BTreeMap<Type, Vec<Type>>>, // vecs are in sorted order
    player_type_range: Bounds<ObjectType>,
    relation_type_range: Bounds<RelationType>,

    filter_fn: Arc<LinksFilterFn>,
    player_cache: Option<Vec<Object>>,

    checker: Checker<(Relation, RolePlayer, u64)>,
}

impl fmt::Debug for LinksReverseExecutor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LinksReverseExecutor")
    }
}

impl LinksReverseExecutor {
    pub(crate) fn new(
        links_reverse: LinksReverseInstruction<ExecutorVariable>,
        variable_modes: VariableModes,
        sort_by: ExecutorVariable,
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
    ) -> Result<Self, Box<ConceptReadError>> {
        debug_assert!(!variable_modes.all_inputs());
        let player_relation_types = links_reverse.player_to_relation_types().clone();
        debug_assert!(!player_relation_types.is_empty());
        let relation_types = links_reverse.relation_types().clone();
        let relation_role_types = links_reverse.relation_to_role_types().clone();
        let LinksReverseInstruction { links, checks, .. } = links_reverse;
        let iterate_mode = LinksIterateMode::new(links.player(), links.relation(), &variable_modes, sort_by);
        let filter_fn = create_links_filter_relations_players_roles(player_relation_types.clone(), relation_role_types);

        let relation = links.relation().as_variable().unwrap();
        let player = links.player().as_variable().unwrap();
        let role_type = links.role_type().as_variable().unwrap();

        let output_tuple_positions = match iterate_mode {
            LinksIterateMode::Unbound => TuplePositions::Triple([Some(player), Some(relation), Some(role_type)]),
            LinksIterateMode::UnboundInverted => {
                TuplePositions::Triple([Some(relation), Some(player), Some(role_type)])
            }
            LinksIterateMode::BoundFrom => TuplePositions::Triple([Some(relation), Some(player), Some(role_type)]),
            LinksIterateMode::BoundFromBoundTo => {
                TuplePositions::Triple([Some(role_type), Some(relation), Some(player)])
            }
        };

        let checker = Checker::<(Relation, RolePlayer, _)>::new(
            checks,
            HashMap::from([(relation, EXTRACT_RELATION), (player, EXTRACT_PLAYER), (role_type, EXTRACT_ROLE)]),
        );

        let player_type_range = (
            Bound::Included(player_relation_types.first_key_value().unwrap().0.as_object_type()),
            Bound::Included(player_relation_types.last_key_value().unwrap().0.as_object_type()),
        );

        let (min_relation_type, max_relation_type) = min_max_types(relation_types.iter());
        let relation_type_range = (
            Bound::Included(min_relation_type.as_relation_type()),
            Bound::Included(max_relation_type.as_relation_type()),
        );

        let player_cache = if iterate_mode == LinksIterateMode::UnboundInverted {
            let mut cache = Vec::new();
            for type_ in player_relation_types.keys() {
                let instances: Vec<Object> =
                    thing_manager.get_objects_in(snapshot, type_.as_object_type()).try_collect()?;
                cache.extend(instances);
            }
            #[cfg(debug_assertions)]
            if cache.len() < CONSTANT_CONCEPT_LIMIT {
                eprintln!("DEBUG_ASSERT_FAILURE: cache.len() > CONSTANT_CONCEPT_LIMIT");
            }
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
            player_type_range,
            relation_type_range,
            filter_fn,
            player_cache,
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

        let existing_role = may_get_role(self.links.role_type().as_variable().unwrap(), row.as_reference());
        let filter_for_row: Box<LinksFilterMapFn> = Box::new(move |item| match filter(&item) {
            Ok(true) => match check(&item) {
                Ok(true) => match verify_role(&item, existing_role) {
                    Ok(true) | Err(_) => Some(item),
                    Ok(false) => None,
                },
                Ok(false) => None,
                Err(_) => Some(item),
            },
            Ok(false) => None,
            Err(_) => Some(item),
        });

        let snapshot = &**context.snapshot();
        let thing_manager = context.thing_manager();

        match self.iterate_mode {
            LinksIterateMode::Unbound => {
                // TODO: we could cache the range byte arrays computed inside the thing_manager, for this case
                let iterator = thing_manager.get_links_reverse_by_player_type_range(snapshot, &self.player_type_range);
                let as_tuples: LinksTupleIteratorSingle =
                    iterator.filter_map(filter_for_row).map(links_to_tuple_player_relation_role);
                Ok(TupleIterator::LinksReverseSingle(SortedTupleIterator::new(
                    as_tuples,
                    self.tuple_positions.clone(),
                    &self.variable_modes,
                )))
            }

            LinksIterateMode::UnboundInverted => {
                debug_assert!(self.player_cache.is_some());
                if let Some([player]) = self.player_cache.as_deref() {
                    // no heap allocs needed if there is only 1 iterator
                    let filtered = thing_manager.get_links_reverse_by_player_and_relation_type_range(
                        snapshot,
                        *player,
                        // TODO: this should be just the types owned by the one instance's type in the cache!
                        &self.relation_type_range,
                    );
                    let as_tuples: LinksTupleIteratorSingle =
                        filtered.filter_map(filter_for_row).map(links_to_tuple_relation_player_role);
                    Ok(TupleIterator::LinksReverseSingle(SortedTupleIterator::new(
                        as_tuples,
                        self.tuple_positions.clone(),
                        &self.variable_modes,
                    )))
                } else {
                    // TODO: we could create a reusable space for these temporarily held iterators
                    //       so we don't have allocate again before the merging iterator
                    let relations = self.player_cache.as_ref().unwrap().iter();
                    let iterators = relations
                        .map(|&relation| {
                            thing_manager.get_links_reverse_by_player_and_relation_type_range(
                                snapshot,
                                relation,
                                &self.relation_type_range,
                            )
                        })
                        .collect_vec();

                    // note: this will always have to heap alloc, if we use don't have a re-usable/small-vec'ed priority queue somewhere
                    let merged: KMergeBy<LinksIterator, LinksOrderingFn> =
                        kmerge_by(iterators, compare_by_relation_then_player);
                    let as_tuples: LinksTupleIteratorMerged =
                        merged.filter_map(filter_for_row).map(links_to_tuple_relation_player_role);
                    Ok(TupleIterator::LinksReverseMerged(SortedTupleIterator::new(
                        as_tuples,
                        self.tuple_positions.clone(),
                        &self.variable_modes,
                    )))
                }
            }

            LinksIterateMode::BoundFrom => {
                let player = self.links.player().as_variable().unwrap().as_position().unwrap();
                debug_assert!(row.len() > player.as_usize());
                let iterator = thing_manager.get_links_reverse_by_player_and_relation_type_range(
                    snapshot,
                    row.get(player).as_thing().as_object(),
                    &self.relation_type_range,
                );
                let as_tuples: LinksTupleIteratorSingle =
                    iterator.filter_map(filter_for_row).map(links_to_tuple_relation_player_role);
                Ok(TupleIterator::LinksReverseSingle(SortedTupleIterator::new(
                    as_tuples,
                    self.tuple_positions.clone(),
                    &self.variable_modes,
                )))
            }

            LinksIterateMode::BoundFromBoundTo => {
                let relation = self.links.relation().as_variable().unwrap().as_position().unwrap();
                let player = self.links.player().as_variable().unwrap().as_position().unwrap();
                debug_assert!(row.len() > player.as_usize());
                debug_assert!(row.len() > relation.as_usize());
                let relation = row.get(relation).as_thing().as_relation();
                let player = row.get(player).as_thing().as_object();
                let iterator = thing_manager.get_links_by_relation_and_player(snapshot, relation, player); // NOTE: not reverse, no difference
                let as_tuples: LinksTupleIteratorSingle =
                    iterator.filter_map(filter_for_row).map(links_to_tuple_role_relation_player);
                Ok(TupleIterator::LinksReverseSingle(SortedTupleIterator::new(
                    as_tuples,
                    self.tuple_positions.clone(),
                    &self.variable_modes,
                )))
            }
        }
    }
}

impl fmt::Display for LinksReverseExecutor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Reverse[{}], mode={}", &self.links, &self.iterate_mode)
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
    left: &Result<(Relation, RolePlayer, u64), Box<ConceptReadError>>,
    right: &Result<(Relation, RolePlayer, u64), Box<ConceptReadError>>,
) -> bool {
    if let (Ok((rel_1, rp_1, _)), Ok((rel_2, rp_2, _))) = (left, right) {
        (rel_1, rp_1.player()) < (rel_2, rp_2.player())
    } else {
        false
    }
}
