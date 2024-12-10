/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */


use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;

use itertools::Itertools;
use answer::Type;
use compiler::executable::match_::instructions::thing::IndexedRelationInstruction;

use compiler::executable::match_::instructions::{VariableMode, VariableModes};
use compiler::ExecutorVariable;
use concept::error::ConceptReadError;
use concept::thing::thing_manager::ThingManager;
use ir::pattern::constraint::IndexedRelation;
use ir::pattern::Vertex;
use storage::snapshot::ReadableSnapshot;

use crate::instruction::iterator::TupleIterator;
use crate::instruction::TernaryIterateMode;
use crate::instruction::tuple::TuplePositions;
use crate::pipeline::stage::ExecutionContext;
use crate::row::MaybeOwnedRow;

pub(crate) struct IndexedRelationExecutor {
    indexed_relation: IndexedRelation<ExecutorVariable>,

    // TODO??
    iterate_mode: TernaryIterateMode,
    variable_modes: VariableModes,

    tuple_positions: TuplePositions,

    // relation_player_types: Arc<BTreeMap<Type, Vec<Type>>>, // vecs are in sorted order
    // relation_type_range: Bounds<RelationType>,
    // player_type_range: Bounds<ObjectType>,

    // filter_fn: Arc<crate::instruction::links_executor::LinksFilterFn>,
    // relation_cache: Option<Vec<Relation>>,

    // checker: Checker<(Relation, RolePlayer, u64)>,
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

        // let relation_player_types = links.relation_to_player_types().clone();
        // debug_assert!(!relation_player_types.is_empty());
        // let player_types = links.player_types().clone();
        // let player_role_types = links.relation_to_role_types().clone();
        // let LinksInstruction { links, checks, .. } = links;
        let iterate_mode = IndexedRelationIterateIterateMode::new(
            *indexed_relation.player_start(),
            *indexed_relation.player_end(),
            *indexed_relation.relation(),
            *indexed_relation.role_type_start(),
            &variable_modes,
            &sort_by
        );
        // let filter_fn = create_links_filter_relations_players_roles(relation_player_types.clone(), player_role_types);
        //
        let start_player = indexed_relation.player_start().as_variable().unwrap();
        let end_player = indexed_relation.player_end().as_variable().unwrap();
        let relation = indexed_relation.relation().as_variable().unwrap();
        let start_role = indexed_relation.role_start().as_variable().unwrap();
        let end_role = indexed_relation.role_end().as_variable().unwrap();

        let output_tuple_positions = match iterate_mode {
            IndexedRelationIterateIterateMode::Unbound  => {
                TuplePositions::Quintuple([Some(start_player), Some(end_player), Some(relation), Some(start_role), Some(end_role)])
            },
            IndexedRelationIterateIterateMode::UnboundInvertedToPlayer => {
                TuplePositions::Quintuple([Some(end_player), Some(start_player), Some(relation), Some(start_role), Some(end_role)])
            }
            IndexedRelationIterateIterateMode::BoundStart => {
                TuplePositions::Quintuple([Some(start_player), Some(end_player), Some(relation), Some(start_role), Some(end_role)])
            }
            IndexedRelationIterateIterateMode::BoundFromBoundTo => {
                TuplePositions::Triple([Some(role_type), Some(relation), Some(player)])
            }
        };
        //
        // let checker = Checker::<(Relation, RolePlayer, _)>::new(
        //     checks,
        //     HashMap::from([(relation, crate::instruction::links_executor::EXTRACT_RELATION), (player, crate::instruction::links_executor::EXTRACT_PLAYER), (role_type, crate::instruction::links_executor::EXTRACT_ROLE)]),
        // );
        //
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
        // Ok(Self {
        //     links,
        //     iterate_mode,
        //     variable_modes,
        //     tuple_positions: output_tuple_positions,
        //     relation_player_types,
        //     relation_type_range,
        //     player_type_range,
        //     filter_fn,
        //     relation_cache,
        //     checker,
        // })
        todo!()
    }

    pub(crate) fn get_iterator(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: MaybeOwnedRow<'_>,
    ) -> Result<TupleIterator, Box<ConceptReadError>> {
        // let filter = self.filter_fn.clone();
        // let check = self.checker.filter_for_row(context, &row);
        // let filter_for_row: Box<crate::instruction::links_executor::LinksFilterMapFn> = Box::new(move |item| match filter(&item) {
        //     Ok(true) => match check(&item) {
        //         Ok(true) | Err(_) => Some(item),
        //         Ok(false) => None,
        //     },
        //     Ok(false) => None,
        //     Err(_) => Some(item),
        // });
        //
        // let snapshot = &**context.snapshot();
        // let thing_manager = context.thing_manager();
        //
        // match self.iterate_mode {
        //     TernaryIterateMode::Unbound => {
        //         // TODO: we could cache the range byte arrays computed inside the thing_manager, for this case
        //         let iterator = thing_manager.get_links_by_relation_type_range(snapshot, &self.relation_type_range);
        //         let as_tuples: crate::instruction::links_executor::LinksUnboundedSortedRelation =
        //             iterator.filter_map(filter_for_row).map(links_to_tuple_relation_player_role as _);
        //         Ok(TupleIterator::LinksUnbounded(SortedTupleIterator::new(
        //             as_tuples,
        //             self.tuple_positions.clone(),
        //             &self.variable_modes,
        //         )))
        //     }
        //
        //     TernaryIterateMode::UnboundInverted => {
        //         debug_assert!(self.relation_cache.is_some());
        //         if let Some([relation]) = self.relation_cache.as_deref() {
        //             // no heap allocs needed if there is only 1 iterator
        //             let iterator = thing_manager.get_links_by_relation_and_player_type_range(
        //                 snapshot,
        //                 *relation,
        //                 // TODO: this should be just the types owned by the one instance's type in the cache!
        //                 &self.player_type_range,
        //             );
        //             let as_tuples: crate::instruction::links_executor::LinksUnboundedSortedPlayerSingle =
        //                 iterator.filter_map(filter_for_row).map(links_to_tuple_player_relation_role);
        //             Ok(TupleIterator::LinksUnboundedInvertedSingle(SortedTupleIterator::new(
        //                 as_tuples,
        //                 self.tuple_positions.clone(),
        //                 &self.variable_modes,
        //             )))
        //         } else {
        //             // TODO: we could create a reusable space for these temporarily held iterators
        //             //       so we don't have allocate again before the merging iterator
        //             let relations = self.relation_cache.as_ref().unwrap().iter();
        //             let iterators = relations
        //                 .map(|&relation| {
        //                     thing_manager.get_links_by_relation_and_player_type_range(
        //                         snapshot,
        //                         relation,
        //                         &self.player_type_range,
        //                     )
        //                 })
        //                 .collect_vec();
        //
        //             // note: this will always have to heap alloc, if we use don't have a re-usable/small-vec'ed priority queue somewhere
        //             let merged: KMergeBy<LinksIterator, crate::instruction::links_executor::LinksOrderingFn> =
        //                 kmerge_by(iterators, compare_by_player_then_relation);
        //             let as_tuples: crate::instruction::links_executor::LinksUnboundedSortedPlayerMerged =
        //                 merged.filter_map(filter_for_row).map(links_to_tuple_player_relation_role);
        //             Ok(TupleIterator::LinksUnboundedInvertedMerged(SortedTupleIterator::new(
        //                 as_tuples,
        //                 self.tuple_positions.clone(),
        //                 &self.variable_modes,
        //             )))
        //         }
        //     }
        //
        //     TernaryIterateMode::BoundFrom => {
        //         let relation = self.links.relation().as_variable().unwrap().as_position().unwrap();
        //         debug_assert!(row.len() > relation.as_usize());
        //         let iterator = match row.get(relation) {
        //             &VariableValue::Thing(Thing::Relation(relation)) => thing_manager
        //                 .get_links_by_relation_and_player_type_range(snapshot, relation, &self.player_type_range),
        //             _ => unreachable!("Links relation must be a relation."),
        //         };
        //         let as_tuples: crate::instruction::links_executor::LinksBoundedRelationSortedPlayer =
        //             iterator.filter_map(filter_for_row).map(links_to_tuple_player_relation_role);
        //         Ok(TupleIterator::LinksBoundedRelation(SortedTupleIterator::new(
        //             as_tuples,
        //             self.tuple_positions.clone(),
        //             &self.variable_modes,
        //         )))
        //     }
        //
        //     TernaryIterateMode::BoundFromBoundTo => {
        //         let relation = self.links.relation().as_variable().unwrap().as_position().unwrap();
        //         let player = self.links.player().as_variable().unwrap().as_position().unwrap();
        //         debug_assert!(row.len() > relation.as_usize());
        //         debug_assert!(row.len() > player.as_usize());
        //         let relation = row.get(relation).as_thing().as_relation();
        //         let player = row.get(player).as_thing().as_object();
        //         let iterator = thing_manager.get_links_by_relation_and_player(snapshot, relation, player);
        //         let as_tuples: crate::instruction::links_executor::LinksBoundedRelationSortedPlayer =
        //             iterator.filter_map(filter_for_row).map(links_to_tuple_role_relation_player);
        //         Ok(TupleIterator::LinksBoundedRelationPlayer(SortedTupleIterator::new(
        //             as_tuples,
        //             self.tuple_positions.clone(),
        //             &self.variable_modes,
        //         )))
        //     }
        // }
        todo!()
    }
}

impl fmt::Display for IndexedRelationExecutor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "[{}], mode={}", &self.indexed_relation, &self.iterate_mode)
    }
}

fn create_links_filter_relations_players_roles(
    relation_to_player: Arc<BTreeMap<Type, Vec<Type>>>,
    player_to_role: Arc<BTreeMap<Type, BTreeSet<Type>>>,
) -> Arc<crate::instruction::links_executor::LinksFilterFn> {
    Arc::new(move |result| {
        let (rel, rp) = match result {
            Ok((rel, rp, _)) => (rel, rp),
            Err(err) => return Err(err.clone()),
        };
        let Some(player_types) = relation_to_player.get(&Type::from(rel.type_())) else {
            return Ok(false);
        };
        let player_type = Type::from(rp.player().type_());
        let role_type = Type::from(rp.role_type());
        Ok(player_types.contains(&player_type)
            && player_to_role.get(&player_type).is_some_and(|role_types| role_types.contains(&role_type)))
    })
}
//
// fn compare_by_player_then_relation(
//     left: &Result<(Relation, RolePlayer, u64), Box<ConceptReadError>>,
//     right: &Result<(Relation, RolePlayer, u64), Box<ConceptReadError>>,
// ) -> bool {
//     if let (Ok((rel_1, rp_1, _)), Ok((rel_2, rp_2, _))) = (left, right) {
//         (rp_1.player(), rel_1) < (rp_2.player(), rel_2)
//     } else {
//         false
//     }
// }


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
        player_start_role: ExecutorVariable,
        var_modes: &VariableModes,
        sort_by: ExecutorVariable,
    ) -> Self {
        debug_assert!(var_modes.len() == 3);
        debug_assert!(!var_modes.all_inputs());

        let is_start_bound = var_modes.get(player_start) == Some(VariableMode::Input);
        let is_end_bound = var_modes.get(player_end) == Some(VariableMode::Input);
        let is_rel_bound = var_modes.get(relation) == Some(VariableMode::Input);
        let is_start_role_bound = var_modes.get(player_start_role) == Some(VariableMode::Input);
        if is_start_role_bound {
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
        } else if Some(sort_by) == player_end.as_variable() {
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
