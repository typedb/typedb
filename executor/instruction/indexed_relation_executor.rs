/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    fmt, iter,
    sync::Arc,
};

use answer::{variable_value::VariableValue, Thing, Type};
use compiler::{
    executable::match_::instructions::{thing::IndexedRelationInstruction, VariableMode, VariableModes},
    ExecutorVariable,
};
use concept::{
    error::ConceptReadError,
    thing::{
        object::{Object, ObjectAPI},
        relation::{IndexedRelationPlayers, IndexedRelationsIterator, Relation},
        thing_manager::ThingManager,
    },
    type_::role_type::RoleType,
};
use itertools::{kmerge_by, Itertools, KMergeBy};
use resource::constants::traversal::CONSTANT_CONCEPT_LIMIT;
use storage::snapshot::ReadableSnapshot;

use crate::{
    instruction::{
        iterator::{SortedTupleIterator, TupleIterator},
        links_executor::LinksExecutor,
        tuple::{Tuple, TuplePositions, TupleResult},
        Checker, FilterFn, FilterMapFn,
    },
    pipeline::stage::ExecutionContext,
    row::MaybeOwnedRow,
};

pub(super) type IndexedRelationTupleIterator<I> = iter::FilterMap<I, Box<IndexedRelationFilterMapFn>>;

pub(crate) type IndexedRelationTupleIteratorSingle = IndexedRelationTupleIterator<IndexedRelationsIterator>;
pub(crate) type IndexedRelationTupleIteratorMerged =
    IndexedRelationTupleIterator<KMergeBy<IndexedRelationsIterator, IndexedRelationOrderingFn>>;

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
pub(super) type IndexedRelationFilterMapFn = FilterMapFn<(IndexedRelationPlayers, u64), Tuple<'static>>;
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

    iterate_mode: IndexedRelationIterateMode,
    variable_modes: VariableModes,

    tuple_positions: TuplePositions,
    variables_lexicographically_ordered: [ExecutorVariable; 5], // TODO: can we just store an Arc<Fn> to do this instead of allocating it each time??

    pub(crate) relation_to_player_start_types: Arc<BTreeMap<Type, Vec<Type>>>,
    pub(crate) player_start_to_player_end_types: Arc<BTreeMap<Type, BTreeSet<Type>>>,
    pub(crate) role_start_types: Arc<BTreeSet<RoleType>>,
    pub(crate) role_end_types: Arc<BTreeSet<RoleType>>,

    filter_fn: Arc<IndexedRelationFilterFn>,
    start_player_cache: Option<Vec<Object>>,

    checker: Checker<(IndexedRelationPlayers, u64)>,
}

impl fmt::Debug for IndexedRelationExecutor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "IndexedRelationExecutor")
    }
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
        debug_assert!([role_start, role_end, player_start, player_end, relation]
            .iter()
            .all(|v| { variable_modes.get(*v).is_some() }));
        let iterate_mode =
            IndexedRelationIterateMode::new(player_start, player_end, relation, &variable_modes, sort_by);
        let filter_fn = create_indexed_players_filter(
            player_start_to_player_end_types.clone(),
            role_start_types.clone(),
            role_end_types.clone(),
        );

        // sort variable always comes first, then inputs, and any lexicographically ordered items come afterward
        // note that we don't record Roles as 'bound' (though they may be), and sometimes Relations are also bound but may need post-filtering
        let variables_lexicographically_ordered = [player_start, player_end, relation, role_start, role_end];

        static MODE_PRIORITY: [VariableMode; 4] =
            [VariableMode::Input, VariableMode::Output, VariableMode::Count, VariableMode::Check];

        let mut output_tuple_positions: [Option<ExecutorVariable>; 5] = [None; 5];
        // index 0 is always the sort variable
        match iterate_mode {
            IndexedRelationIterateMode::Unbound => output_tuple_positions[0] = Some(player_start),
            IndexedRelationIterateMode::UnboundInvertedToPlayer | IndexedRelationIterateMode::BoundStart => {
                output_tuple_positions[0] = Some(player_end);
            }
            IndexedRelationIterateMode::BoundStartBoundEnd => output_tuple_positions[0] = Some(relation),
            IndexedRelationIterateMode::BoundStartBoundEndBoundRelation => output_tuple_positions[0] = Some(role_start),
        };
        for output_index in 1..5 {
            let preceding_variable = output_tuple_positions[output_index - 1].unwrap();
            let preceding_variable_mode = if Some(preceding_variable) == output_tuple_positions[0] {
                // special case: we need to allow inputs to follow, so ignore actual mode of sort variable and treat it as input
                VariableMode::Input
            } else {
                variable_modes.get(preceding_variable).unwrap()
            };
            'mode: for &mode in MODE_PRIORITY.iter().skip_while(|&&mode| mode != preceding_variable_mode) {
                // find first unused variable with this mode (else, try the next mode)
                for variable in variables_lexicographically_ordered {
                    let variable_mode = variable_modes.get(variable).unwrap();
                    if !output_tuple_positions.contains(&Some(variable)) && mode == variable_mode {
                        output_tuple_positions[output_index] = Some(variable);
                        break 'mode;
                    }
                }
            }
            debug_assert!(output_tuple_positions[output_index].is_some());
        }
        debug_assert!(output_tuple_positions.iter().all(|option| option.is_some()));

        let output_tuple_positions = TuplePositions::Quintuple(output_tuple_positions);

        let checker = Checker::<(IndexedRelationPlayers, u64)>::new(
            checks,
            HashMap::from([
                (player_start, EXTRACT_PLAYER_START),
                (player_end, EXTRACT_PLAYER_END),
                (relation, EXTRACT_RELATION),
                (role_start, EXTRACT_ROLE_START),
                (role_end, EXTRACT_ROLE_END),
            ]),
        );

        let start_player_cache = if iterate_mode == IndexedRelationIterateMode::UnboundInvertedToPlayer {
            let mut cache = Vec::new();
            for type_ in player_start_to_player_end_types.keys() {
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
            player_start,
            player_end,
            relation,
            role_start,
            role_end,

            iterate_mode,
            variable_modes,
            tuple_positions: output_tuple_positions.clone(),
            variables_lexicographically_ordered,

            relation_to_player_start_types,
            player_start_to_player_end_types,
            role_start_types,
            role_end_types,
            filter_fn,

            start_player_cache,
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

        let (relation, start_role, end_role) = self.may_get_relation_and_roles(row.as_reference());

        let positions = self.tuple_positions.clone();
        let component_ordering = self.variables_lexicographically_ordered;
        let filter_map_for_row: Box<IndexedRelationFilterMapFn> = Box::new(move |item| match filter(&item) {
            Ok(true) => match check(&item) {
                Ok(true) => match verify_relation_and_roles(&item, relation, start_role, end_role) {
                    Ok(true) | Err(_) => Some(to_tuple(item, positions.clone(), component_ordering)),
                    Ok(false) => None,
                },
                Ok(false) => None,
                Err(_) => Some(to_tuple(item, positions.clone(), component_ordering)),
            },
            Ok(false) => None,
            Err(_) => Some(to_tuple(item, positions.clone(), component_ordering)),
        });

        let snapshot = &**context.snapshot();
        let thing_manager = context.thing_manager();

        match self.iterate_mode {
            IndexedRelationIterateMode::Unbound => {
                // want it sorted by start player, so we must merge an iterator per relation type
                if self.relation_to_player_start_types.len() == 1 {
                    let &relation_type = self.relation_to_player_start_types.keys().next().unwrap();
                    let as_tuples = thing_manager
                        .get_indexed_relations_in(snapshot, relation_type.as_relation_type())
                        .expect("Relation index should be available")
                        .filter_map(filter_map_for_row);
                    Ok(TupleIterator::IndexedRelationsSingle(SortedTupleIterator::new(
                        as_tuples,
                        self.tuple_positions.clone(),
                        &self.variable_modes,
                    )))
                } else {
                    let iterators = self
                        .relation_to_player_start_types
                        .keys()
                        .map(|relation_type| {
                            thing_manager
                                .get_indexed_relations_in(snapshot, relation_type.as_relation_type())
                                .expect("Relation index should be available")
                        })
                        .collect_vec();
                    let merged: KMergeBy<IndexedRelationsIterator, IndexedRelationOrderingFn> =
                        kmerge_by(iterators, compare_indexed_players);
                    let as_tuples = merged.filter_map(filter_map_for_row);
                    Ok(TupleIterator::IndexedRelationsMerged(SortedTupleIterator::new(
                        as_tuples,
                        self.tuple_positions.clone(),
                        &self.variable_modes,
                    )))
                }
            }
            IndexedRelationIterateMode::UnboundInvertedToPlayer => {
                debug_assert!(self.start_player_cache.is_some());
                let mut iterators = Vec::new();
                self.start_player_cache.as_ref().into_iter().flat_map(|start_players| start_players.iter()).for_each(
                    |start_player| {
                        for relation_type in self.relation_to_player_start_types.keys() {
                            iterators.push(
                                start_player
                                    .get_indexed_relations(snapshot, thing_manager, relation_type.as_relation_type())
                                    .expect("Relation index expected to be available"),
                            )
                        }
                    },
                );
                let merged: KMergeBy<IndexedRelationsIterator, IndexedRelationOrderingFn> =
                    kmerge_by(iterators, compare_indexed_players_inverted);
                let as_tuples: IndexedRelationTupleIteratorMerged = merged.filter_map(filter_map_for_row);
                Ok(TupleIterator::IndexedRelationsMerged(SortedTupleIterator::new(
                    as_tuples,
                    self.tuple_positions.clone(),
                    &self.variable_modes,
                )))
            }
            IndexedRelationIterateMode::BoundStart => {
                let start_player = match row.get(self.player_start.as_position().unwrap()) {
                    VariableValue::Thing(thing) => thing.as_object(),
                    _ => unreachable!("Start player just be a thing object"),
                };
                if self.relation_to_player_start_types.len() == 1 {
                    let relation_type = self.relation_to_player_start_types.keys().next().unwrap().as_relation_type();
                    let as_tuples: IndexedRelationTupleIteratorSingle = start_player
                        .get_indexed_relations(snapshot, thing_manager, relation_type)
                        .expect("Relation index should be available")
                        .filter_map(filter_map_for_row);
                    Ok(TupleIterator::IndexedRelationsSingle(SortedTupleIterator::new(
                        as_tuples,
                        self.tuple_positions.clone(),
                        &self.variable_modes,
                    )))
                } else {
                    let iterators = self
                        .relation_to_player_start_types
                        .keys()
                        .map(|relation_type| {
                            start_player
                                .get_indexed_relations(snapshot, thing_manager, relation_type.as_relation_type())
                                .expect("Relation index should be available")
                        })
                        .collect_vec();
                    let merged: KMergeBy<IndexedRelationsIterator, IndexedRelationOrderingFn> =
                        kmerge_by(iterators, compare_indexed_players);
                    let as_tuples: IndexedRelationTupleIteratorMerged = merged.filter_map(filter_map_for_row);
                    Ok(TupleIterator::IndexedRelationsMerged(SortedTupleIterator::new(
                        as_tuples,
                        self.tuple_positions.clone(),
                        &self.variable_modes,
                    )))
                }
            }
            IndexedRelationIterateMode::BoundStartBoundEnd => {
                let start_player = match row.get(self.player_start.as_position().unwrap()) {
                    VariableValue::Thing(thing) => thing.as_object(),
                    _ => unreachable!("Start player just be a thing object"),
                };
                let end_player = match row.get(self.player_end.as_position().unwrap()) {
                    VariableValue::Thing(thing) => thing.as_object(),
                    _ => unreachable!("End player just be a thing object"),
                };
                if self.relation_to_player_start_types.len() == 1 {
                    let relation_type = self.relation_to_player_start_types.keys().next().unwrap().as_relation_type();
                    let as_tuples: IndexedRelationTupleIteratorSingle = start_player
                        .get_indexed_relations_with_player(snapshot, thing_manager, end_player, relation_type)
                        .expect("Relation index should be available")
                        .filter_map(filter_map_for_row);
                    Ok(TupleIterator::IndexedRelationsSingle(SortedTupleIterator::new(
                        as_tuples,
                        self.tuple_positions.clone(),
                        &self.variable_modes,
                    )))
                } else {
                    let iterators = self
                        .relation_to_player_start_types
                        .keys()
                        .map(|relation_type| {
                            start_player
                                .get_indexed_relations_with_player(
                                    snapshot,
                                    thing_manager,
                                    end_player,
                                    relation_type.as_relation_type(),
                                )
                                .expect("Relation index should be available")
                        })
                        .collect_vec();
                    let merged: KMergeBy<IndexedRelationsIterator, IndexedRelationOrderingFn> =
                        kmerge_by(iterators, compare_indexed_players);
                    let as_tuples: IndexedRelationTupleIteratorMerged = merged.filter_map(filter_map_for_row);
                    Ok(TupleIterator::IndexedRelationsMerged(SortedTupleIterator::new(
                        as_tuples,
                        self.tuple_positions.clone(),
                        &self.variable_modes,
                    )))
                }
            }
            IndexedRelationIterateMode::BoundStartBoundEndBoundRelation => {
                let start_player = match row.get(self.player_start.as_position().unwrap()) {
                    VariableValue::Thing(thing) => thing.as_object(),
                    _ => unreachable!("Start player just be a thing object"),
                };
                let end_player = match row.get(self.player_end.as_position().unwrap()) {
                    VariableValue::Thing(thing) => thing.as_object(),
                    _ => unreachable!("End player just be a thing object"),
                };
                let relation = match row.get(self.relation.as_position().unwrap()) {
                    VariableValue::Thing(thing) => thing.as_relation(),
                    _ => unreachable!("Indexed relation must be a thing relation"),
                };
                let as_tuples: IndexedRelationTupleIteratorSingle = start_player
                    .get_indexed_relation_roles_with_player_and_relation(snapshot, thing_manager, end_player, relation)
                    .expect("Relation index should be available")
                    .filter_map(filter_map_for_row);
                Ok(TupleIterator::IndexedRelationsSingle(SortedTupleIterator::new(
                    as_tuples,
                    self.tuple_positions.clone(),
                    &self.variable_modes,
                )))
            }
        }
    }

    fn may_get_relation_and_roles(
        &self,
        row: MaybeOwnedRow<'_>,
    ) -> (Option<Relation>, Option<RoleType>, Option<RoleType>) {
        let relation = match self.relation {
            ExecutorVariable::RowPosition(position) => {
                if position.as_usize() < row.len() {
                    if let VariableValue::Thing(thing) = row.get(position) {
                        Some(thing.as_relation())
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
            ExecutorVariable::Internal(_) => None,
        };
        let start_role = match self.role_start {
            ExecutorVariable::RowPosition(position) => {
                if position.as_usize() < row.len() {
                    if let VariableValue::Type(type_) = row.get(position) {
                        Some(type_.as_role_type())
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
            ExecutorVariable::Internal(_) => None,
        };
        let end_role = match self.role_end {
            ExecutorVariable::RowPosition(position) => {
                if position.as_usize() < row.len() {
                    if let VariableValue::Type(type_) = row.get(position) {
                        Some(type_.as_role_type())
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
            ExecutorVariable::Internal(_) => None,
        };
        (relation, start_role, end_role)
    }
}

impl fmt::Display for IndexedRelationExecutor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "[{} -(role: {}, relation: {}, role: {})-> {}], mode={}",
            self.player_start, self.role_start, self.relation, self.role_end, self.player_end, &self.iterate_mode
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
            Ok(((player_start, player_end, _relation, role_start, role_end), _)) => {
                (player_start, player_end, role_start, role_end)
            }
            Err(err) => return Err(err.clone()),
        };
        let Some(end_player_types) = start_player_to_end_player_types.get(&Type::from(player_start.type_())) else {
            return Ok(false);
        };
        Ok(end_player_types.contains(&Type::from(player_end.type_()))
            && start_role_types.contains(role_start)
            && end_role_types.contains(role_end))
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

fn compare_indexed_players_inverted(
    left: &Result<(IndexedRelationPlayers, u64), Box<ConceptReadError>>,
    right: &Result<(IndexedRelationPlayers, u64), Box<ConceptReadError>>,
) -> bool {
    if let (
        Ok(((start_1, end_1, rel_1, start_role_1, end_role_1), _)),
        Ok(((start_2, end_2, rel_2, start_role_2, end_role_2), _)),
    ) = (left, right)
    {
        (end_1, start_1, rel_1, start_role_1, end_role_1) < (end_2, start_2, rel_2, start_role_2, end_role_2)
    } else {
        false
    }
}

fn verify_relation_and_roles(
    item: &Result<(IndexedRelationPlayers, u64), Box<ConceptReadError>>,
    expected_relation: Option<Relation>,
    expected_start_role: Option<RoleType>,
    expected_end_role: Option<RoleType>,
) -> Result<bool, &ConceptReadError> {
    match item {
        Ok(((_, _, item_relation, item_start_role, item_end_role), _)) => {
            Ok(expected_relation.map(|relation| relation == *item_relation).unwrap_or(true)
                && expected_start_role.map(|role| role == *item_start_role).unwrap_or(true)
                && expected_end_role.map(|role| role == *item_end_role).unwrap_or(true))
        }
        Err(err) => Err(err),
    }
}

fn to_tuple(
    indexed_relation_players: Result<(IndexedRelationPlayers, u64), Box<ConceptReadError>>,
    tuple_positions: TuplePositions,
    component_ordering: [ExecutorVariable; 5],
) -> TupleResult<'static> {
    let (components, _) = indexed_relation_players?;
    let tuple: [VariableValue<'static>; 5] = std::array::from_fn(|i| {
        let variable_at_position = tuple_positions.as_quintuple()[i].unwrap();
        let source_component_index = component_ordering.iter().position(|var| *var == variable_at_position).unwrap();
        match source_component_index {
            0 => VariableValue::Thing(components.0.into()),
            1 => VariableValue::Thing(components.1.into()),
            2 => VariableValue::Thing(Thing::Relation(components.2)),
            3 => VariableValue::Type(Type::RoleType(components.3)),
            4 => VariableValue::Type(Type::RoleType(components.4)),
            _ => unreachable!("only 5 components exist"),
        }
    });
    TupleResult::Ok(Tuple::Quintuple(tuple))
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(crate) enum IndexedRelationIterateMode {
    // [x, y, r, a, b] = standard sort order
    Unbound,
    // [y, x, r, a, b] sort order
    UnboundInvertedToPlayer,
    // [X, y, r, a, b] sort order
    BoundStart,
    // [X, Y, r, a, b]
    BoundStartBoundEnd,

    // note: this needs a specific check for when all 3 are bound, we can't just handle a bound relation on its own without all prefixes satisfied
    BoundStartBoundEndBoundRelation,
}

impl IndexedRelationIterateMode {
    pub(crate) fn new(
        player_start: ExecutorVariable,
        player_end: ExecutorVariable,
        relation: ExecutorVariable,
        var_modes: &VariableModes,
        sort_by: ExecutorVariable,
    ) -> Self {
        debug_assert!(!var_modes.all_inputs());
        let is_start_bound = var_modes.get(player_start) == Some(VariableMode::Input);
        let is_end_bound = var_modes.get(player_end) == Some(VariableMode::Input);
        let is_rel_bound = var_modes.get(relation) == Some(VariableMode::Input);
        if is_rel_bound && is_end_bound && is_start_bound {
            Self::BoundStartBoundEndBoundRelation
        } else if is_end_bound {
            assert!(is_start_bound); // QP should have inverted the direction
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

impl fmt::Display for IndexedRelationIterateMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}
