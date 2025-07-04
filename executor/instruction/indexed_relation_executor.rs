/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet, HashMap},
    fmt,
    ops::Bound,
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
        ThingAPI,
    },
    type_::{object_type::ObjectType, role_type::RoleType, TypeAPI},
};
use encoding::graph::{
    thing::vertex_object::{ObjectID, ObjectVertex},
    type_::vertex::{TypeID, TypeVertexEncoding},
    Typed,
};
use itertools::Itertools;
use lending_iterator::{kmerge::KMergeBy, LendingIterator, Peekable};
use primitive::Bounds;
use resource::{constants::traversal::CONSTANT_CONCEPT_LIMIT, profile::StorageCounters};
use storage::snapshot::ReadableSnapshot;

use crate::{
    instruction::{
        iterator::{SortedTupleIterator, TupleIterator, TupleSeekable},
        tuple::{unsafe_compare_result_tuple, Tuple, TupleOrderingFn, TuplePositions, TupleResult},
        Checker, FilterFn, FilterMapUnchangedFn,
    },
    pipeline::stage::ExecutionContext,
    row::MaybeOwnedRow,
};

pub(crate) type IndexedRelationTupleIteratorSingle = IndexedRelationTupleIterator<IndexedRelationsIterator>;
pub(crate) type IndexedRelationTupleIteratorMerged =
    KMergeBy<IndexedRelationTupleIterator<IndexedRelationsIterator>, TupleOrderingFn>;

type IndexedRelationValueExtractor = fn(&(IndexedRelationPlayers, u64)) -> VariableValue<'static>;

pub(super) const EXTRACT_PLAYER_START: IndexedRelationValueExtractor =
    |(indexed, _)| VariableValue::Thing(Thing::from(indexed.0));
pub(super) const EXTRACT_PLAYER_END: IndexedRelationValueExtractor =
    |(indexed, _)| VariableValue::Thing(Thing::from(indexed.1));
pub(super) const EXTRACT_RELATION: IndexedRelationValueExtractor = |(indexed, _)| {
    VariableValue::Thing(Thing::Relation(Relation::new(ObjectVertex::build_relation(indexed.2, indexed.3))))
};
pub(super) const EXTRACT_ROLE_START: IndexedRelationValueExtractor =
    |(indexed, _)| VariableValue::Type(Type::RoleType(indexed.4));
pub(super) const EXTRACT_ROLE_END: IndexedRelationValueExtractor =
    |(indexed, _)| VariableValue::Type(Type::RoleType(indexed.5));

pub(super) type IndexedRelationFilterFn = FilterFn<(IndexedRelationPlayers, u64)>;
pub(super) type IndexedRelationFilterMapFn = FilterMapUnchangedFn<(IndexedRelationPlayers, u64)>;
pub(crate) type IndexedRelationOrderingFn = for<'a, 'b> fn(
    (
        &'a Result<(IndexedRelationPlayers, u64), Box<ConceptReadError>>,
        &'b Result<(IndexedRelationPlayers, u64), Box<ConceptReadError>>,
    ),
) -> Ordering;

pub(crate) struct IndexedRelationExecutor {
    pub(crate) player_start: ExecutorVariable,
    pub(crate) player_end: ExecutorVariable,
    pub(crate) relation: ExecutorVariable,
    pub(crate) role_start: ExecutorVariable,
    pub(crate) role_end: ExecutorVariable,

    iterate_mode: IndexedRelationIterateMode,
    variable_modes: VariableModes,

    tuple_positions: TuplePositions,
    variable_component_ordering: [ExecutorVariable; 5],

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
        let variable_component_ordering = [player_start, player_end, relation, role_start, role_end];

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
                for variable in variable_component_ordering {
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
                let instances: Vec<Object> = Itertools::try_collect(thing_manager.get_objects_in(
                    snapshot,
                    type_.as_object_type(),
                    StorageCounters::DISABLED,
                ))?;
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
            variable_component_ordering: variable_component_ordering,

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
        storage_counters: StorageCounters,
    ) -> Result<TupleIterator, Box<ConceptReadError>> {
        let filter = self.filter_fn.clone();
        let check = self.checker.filter_fn_for_row(context, &row, storage_counters.clone());

        let (relation, start_role, end_role) = self.may_get_relation_and_roles(row.as_reference());

        let component_ordering = self.variable_component_ordering;
        let filter_for_row: Arc<IndexedRelationFilterMapFn> = Arc::new(move |item| match filter(&item) {
            Ok(true) => match check(&item) {
                Ok(true) => match verify_relation_and_roles(&item, relation, start_role, end_role) {
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
            IndexedRelationIterateMode::Unbound => {
                // want it sorted by start player, so we must merge an iterator per relation type
                if self.relation_to_player_start_types.len() == 1 {
                    let relation_type = self.relation_to_player_start_types.keys().next().unwrap().as_relation_type();
                    let iterator = thing_manager
                        .get_indexed_relations_in(snapshot, relation_type, self.player_start_range(), storage_counters)
                        .expect("Relation index should be available");
                    let as_tuples = IndexedRelationTupleIterator::new(
                        iterator,
                        filter_for_row,
                        self.tuple_positions.clone(),
                        component_ordering,
                        FixedIndexedRelationBounds::new(Some(relation_type.vertex().type_id_()), None, None, None),
                    );
                    Ok(TupleIterator::IndexedRelationsSingle(SortedTupleIterator::new(
                        as_tuples,
                        self.tuple_positions.clone(),
                        &self.variable_modes,
                    )))
                } else {
                    let tuple_iterators = self
                        .relation_to_player_start_types
                        .keys()
                        .map(|relation_type| {
                            let iterator = thing_manager
                                .get_indexed_relations_in(
                                    snapshot,
                                    relation_type.as_relation_type(),
                                    self.player_start_range(),
                                    storage_counters.clone(),
                                )
                                .expect("Relation index should be available");
                            IndexedRelationTupleIterator::new(
                                iterator,
                                filter_for_row.clone(),
                                self.tuple_positions.clone(),
                                component_ordering,
                                FixedIndexedRelationBounds::new(
                                    Some(relation_type.as_relation_type().vertex().type_id_()),
                                    None,
                                    None,
                                    None,
                                ),
                            )
                        })
                        .collect_vec();
                    let merged_tuples: KMergeBy<
                        IndexedRelationTupleIterator<IndexedRelationsIterator>,
                        TupleOrderingFn,
                    > = KMergeBy::new(tuple_iterators, unsafe_compare_result_tuple);
                    Ok(TupleIterator::IndexedRelationsMerged(SortedTupleIterator::new(
                        merged_tuples,
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
                            let iterator = start_player
                                .get_indexed_relations(
                                    snapshot,
                                    thing_manager,
                                    relation_type.as_relation_type(),
                                    storage_counters.clone(),
                                )
                                .expect("Relation index expected to be available");
                            let as_tuples = IndexedRelationTupleIterator::new(
                                iterator,
                                filter_for_row.clone(),
                                self.tuple_positions.clone(),
                                component_ordering,
                                FixedIndexedRelationBounds::new(
                                    Some(relation_type.as_relation_type().vertex().type_id_()),
                                    None,
                                    Some(*start_player),
                                    None,
                                ),
                            );
                            iterators.push(as_tuples);
                        }
                    },
                );
                let merged_tuples: KMergeBy<IndexedRelationTupleIterator<IndexedRelationsIterator>, TupleOrderingFn> =
                    KMergeBy::new(iterators, unsafe_compare_result_tuple);
                Ok(TupleIterator::IndexedRelationsMerged(SortedTupleIterator::new(
                    merged_tuples,
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
                    let iterator = start_player
                        .get_indexed_relations(snapshot, thing_manager, relation_type, storage_counters)
                        .expect("Relation index should be available");
                    let as_tuples = IndexedRelationTupleIterator::new(
                        iterator,
                        filter_for_row,
                        self.tuple_positions.clone(),
                        component_ordering,
                        FixedIndexedRelationBounds::new(
                            Some(relation_type.vertex().type_id_()),
                            None,
                            Some(start_player),
                            None,
                        ),
                    );
                    Ok(TupleIterator::IndexedRelationsSingle(SortedTupleIterator::new(
                        as_tuples,
                        self.tuple_positions.clone(),
                        &self.variable_modes,
                    )))
                } else {
                    let tuple_iterators = self
                        .relation_to_player_start_types
                        .keys()
                        .map(|relation_type| {
                            let iterator = start_player
                                .get_indexed_relations(
                                    snapshot,
                                    thing_manager,
                                    relation_type.as_relation_type(),
                                    storage_counters.clone(),
                                )
                                .expect("Relation index should be available");
                            let as_tuples = IndexedRelationTupleIterator::new(
                                iterator,
                                filter_for_row.clone(),
                                self.tuple_positions.clone(),
                                component_ordering,
                                FixedIndexedRelationBounds::new(
                                    Some(relation_type.as_relation_type().vertex().type_id_()),
                                    None,
                                    Some(start_player),
                                    None,
                                ),
                            );
                            as_tuples
                        })
                        .collect_vec();
                    let merged_tuples: KMergeBy<
                        IndexedRelationTupleIterator<IndexedRelationsIterator>,
                        TupleOrderingFn,
                    > = KMergeBy::new(tuple_iterators, unsafe_compare_result_tuple);
                    Ok(TupleIterator::IndexedRelationsMerged(SortedTupleIterator::new(
                        merged_tuples,
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
                    let iterator = start_player
                        .get_indexed_relations_with_player(
                            snapshot,
                            thing_manager,
                            end_player,
                            relation_type,
                            storage_counters,
                        )
                        .expect("Relation index should be available");
                    let as_tuples = IndexedRelationTupleIterator::new(
                        iterator,
                        filter_for_row,
                        self.tuple_positions.clone(),
                        component_ordering,
                        FixedIndexedRelationBounds::new(
                            Some(relation_type.vertex().type_id_()),
                            None,
                            Some(start_player),
                            Some(end_player),
                        ),
                    );
                    Ok(TupleIterator::IndexedRelationsSingle(SortedTupleIterator::new(
                        as_tuples,
                        self.tuple_positions.clone(),
                        &self.variable_modes,
                    )))
                } else {
                    let tuple_iterators = self
                        .relation_to_player_start_types
                        .keys()
                        .map(|relation_type| {
                            let iterator = start_player
                                .get_indexed_relations_with_player(
                                    snapshot,
                                    thing_manager,
                                    end_player,
                                    relation_type.as_relation_type(),
                                    storage_counters.clone(),
                                )
                                .expect("Relation index should be available");
                            let as_tuples = IndexedRelationTupleIterator::new(
                                iterator,
                                filter_for_row.clone(),
                                self.tuple_positions.clone(),
                                component_ordering,
                                FixedIndexedRelationBounds::new(
                                    Some(relation_type.as_relation_type().vertex().type_id_()),
                                    None,
                                    Some(start_player),
                                    Some(end_player),
                                ),
                            );
                            as_tuples
                        })
                        .collect_vec();
                    let merged_tuples: KMergeBy<
                        IndexedRelationTupleIterator<IndexedRelationsIterator>,
                        TupleOrderingFn,
                    > = KMergeBy::new(tuple_iterators, unsafe_compare_result_tuple);
                    Ok(TupleIterator::IndexedRelationsMerged(SortedTupleIterator::new(
                        merged_tuples,
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
                let iterator = start_player
                    .get_indexed_relation_roles_with_player_and_relation(
                        snapshot,
                        thing_manager,
                        end_player,
                        relation,
                        storage_counters,
                    )
                    .expect("Relation index should be available");
                let as_tuples = IndexedRelationTupleIterator::new(
                    iterator,
                    filter_for_row,
                    self.tuple_positions.clone(),
                    component_ordering,
                    FixedIndexedRelationBounds::new(
                        Some(relation.type_().vertex().type_id_()),
                        Some(relation.vertex().object_id()),
                        Some(start_player),
                        Some(end_player),
                    ),
                );
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

    fn player_start_range(&self) -> Bounds<ObjectType> {
        debug_assert!(!self.player_start_to_player_end_types.is_empty());
        let (first, _) = self.player_start_to_player_end_types.first_key_value().unwrap();
        let (last, _) = self.player_start_to_player_end_types.last_key_value().unwrap();
        (Bound::Included(first.as_object_type()), Bound::Included(last.as_object_type()))
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

pub(super) struct FixedIndexedRelationBounds {
    relation_type_id: Option<TypeID>,
    relation_id: Option<ObjectID>,
    from: Option<Object>,
    to: Option<Object>,
}

impl FixedIndexedRelationBounds {
    fn new(
        relation_type_id: Option<TypeID>,
        relation_id: Option<ObjectID>,
        from: Option<Object>,
        to: Option<Object>,
    ) -> Self {
        Self { relation_type_id, relation_id, from, to }
    }
}

pub(super) struct IndexedRelationTupleIterator<Iter: LendingIterator> {
    inner: Peekable<Iter>,
    filter_map: Arc<IndexedRelationFilterMapFn>,
    tuple_positions: TuplePositions,
    component_ordering: [ExecutorVariable; 5],
    fixed_bounds: FixedIndexedRelationBounds,
}

impl<Iter> IndexedRelationTupleIterator<Iter>
where
    Iter: for<'a> lending_iterator::Seekable<Result<(IndexedRelationPlayers, u64), Box<ConceptReadError>>>
        + for<'a> LendingIterator<Item<'a> = Result<(IndexedRelationPlayers, u64), Box<ConceptReadError>>>,
{
    pub(super) fn new(
        inner: Iter,
        filter: Arc<IndexedRelationFilterMapFn>,
        tuple_positions: TuplePositions,
        component_ordering: [ExecutorVariable; 5],
        fixed_bindings: FixedIndexedRelationBounds,
    ) -> Self {
        Self {
            inner: Peekable::new(inner),
            filter_map: filter,
            tuple_positions,
            component_ordering,
            fixed_bounds: fixed_bindings,
        }
    }
}

impl<Iter: LendingIterator> IndexedRelationTupleIterator<Iter>
where
    Iter: for<'a> lending_iterator::Seekable<Result<(IndexedRelationPlayers, u64), Box<ConceptReadError>>>
        + for<'a> LendingIterator<Item<'a> = Result<(IndexedRelationPlayers, u64), Box<ConceptReadError>>>,
{
    fn indexed_to_tuple(
        &self,
        indexed_relation_players: Result<(IndexedRelationPlayers, u64), Box<ConceptReadError>>,
    ) -> TupleResult<'static> {
        let (components, _) = indexed_relation_players?;
        let tuple: [VariableValue<'static>; 5] = std::array::from_fn(|i| {
            let variable_at_position =
                self.tuple_positions.as_quintuple()[i].expect("Indexed tuple positions must be a quintuple");
            let source_component_index =
                self.component_ordering.iter().position(|var| *var == variable_at_position).unwrap();
            match source_component_index {
                0 => VariableValue::Thing(components.0.into()),
                1 => VariableValue::Thing(components.1.into()),
                2 => VariableValue::Thing(Thing::Relation(Relation::new(ObjectVertex::build_relation(
                    components.2,
                    components.3,
                )))),
                3 => VariableValue::Type(Type::RoleType(components.4)),
                4 => VariableValue::Type(Type::RoleType(components.5)),
                _ => unreachable!("only 5 components exist"),
            }
        });
        TupleResult::Ok(Tuple::Quintuple(tuple))
    }

    fn tuple_to_indexed(&self, tuple: &Tuple<'_>) -> IndexedRelationPlayers {
        debug_assert!(matches!(tuple, &Tuple::Quintuple(_)));
        let mut indexed: (
            Option<Object>,
            Option<Object>,
            Option<TypeID>,
            Option<ObjectID>,
            Option<RoleType>,
            Option<RoleType>,
        ) = (
            self.fixed_bounds.from.clone(),
            self.fixed_bounds.to.clone(),
            self.fixed_bounds.relation_type_id.clone(),
            self.fixed_bounds.relation_id.clone(),
            None,
            None,
        );
        for (index, value) in tuple.values().iter().enumerate() {
            let variable_at_position =
                self.tuple_positions.as_quintuple()[index].expect("Indexed tuple positions must be a quintuple");
            let source_component_index = self
                .component_ordering
                .iter()
                .position(|var| *var == variable_at_position)
                .expect("Variable -> indexed component not found.");
            match source_component_index {
                0 => {
                    if indexed.0.is_none() {
                        indexed.0 =
                            Some(value.get_thing().map(|thing| thing.as_object()).unwrap_or_else(|| Object::MIN))
                    }
                }
                1 => {
                    if indexed.1.is_none() {
                        indexed.1 =
                            Some(value.get_thing().map(|thing| thing.as_object()).unwrap_or_else(|| Object::MIN))
                    }
                }
                2 => {
                    if indexed.2.is_none() {
                        indexed.2 = Some(
                            value
                                .get_thing()
                                .map(|thing| thing.as_relation().type_().vertex().type_id_())
                                .unwrap_or_else(|| TypeID::MIN),
                        )
                    }
                    if indexed.3.is_none() {
                        indexed.3 = Some(
                            value
                                .get_thing()
                                .map(|thing| thing.as_relation().vertex().object_id())
                                .unwrap_or_else(|| ObjectID::MIN),
                        )
                    }
                }
                3 => {
                    if indexed.4.is_none() {
                        indexed.4 =
                            Some(value.get_type().map(|type_| type_.as_role_type()).unwrap_or_else(|| RoleType::MIN))
                    }
                }
                4 => {
                    if indexed.5.is_none() {
                        indexed.5 =
                            Some(value.get_type().map(|type_| type_.as_role_type()).unwrap_or_else(|| RoleType::MIN))
                    }
                }
                _ => unreachable!("only 5 components exist"),
            }
        }
        (
            indexed.0.unwrap(),
            indexed.1.unwrap(),
            indexed.2.unwrap(),
            indexed.3.unwrap(),
            indexed.4.unwrap(),
            indexed.5.unwrap(),
        )
    }
}

impl<Iter> LendingIterator for IndexedRelationTupleIterator<Iter>
where
    Iter: for<'a> lending_iterator::Seekable<Result<(IndexedRelationPlayers, u64), Box<ConceptReadError>>>
        + for<'a> LendingIterator<Item<'a> = Result<(IndexedRelationPlayers, u64), Box<ConceptReadError>>>,
{
    type Item<'a> = TupleResult<'static>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        // TODO: can this be simplified with something like `.by_ref()` on iterators?
        while let Some(next) = self.inner.next() {
            if let Some(filter_mapped) = (self.filter_map)(next) {
                return Some(self.indexed_to_tuple(filter_mapped));
            }
        }
        None
    }
}

impl<Iter> TupleSeekable for IndexedRelationTupleIterator<Iter>
where
    Iter: for<'a> lending_iterator::Seekable<Result<(IndexedRelationPlayers, u64), Box<ConceptReadError>>>
        + for<'a> LendingIterator<Item<'a> = Result<(IndexedRelationPlayers, u64), Box<ConceptReadError>>>,
{
    fn seek(&mut self, target: &Tuple<'_>) -> Result<(), Box<ConceptReadError>> {
        let target = self.tuple_to_indexed(&target);
        lending_iterator::Seekable::seek(&mut self.inner, &Ok((target, 0)));
        Ok(())
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
            Ok(((player_start, player_end, _relation_type_id, _relation_id, role_start, role_end), _)) => {
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
    (left, right): (
        &Result<(IndexedRelationPlayers, u64), Box<ConceptReadError>>,
        &Result<(IndexedRelationPlayers, u64), Box<ConceptReadError>>,
    ),
) -> Ordering {
    if let (Ok((indexed_players_1, _)), Ok((indexed_players_2, _))) = (left, right) {
        indexed_players_1.cmp(&indexed_players_2)
    } else {
        // arbitrary
        Ordering::Equal
    }
}

fn compare_indexed_players_inverted(
    (left, right): (
        &Result<(IndexedRelationPlayers, u64), Box<ConceptReadError>>,
        &Result<(IndexedRelationPlayers, u64), Box<ConceptReadError>>,
    ),
) -> Ordering {
    if let (
        Ok(((start_1, end_1, rel_type_1, rel_id_1, start_role_1, end_role_1), _)),
        Ok(((start_2, end_2, rel_type_2, rel_id_2, start_role_2, end_role_2), _)),
    ) = (left, right)
    {
        (end_1, start_1, rel_type_1, rel_id_1, start_role_1, end_role_1).cmp(&(
            end_2,
            start_2,
            rel_type_2,
            rel_id_2,
            start_role_2,
            end_role_2,
        ))
    } else {
        // arbitrary
        Ordering::Equal
    }
}

fn verify_relation_and_roles(
    item: &Result<(IndexedRelationPlayers, u64), Box<ConceptReadError>>,
    expected_relation: Option<Relation>,
    expected_start_role: Option<RoleType>,
    expected_end_role: Option<RoleType>,
) -> Result<bool, &ConceptReadError> {
    match item {
        Ok(((_, _, item_relation_type_id, item_relation_id, item_start_role, item_end_role), _)) => {
            let item_relation = Relation::new(ObjectVertex::build_relation(*item_relation_type_id, *item_relation_id));
            Ok(expected_relation.map(|relation| relation == item_relation).unwrap_or(true)
                && expected_start_role.map(|role| role == *item_start_role).unwrap_or(true)
                && expected_end_role.map(|role| role == *item_end_role).unwrap_or(true))
        }
        Err(err) => Err(err),
    }
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
