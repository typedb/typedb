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
use compiler::{executable::match_::instructions::thing::LinksInstruction, ExecutorVariable};
use concept::{
    error::ConceptReadError,
    thing::{
        object::Object,
        relation::{Links, LinksIterator, Relation},
        thing_manager::ThingManager,
    },
    type_::{object_type::ObjectType, relation_type::RelationType, role_type::RoleType},
};
use itertools::Itertools;
use lending_iterator::{kmerge::KMergeBy, LendingIterator, Peekable};
use primitive::Bounds;
use resource::{constants::traversal::CONSTANT_CONCEPT_LIMIT, profile::StorageCounters};
use storage::snapshot::ReadableSnapshot;

use crate::{
    instruction::{
        iterator::{SortedTupleIterator, TupleIterator, TupleSeekable},
        min_max_types,
        tuple::{
            links_to_tuple_player_relation_role, links_to_tuple_relation_player_role,
            links_to_tuple_role_relation_player, tuple_player_relation_role_to_links_canonical,
            tuple_relation_player_role_to_links_canonical, tuple_role_relation_player_to_links_canonical,
            unsafe_compare_result_tuple, LinksToTupleFn, Tuple, TupleOrderingFn, TuplePositions, TupleResult,
            TupleToLinksFn,
        },
        Checker, FilterFn, FilterMapUnchangedFn, LinksIterateMode, VariableModes,
    },
    pipeline::stage::ExecutionContext,
    row::MaybeOwnedRow,
};

pub(crate) struct LinksExecutor {
    links: ir::pattern::constraint::Links<ExecutorVariable>,

    iterate_mode: LinksIterateMode,
    variable_modes: Arc<VariableModes>,

    tuple_positions: TuplePositions,

    relation_player_types: Arc<BTreeMap<Type, Vec<Type>>>, // vecs are in sorted order
    relation_type_range: Bounds<RelationType>,
    player_type_range: Bounds<ObjectType>,

    filter_fn: Arc<LinksFilterFn>,
    relation_cache: Option<Vec<Relation>>,

    checker: Checker<(Links, u64)>,
}

impl fmt::Debug for LinksExecutor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LinksExecutor")
    }
}

pub(crate) type LinksTupleIteratorSingle = LinksTupleIterator<LinksIterator>;
pub(crate) type LinksTupleIteratorMerged = KMergeBy<LinksTupleIterator<LinksIterator>, TupleOrderingFn>;

pub(super) type LinksFilterFn = FilterFn<(Links, u64)>;
pub(super) type LinksFilterMapFn = FilterMapUnchangedFn<(Links, u64)>;

type LinksVariableValueExtractor = fn(&(Links, u64)) -> VariableValue<'static>;
pub(super) const EXTRACT_RELATION: LinksVariableValueExtractor =
    |(links, _)| VariableValue::Thing(Thing::Relation(links.relation()));
pub(super) const EXTRACT_PLAYER: LinksVariableValueExtractor =
    |(links, _)| VariableValue::Thing(Thing::from(links.player()));
pub(super) const EXTRACT_ROLE: LinksVariableValueExtractor =
    |(links, _)| VariableValue::Type(Type::RoleType(links.role_type()));

pub(crate) type LinksOrderingFn = for<'a, 'b> fn(
    (&'a Result<(Links, u64), Box<ConceptReadError>>, &'b Result<(Links, u64), Box<ConceptReadError>>),
) -> Ordering;

impl LinksExecutor {
    pub(crate) fn new(
        links: LinksInstruction<ExecutorVariable>,
        variable_modes: VariableModes,
        sort_by: ExecutorVariable,
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
    ) -> Result<Self, Box<ConceptReadError>> {
        debug_assert!(!variable_modes.all_inputs());
        let relation_player_types = links.relation_to_player_types().clone();
        debug_assert!(!relation_player_types.is_empty());
        let player_types = links.player_types().clone();
        let player_role_types = links.relation_to_role_types().clone();
        let LinksInstruction { links, checks, .. } = links;
        let iterate_mode = LinksIterateMode::new(links.relation(), links.player(), &variable_modes, sort_by);
        let filter_fn = create_links_filter_relations_players_roles(relation_player_types.clone(), player_role_types);

        let relation = links.relation().as_variable().unwrap();
        let player = links.player().as_variable().unwrap();
        let role_type = links.role_type().as_variable().unwrap();

        let output_tuple_positions = match iterate_mode {
            LinksIterateMode::Unbound => TuplePositions::Triple([Some(relation), Some(player), Some(role_type)]),
            LinksIterateMode::UnboundInverted => {
                TuplePositions::Triple([Some(player), Some(relation), Some(role_type)])
            }
            LinksIterateMode::BoundFrom => TuplePositions::Triple([Some(player), Some(relation), Some(role_type)]),
            LinksIterateMode::BoundFromBoundTo => {
                TuplePositions::Triple([Some(role_type), Some(relation), Some(player)])
            }
        };

        let checker = Checker::<(Links, _)>::new(
            checks,
            HashMap::from([(relation, EXTRACT_RELATION), (player, EXTRACT_PLAYER), (role_type, EXTRACT_ROLE)]),
        );

        let relation_type_range = (
            Bound::Included(relation_player_types.first_key_value().unwrap().0.as_relation_type()),
            Bound::Included(relation_player_types.last_key_value().unwrap().0.as_relation_type()),
        );
        let (min_player_type, max_player_type) = min_max_types(player_types.iter());
        let player_type_range =
            (Bound::Included(min_player_type.as_object_type()), Bound::Included(max_player_type.as_object_type()));
        let relation_cache = if iterate_mode == LinksIterateMode::UnboundInverted {
            let mut cache = Vec::new();
            for type_ in relation_player_types.keys() {
                let instances: Vec<Relation> = Itertools::try_collect(thing_manager.get_relations_in(
                    snapshot,
                    type_.as_relation_type(),
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
            links,
            iterate_mode,
            variable_modes: Arc::new(variable_modes),
            tuple_positions: output_tuple_positions,
            relation_player_types,
            relation_type_range,
            player_type_range,
            filter_fn,
            relation_cache,
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
        let check = self.checker.filter_for_row(context, &row, storage_counters.clone());

        let existing_role = may_get_role(self.links.role_type().as_variable().unwrap(), row.as_reference());
        let filter_for_row: Arc<LinksFilterMapFn> = Arc::new(move |item| match filter(&item) {
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
                let iterator = thing_manager.get_links_by_relation_type_range(
                    snapshot,
                    &self.relation_type_range,
                    storage_counters,
                );
                let as_tuples: LinksTupleIteratorSingle = LinksTupleIterator::new(
                    iterator,
                    filter_for_row,
                    links_to_tuple_relation_player_role,
                    tuple_relation_player_role_to_links_canonical,
                    FixedLinksBounds::None,
                );
                Ok(TupleIterator::LinksSingle(SortedTupleIterator::new(
                    as_tuples,
                    self.tuple_positions.clone(),
                    &self.variable_modes,
                )))
            }

            LinksIterateMode::UnboundInverted => {
                debug_assert!(self.relation_cache.is_some());
                if let Some([relation]) = self.relation_cache.as_deref() {
                    // no heap allocs needed if there is only 1 iterator
                    let iterator = thing_manager.get_links_by_relation_and_player_type_range(
                        snapshot,
                        *relation,
                        // TODO: this should be just the types owned by the one instance's type in the cache!
                        &self.player_type_range,
                        storage_counters,
                    );
                    let as_tuples = LinksTupleIterator::new(
                        iterator,
                        filter_for_row,
                        links_to_tuple_player_relation_role,
                        tuple_player_relation_role_to_links_canonical,
                        FixedLinksBounds::Relation(*relation),
                    );
                    Ok(TupleIterator::LinksSingle(SortedTupleIterator::new(
                        as_tuples,
                        self.tuple_positions.clone(),
                        &self.variable_modes,
                    )))
                } else {
                    // TODO: we could create a reusable space for these temporarily held iterators
                    //       so we don't have allocate again before the merging iterator
                    let relations = self.relation_cache.as_ref().unwrap().iter();
                    let tuple_iterators = relations
                        .map(|&relation| {
                            let iterator = thing_manager.get_links_by_relation_and_player_type_range(
                                snapshot,
                                relation,
                                &self.player_type_range,
                                storage_counters.clone(),
                            );
                            LinksTupleIterator::new(
                                iterator,
                                filter_for_row.clone(),
                                links_to_tuple_player_relation_role,
                                tuple_player_relation_role_to_links_canonical,
                                FixedLinksBounds::Relation(relation),
                            )
                        })
                        .collect_vec();

                    // note: this will always have to heap alloc, if we use don't have a re-usable/small-vec'ed priority queue somewhere
                    let merged_tuples: KMergeBy<LinksTupleIterator<LinksIterator>, TupleOrderingFn> =
                        KMergeBy::new(tuple_iterators, unsafe_compare_result_tuple);
                    Ok(TupleIterator::LinksMerged(SortedTupleIterator::new(
                        merged_tuples,
                        self.tuple_positions.clone(),
                        &self.variable_modes,
                    )))
                }
            }

            LinksIterateMode::BoundFrom => {
                let relation = self.links.relation().as_variable().unwrap().as_position().unwrap();
                debug_assert!(row.len() > relation.as_usize());
                let iterator = match row.get(relation) {
                    &VariableValue::Thing(Thing::Relation(relation)) => thing_manager
                        .get_links_by_relation_and_player_type_range(
                            snapshot,
                            relation,
                            &self.player_type_range,
                            storage_counters,
                        ),
                    _ => unreachable!("Links relation must be a relation."),
                };
                let as_tuples = LinksTupleIterator::new(
                    iterator,
                    filter_for_row,
                    links_to_tuple_player_relation_role,
                    tuple_player_relation_role_to_links_canonical,
                    FixedLinksBounds::Relation(row.get(relation).as_thing().as_relation()),
                );
                Ok(TupleIterator::LinksSingle(SortedTupleIterator::new(
                    as_tuples,
                    self.tuple_positions.clone(),
                    &self.variable_modes,
                )))
            }

            LinksIterateMode::BoundFromBoundTo => {
                let relation = self.links.relation().as_variable().unwrap().as_position().unwrap();
                let player = self.links.player().as_variable().unwrap().as_position().unwrap();
                debug_assert!(row.len() > relation.as_usize());
                debug_assert!(row.len() > player.as_usize());
                let relation = row.get(relation).as_thing().as_relation();
                let player = row.get(player).as_thing().as_object();
                let iterator =
                    thing_manager.get_links_by_relation_and_player(snapshot, relation, player, storage_counters);
                let as_tuples = LinksTupleIterator::new(
                    iterator,
                    filter_for_row,
                    links_to_tuple_role_relation_player,
                    tuple_role_relation_player_to_links_canonical,
                    FixedLinksBounds::RelationAndPlayer(relation, player),
                );
                Ok(TupleIterator::LinksSingle(SortedTupleIterator::new(
                    as_tuples,
                    self.tuple_positions.clone(),
                    &self.variable_modes,
                )))
            }
        }
    }
}

impl fmt::Display for LinksExecutor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "[{}], mode={}", &self.links, &self.iterate_mode)
    }
}

pub(super) enum FixedLinksBounds {
    None,
    Relation(Relation),
    Player(Object),
    RelationAndPlayer(Relation, Object),
}

pub(super) struct LinksTupleIterator<Iter: LendingIterator> {
    inner: Peekable<Iter>,
    filter_map: Arc<LinksFilterMapFn>,
    to_tuple_fn: LinksToTupleFn,
    from_tuple_fn: TupleToLinksFn,
    fixed_bounds: FixedLinksBounds,
}

impl<Iter> LinksTupleIterator<Iter>
where
    Iter: for<'a> lending_iterator::Seekable<Result<(Links, u64), Box<ConceptReadError>>>
        + for<'a> LendingIterator<Item<'a> = Result<(Links, u64), Box<ConceptReadError>>>,
{
    pub(super) fn new(
        inner: Iter,
        filter_map: Arc<LinksFilterMapFn>,
        to_tuple_fn: LinksToTupleFn,
        from_tuple_fn: TupleToLinksFn,
        fixed_bounds: FixedLinksBounds,
    ) -> Self {
        Self { inner: Peekable::new(inner), filter_map, to_tuple_fn, from_tuple_fn, fixed_bounds }
    }
}

impl<Iter> LendingIterator for LinksTupleIterator<Iter>
where
    Iter: for<'a> lending_iterator::Seekable<Result<(Links, u64), Box<ConceptReadError>>>
        + for<'a> LendingIterator<Item<'a> = Result<(Links, u64), Box<ConceptReadError>>>,
{
    type Item<'a> = TupleResult<'static>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        // TODO: can this be simplified with something like `.by_ref()` on iterators?
        while let Some(next) = self.inner.next() {
            if let Some(filter_mapped) = (self.filter_map)(next) {
                return Some((self.to_tuple_fn)(filter_mapped));
            }
        }
        None
    }
}

impl<Iter> TupleSeekable for LinksTupleIterator<Iter>
where
    Iter: for<'a> lending_iterator::Seekable<Result<(Links, u64), Box<ConceptReadError>>>
        + for<'a> LendingIterator<Item<'a> = Result<(Links, u64), Box<ConceptReadError>>>,
{
    fn seek(&mut self, target: &Tuple<'_>) -> Result<(), Box<ConceptReadError>> {
        let target_links = (self.from_tuple_fn)(&target, &self.fixed_bounds);
        let target_pair = (target_links, 0);
        lending_iterator::Seekable::seek(&mut self.inner, &Ok(target_pair));
        Ok(())
    }
}

fn create_links_filter_relations_players_roles(
    relation_to_player: Arc<BTreeMap<Type, Vec<Type>>>,
    player_to_role: Arc<BTreeMap<Type, BTreeSet<Type>>>,
) -> Arc<LinksFilterFn> {
    Arc::new(move |result| {
        let links = match result {
            Ok((links, _)) => links,
            Err(err) => return Err(err.clone()),
        };
        let Some(player_types) = relation_to_player.get(&Type::from(links.relation().type_())) else {
            return Ok(false);
        };
        let player_type = Type::from(links.player().type_());
        let role_type = Type::from(links.role_type());
        Ok(player_types.contains(&player_type)
            && player_to_role.get(&player_type).is_some_and(|role_types| role_types.contains(&role_type)))
    })
}

fn compare_by_player_then_relation(
    (left, right): (&Result<(Links, u64), Box<ConceptReadError>>, &Result<(Links, u64), Box<ConceptReadError>>),
) -> Ordering {
    if let (Ok((links_1, _)), Ok((links_2, _))) = (left, right) {
        (links_1.player(), links_1.relation()).cmp(&(links_2.player(), links_2.relation()))
    } else {
        // arbitrary
        Ordering::Equal
    }
}

pub(crate) fn may_get_role(role_var: ExecutorVariable, row: MaybeOwnedRow<'_>) -> Option<RoleType> {
    match role_var {
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
    }
}

pub(crate) fn verify_role(
    item: &Result<(Links, u64), Box<ConceptReadError>>,
    expected_role: Option<RoleType>,
) -> Result<bool, &ConceptReadError> {
    match item {
        Ok((links, _)) => Ok(expected_role.map(|role| role == links.role_type()).unwrap_or(true)),
        Err(err) => Err(err),
    }
}
