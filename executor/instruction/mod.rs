/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, fmt, marker::PhantomData, ops::Bound};

use ::iterator::minmax_or;
use answer::{variable_value::VariableValue, Thing, Type};
use compiler::{
    executable::match_::instructions::{
        CheckInstruction, CheckVertex, ConstraintInstruction, VariableMode, VariableModes,
    },
    ExecutorVariable,
};
use concept::{
    error::ConceptReadError,
    thing::{object::ObjectAPI, thing_manager::ThingManager, ThingAPI},
    type_::{OwnerAPI, PlayerAPI},
};
use encoding::{
    value::{value::Value, ValueEncodable},
    AsBytes,
};
use error::unimplemented_feature;
use ir::{
    pattern::{
        constraint::{Comparator, IsaKind, SubKind},
        Vertex,
    },
    pipeline::ParameterRegistry,
};
use itertools::Itertools;
use resource::profile::StorageCounters;
use storage::snapshot::ReadableSnapshot;
use unicase::UniCase;

use crate::{
    instruction::{
        has_executor::HasExecutor, has_reverse_executor::HasReverseExecutor, iid_executor::IidExecutor,
        indexed_relation_executor::IndexedRelationExecutor, is_executor::IsExecutor, isa_executor::IsaExecutor,
        isa_reverse_executor::IsaReverseExecutor, iterator::TupleIterator, links_executor::LinksExecutor,
        links_reverse_executor::LinksReverseExecutor, owns_executor::OwnsExecutor,
        owns_reverse_executor::OwnsReverseExecutor, plays_executor::PlaysExecutor,
        plays_reverse_executor::PlaysReverseExecutor, relates_executor::RelatesExecutor,
        relates_reverse_executor::RelatesReverseExecutor, sub_executor::SubExecutor,
        sub_reverse_executor::SubReverseExecutor, type_list_executor::TypeListExecutor,
    },
    pipeline::stage::ExecutionContext,
    row::MaybeOwnedRow,
};

mod has_executor;
mod has_reverse_executor;
mod iid_executor;
mod indexed_relation_executor;
mod is_executor;
mod isa_executor;
mod isa_reverse_executor;
pub(crate) mod iterator;
mod links_executor;
mod links_reverse_executor;
mod owns_executor;
mod owns_reverse_executor;
mod plays_executor;
mod plays_reverse_executor;
mod relates_executor;
mod relates_reverse_executor;
mod sub_executor;
mod sub_reverse_executor;
pub(crate) mod tuple;
mod type_list_executor;

pub(crate) const TYPES_EMPTY: Vec<Type> = Vec::new();

#[derive(Debug)]
pub(crate) enum InstructionExecutor {
    Is(IsExecutor),
    Iid(IidExecutor),
    TypeList(TypeListExecutor),

    Sub(SubExecutor),
    SubReverse(SubReverseExecutor),

    Owns(OwnsExecutor),
    OwnsReverse(OwnsReverseExecutor),

    Relates(RelatesExecutor),
    RelatesReverse(RelatesReverseExecutor),

    Plays(PlaysExecutor),
    PlaysReverse(PlaysReverseExecutor),

    Isa(IsaExecutor),
    IsaReverse(IsaReverseExecutor),

    Has(HasExecutor),
    HasReverse(HasReverseExecutor),

    Links(LinksExecutor),
    LinksReverse(LinksReverseExecutor),

    IndexedRelation(IndexedRelationExecutor),
}

impl InstructionExecutor {
    pub(crate) fn new(
        instruction: ConstraintInstruction<ExecutorVariable>,
        variable_modes: VariableModes,
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        sort_by: ExecutorVariable,
    ) -> Result<Self, Box<ConceptReadError>> {
        match instruction {
            ConstraintInstruction::Is(is) => Ok(Self::Is(IsExecutor::new(is, variable_modes, sort_by))),
            ConstraintInstruction::Iid(iid) => Ok(Self::Iid(IidExecutor::new(iid, variable_modes, sort_by))),
            ConstraintInstruction::TypeList(type_) => {
                Ok(Self::TypeList(TypeListExecutor::new(type_, variable_modes, sort_by)))
            }
            ConstraintInstruction::Sub(sub) => Ok(Self::Sub(SubExecutor::new(sub, variable_modes, sort_by))),
            ConstraintInstruction::SubReverse(sub_reverse) => {
                Ok(Self::SubReverse(SubReverseExecutor::new(sub_reverse, variable_modes, sort_by)))
            }
            ConstraintInstruction::Owns(owns) => Ok(Self::Owns(OwnsExecutor::new(owns, variable_modes, sort_by))),
            ConstraintInstruction::OwnsReverse(owns_reverse) => {
                Ok(Self::OwnsReverse(OwnsReverseExecutor::new(owns_reverse, variable_modes, sort_by)))
            }
            ConstraintInstruction::Relates(relates) => {
                Ok(Self::Relates(RelatesExecutor::new(relates, variable_modes, sort_by)))
            }
            ConstraintInstruction::RelatesReverse(relates_reverse) => {
                Ok(Self::RelatesReverse(RelatesReverseExecutor::new(relates_reverse, variable_modes, sort_by)))
            }
            ConstraintInstruction::Plays(plays) => Ok(Self::Plays(PlaysExecutor::new(plays, variable_modes, sort_by))),
            ConstraintInstruction::PlaysReverse(plays_reverse) => {
                Ok(Self::PlaysReverse(PlaysReverseExecutor::new(plays_reverse, variable_modes, sort_by)))
            }
            ConstraintInstruction::Isa(isa) => Ok(Self::Isa(IsaExecutor::new(isa, variable_modes, sort_by))),
            ConstraintInstruction::IsaReverse(isa_reverse) => {
                Ok(Self::IsaReverse(IsaReverseExecutor::new(isa_reverse, variable_modes, sort_by)))
            }
            ConstraintInstruction::Has(has) => {
                Ok(Self::Has(HasExecutor::new(has, variable_modes, sort_by, snapshot, thing_manager)?))
            }
            ConstraintInstruction::HasReverse(has_reverse) => Ok(Self::HasReverse(HasReverseExecutor::new(
                has_reverse,
                variable_modes,
                sort_by,
                snapshot,
                thing_manager,
            )?)),
            ConstraintInstruction::Links(links) => {
                Ok(Self::Links(LinksExecutor::new(links, variable_modes, sort_by, snapshot, thing_manager)?))
            }
            ConstraintInstruction::LinksReverse(links_reverse) => Ok(Self::LinksReverse(LinksReverseExecutor::new(
                links_reverse,
                variable_modes,
                sort_by,
                snapshot,
                thing_manager,
            )?)),
            ConstraintInstruction::IndexedRelation(indexed_relation) => Ok(Self::IndexedRelation(
                IndexedRelationExecutor::new(indexed_relation, variable_modes, sort_by, snapshot, thing_manager)?,
            )),
        }
    }

    pub(crate) fn get_iterator(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: MaybeOwnedRow<'_>,
        storage_counters: StorageCounters,
    ) -> Result<TupleIterator, Box<ConceptReadError>> {
        match self {
            Self::Is(executor) => executor.get_iterator(context, row, storage_counters),
            Self::Iid(executor) => executor.get_iterator(context, row, storage_counters),
            Self::TypeList(executor) => executor.get_iterator(context, row, storage_counters),
            Self::Sub(executor) => executor.get_iterator(context, row, storage_counters),
            Self::SubReverse(executor) => executor.get_iterator(context, row, storage_counters),
            Self::Owns(executor) => executor.get_iterator(context, row, storage_counters),
            Self::OwnsReverse(executor) => executor.get_iterator(context, row, storage_counters),
            Self::Relates(executor) => executor.get_iterator(context, row, storage_counters),
            Self::RelatesReverse(executor) => executor.get_iterator(context, row, storage_counters),
            Self::Plays(executor) => executor.get_iterator(context, row, storage_counters),
            Self::PlaysReverse(executor) => executor.get_iterator(context, row, storage_counters),
            Self::Isa(executor) => executor.get_iterator(context, row, storage_counters),
            Self::IsaReverse(executor) => executor.get_iterator(context, row, storage_counters),
            Self::Has(executor) => executor.get_iterator(context, row, storage_counters),
            Self::HasReverse(executor) => executor.get_iterator(context, row, storage_counters),
            Self::Links(executor) => executor.get_iterator(context, row, storage_counters),
            Self::LinksReverse(executor) => executor.get_iterator(context, row, storage_counters),
            Self::IndexedRelation(executor) => executor.get_iterator(context, row, storage_counters),
        }
    }

    pub(crate) const fn name(&self) -> &'static str {
        match self {
            Self::Is(_) => "is",
            Self::Iid(_) => "iid",
            Self::Isa(_) => "isa",
            Self::IsaReverse(_) => "isa_reverse",
            Self::Has(_) => "has",
            Self::HasReverse(_) => "has_reverse",
            Self::Links(_) => "links",
            Self::LinksReverse(_) => "links_reverse",
            Self::TypeList(_) => "[internal]type_list",
            Self::Sub(_) => "sub",
            Self::SubReverse(_) => "sub_reverse",
            Self::Owns(_) => "owns",
            Self::OwnsReverse(_) => "owns_reverse",
            Self::Relates(_) => "relates",
            Self::RelatesReverse(_) => "relates_reverse",
            Self::Plays(_) => "plays",
            Self::PlaysReverse(_) => "plays_reverse",
            Self::IndexedRelation(_) => "indexed_relation",
        }
    }
}

impl fmt::Display for InstructionExecutor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            InstructionExecutor::Is(inner) => fmt::Display::fmt(inner, f),
            InstructionExecutor::Iid(inner) => fmt::Display::fmt(inner, f),
            InstructionExecutor::TypeList(inner) => fmt::Display::fmt(inner, f),
            InstructionExecutor::Sub(inner) => fmt::Display::fmt(inner, f),
            InstructionExecutor::SubReverse(inner) => fmt::Display::fmt(inner, f),
            InstructionExecutor::Owns(inner) => fmt::Display::fmt(inner, f),
            InstructionExecutor::OwnsReverse(inner) => fmt::Display::fmt(inner, f),
            InstructionExecutor::Relates(inner) => fmt::Display::fmt(inner, f),
            InstructionExecutor::RelatesReverse(inner) => fmt::Display::fmt(inner, f),
            InstructionExecutor::Plays(inner) => fmt::Display::fmt(inner, f),
            InstructionExecutor::PlaysReverse(inner) => fmt::Display::fmt(inner, f),
            InstructionExecutor::Isa(inner) => fmt::Display::fmt(inner, f),
            InstructionExecutor::IsaReverse(inner) => fmt::Display::fmt(inner, f),
            InstructionExecutor::Has(inner) => fmt::Display::fmt(inner, f),
            InstructionExecutor::HasReverse(inner) => fmt::Display::fmt(inner, f),
            InstructionExecutor::Links(inner) => fmt::Display::fmt(inner, f),
            InstructionExecutor::LinksReverse(inner) => fmt::Display::fmt(inner, f),
            InstructionExecutor::IndexedRelation(inner) => fmt::Display::fmt(inner, f),
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(crate) enum BinaryIterateMode {
    // [x, y] in standard order, sorted by x, then y
    Unbound,
    // [x, y] in [y, x] sort order
    UnboundInverted,
    // [X, y], where X is bound
    BoundFrom,
}

impl BinaryIterateMode {
    pub(crate) fn new(
        from_vertex: &Vertex<ExecutorVariable>,
        to_vertex: &Vertex<ExecutorVariable>,
        var_modes: &VariableModes,
        sort_by: ExecutorVariable,
    ) -> BinaryIterateMode {
        // TODO
        // debug_assert!(var_modes.len() == 2);
        debug_assert!(!var_modes.all_inputs());

        let is_from_bound = match from_vertex {
            &Vertex::Variable(pos) => var_modes.get(pos) == Some(VariableMode::Input),
            Vertex::Label(_) | Vertex::Parameter(_) => true,
        };

        // TODO
        // debug_assert!(var_modes.get(to_var) != Some(&VariableMode::Input));

        if is_from_bound {
            Self::BoundFrom
        } else if Some(sort_by) == to_vertex.as_variable() {
            Self::UnboundInverted
        } else {
            Self::Unbound
        }
    }

    pub(crate) fn is_unbound_inverted(&self) -> bool {
        self == &Self::UnboundInverted
    }
}

impl fmt::Display for BinaryIterateMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(crate) enum LinksIterateMode {
    // [x, y, z] = standard sort order
    Unbound,
    // [y, x, z] sort order
    UnboundInverted,
    // [X, y, z] sort order
    BoundFrom,
    // [X, Y, z]
    BoundFromBoundTo,
}

impl LinksIterateMode {
    pub(crate) fn new(
        from_vertex: &Vertex<ExecutorVariable>,
        to_vertex: &Vertex<ExecutorVariable>,
        var_modes: &VariableModes,
        sort_by: ExecutorVariable,
    ) -> LinksIterateMode {
        debug_assert!(var_modes.len() == 3 || from_vertex == to_vertex);
        debug_assert!(!var_modes.all_inputs());

        let is_from_bound = match from_vertex {
            &Vertex::Variable(from_var) => var_modes.get(from_var) == Some(VariableMode::Input),
            Vertex::Label(_) | Vertex::Parameter(_) => true,
        };

        let is_to_bound = match to_vertex {
            &Vertex::Variable(to_var) => var_modes.get(to_var) == Some(VariableMode::Input),
            Vertex::Label(_) | Vertex::Parameter(_) => true,
        };

        if is_to_bound {
            assert!(is_from_bound);
            Self::BoundFromBoundTo
        } else if is_from_bound {
            Self::BoundFrom
        } else if Some(sort_by) == to_vertex.as_variable() {
            Self::UnboundInverted
        } else {
            Self::Unbound
        }
    }
}

impl fmt::Display for LinksIterateMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

fn type_from_row_or_annotations<'a>(
    vertex: &Vertex<ExecutorVariable>,
    row: MaybeOwnedRow<'_>,
    annos: impl Iterator<Item = &'a Type> + fmt::Debug,
) -> Type {
    match vertex {
        &Vertex::Variable(ExecutorVariable::RowPosition(var)) => {
            debug_assert!(row.len() > var.as_usize());
            match row.get(var).to_owned() {
                VariableValue::Type(type_) => type_,
                other => unreachable!("Supertype must be a type, found: {other}"),
            }
        }
        &Vertex::Variable(ExecutorVariable::Internal(_)) => unreachable!("an internal variable cannot be an input"),
        Vertex::Label(_) => annos.cloned().exactly_one().expect("multiple types for fixed label?"),
        Vertex::Parameter(_) => unreachable!(),
    }
}

pub(super) type FilterMapUnchangedFn<T> =
    dyn Fn(Result<T, Box<ConceptReadError>>) -> Option<Result<T, Box<ConceptReadError>>>;
pub(super) type FilterMapFn<T, U> =
    dyn Fn(Result<T, Box<ConceptReadError>>) -> Option<Result<U, Box<ConceptReadError>>>;
type FilterFn<T> = dyn Fn(&Result<T, Box<ConceptReadError>>) -> Result<bool, Box<ConceptReadError>>;

#[derive(Debug)]
pub(crate) struct Checker<T: 'static> {
    extractors: HashMap<ExecutorVariable, fn(&T) -> VariableValue<'_>>,
    pub checks: Vec<CheckInstruction<ExecutorVariable>>,
    _phantom_data: PhantomData<T>,
}

type BoxExtractor<T> = Box<dyn for<'a> Fn(&'a T) -> VariableValue<'a>>;

macro_rules! unwrap_or_bail {
    ($value:expr => $variant:ident) => {{
        let VariableValue::$variant(x) = $value else { return Ok(false) };
        x
    }};
}

impl<T> Checker<T> {
    pub(crate) fn new(
        checks: Vec<CheckInstruction<ExecutorVariable>>,
        extractors: HashMap<ExecutorVariable, fn(&T) -> VariableValue<'_>>,
    ) -> Self {
        Self { extractors, checks, _phantom_data: PhantomData }
    }

    pub(crate) fn value_range_for(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: Option<MaybeOwnedRow<'_>>,
        target_variable: ExecutorVariable,
        storage_counters: StorageCounters,
    ) -> Result<(Bound<Value<'_>>, Bound<Value<'_>>), Box<ConceptReadError>> {
        fn intersect<'a>(
            (a_min, a_max): (Bound<Value<'a>>, Bound<Value<'a>>),
            (b_min, b_max): (Bound<Value<'a>>, Bound<Value<'a>>),
        ) -> (Bound<Value<'a>>, Bound<Value<'a>>) {
            let select_a_min = match (&a_min, &b_min) {
                (_, Bound::Unbounded) => true,
                (Bound::Excluded(a), Bound::Included(b)) => a >= b,
                (Bound::Excluded(a), Bound::Excluded(b)) => a >= b,
                (Bound::Included(a), Bound::Included(b)) => a >= b,
                (Bound::Included(a), Bound::Excluded(b)) => a > b,
                _ => false,
            };
            let select_a_max = match (&a_max, &b_max) {
                (_, Bound::Unbounded) => true,
                (Bound::Excluded(a), Bound::Included(b)) => a <= b,
                (Bound::Excluded(a), Bound::Excluded(b)) => a <= b,
                (Bound::Included(a), Bound::Included(b)) => a <= b,
                (Bound::Included(a), Bound::Excluded(b)) => a < b,
                _ => false,
            };
            (if select_a_min { a_min } else { b_min }, if select_a_max { a_max } else { b_max })
        }

        let mut range = (Bound::Unbounded, Bound::Unbounded);
        for i in 0..self.checks.len() {
            let check = &self.checks[i];
            match check {
                CheckInstruction::Comparison { lhs, rhs, comparator } => {
                    if lhs.as_variable() == Some(target_variable) {
                        let rhs_variable_value = get_vertex_value(rhs, row.as_ref(), &context.parameters);
                        let rhs_value = Self::read_value(
                            context.snapshot.as_ref(),
                            &context.thing_manager,
                            &rhs_variable_value,
                            storage_counters.clone(),
                        )?;
                        if let Some(rhs_value) = rhs_value {
                            let comp_range = match comparator {
                                Comparator::Equal => (Bound::Included(rhs_value.clone()), Bound::Included(rhs_value)),
                                Comparator::Less => (Bound::Unbounded, Bound::Excluded(rhs_value)),
                                Comparator::LessOrEqual => (Bound::Unbounded, Bound::Included(rhs_value)),
                                Comparator::Greater => (Bound::Excluded(rhs_value), Bound::Unbounded),
                                Comparator::GreaterOrEqual => (Bound::Included(rhs_value), Bound::Unbounded),
                                Comparator::Like => continue,
                                Comparator::Contains => continue,
                                Comparator::NotEqual => continue,
                            };
                            range = intersect(range, comp_range);
                        }
                    } else {
                        debug_assert!(
                            rhs.as_variable().expect("RHS of comparison must be a variable") == target_variable
                        );
                        let lhs_variable_value = get_vertex_value(lhs, row.as_ref(), &context.parameters);
                        let lhs_value = Self::read_value(
                            context.snapshot.as_ref(),
                            &context.thing_manager,
                            &lhs_variable_value,
                            storage_counters.clone(),
                        )?;
                        if let Some(lhs_value) = lhs_value {
                            let comp_range = match comparator {
                                Comparator::Equal => (Bound::Included(lhs_value.clone()), Bound::Included(lhs_value)),
                                Comparator::Less => (Bound::Excluded(lhs_value), Bound::Unbounded),
                                Comparator::LessOrEqual => (Bound::Included(lhs_value), Bound::Unbounded),
                                Comparator::Greater => (Bound::Unbounded, Bound::Excluded(lhs_value)),
                                Comparator::GreaterOrEqual => (Bound::Unbounded, Bound::Included(lhs_value)),
                                Comparator::Like => continue,
                                Comparator::Contains => continue,
                                Comparator::NotEqual => continue,
                            };
                            range = intersect(range, comp_range);
                        }
                    }
                }
                CheckInstruction::Is { lhs, rhs } => {
                    if *lhs == target_variable {
                        let rhs_as_vertex = CheckVertex::Variable(*rhs);
                        let rhs_variable_value = get_vertex_value(&rhs_as_vertex, row.as_ref(), &context.parameters);
                        let rhs_value = Self::read_value(
                            context.snapshot.as_ref(),
                            &context.thing_manager,
                            &rhs_variable_value,
                            storage_counters.clone(),
                        )?;
                        if let Some(rhs_value) = rhs_value {
                            let comp_range = (Bound::Included(rhs_value.clone()), Bound::Included(rhs_value));
                            range = intersect(range, comp_range);
                        }
                    } else {
                        let lhs_as_vertex = CheckVertex::Variable(*lhs);
                        let lhs_variable_value = get_vertex_value(&lhs_as_vertex, row.as_ref(), &context.parameters);
                        let lhs_value = Self::read_value(
                            context.snapshot.as_ref(),
                            &context.thing_manager,
                            &lhs_variable_value,
                            storage_counters.clone(),
                        )?;
                        if let Some(lhs_value) = lhs_value {
                            let comp_range = (Bound::Included(lhs_value.clone()), Bound::Included(lhs_value));
                            range = intersect(range, comp_range);
                        }
                    }
                }
                _ => (),
            }
        }
        let range = (range.0.map(|value| value.into_owned()), range.1.map(|value| value.into_owned()));
        Ok(range)
    }

    fn read_value<'a>(
        snapshot: &'a impl ReadableSnapshot,
        thing_manager: &'a ThingManager,
        variable_value: &'a VariableValue<'_>,
        storage_counters: StorageCounters,
    ) -> Result<Option<Value<'static>>, Box<ConceptReadError>> {
        // TODO: is there a way to do this without cloning the value?
        match variable_value {
            VariableValue::Thing(Thing::Attribute(attribute)) => {
                let value = attribute.get_value(snapshot, thing_manager, storage_counters)?;
                Ok(Some(value.into_owned()))
            }
            VariableValue::Value(value) => {
                let value = value.as_reference();
                Ok(Some(value.into_owned()))
            }
            _ => Ok(None),
        }
    }

    pub(crate) fn filter_fn_for_row(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: &MaybeOwnedRow<'_>,
        storage_counters: StorageCounters,
    ) -> Box<FilterFn<T>> {
        let mut filters: Vec<Box<dyn Fn(&T) -> Result<bool, Box<ConceptReadError>>>> =
            Vec::with_capacity(self.checks.len());

        for check in &self.checks {
            let filter = match check {
                &CheckInstruction::Iid { var, iid } => self.filter_iid(context, row, var, iid),
                &CheckInstruction::TypeList { type_var, ref types } => {
                    self.filter_type_list(context, row, type_var, types)
                }
                &CheckInstruction::ThingTypeList { thing_var, ref types } => {
                    self.filter_thing_type_list(context, row, thing_var, types)
                }
                &CheckInstruction::Sub { sub_kind, ref subtype, ref supertype } => {
                    self.filter_sub(context, row, sub_kind, subtype, supertype)
                }
                CheckInstruction::Owns { owner, attribute } => self.filter_owns(context, row, owner, attribute),
                CheckInstruction::Relates { relation, role_type } => {
                    self.filter_relates(context, row, relation, role_type)
                }
                CheckInstruction::Plays { player, role_type } => self.filter_plays(context, row, player, role_type),
                &CheckInstruction::Isa { isa_kind, ref type_, ref thing } => {
                    self.filter_isa(context, row, isa_kind, type_, thing)
                }
                CheckInstruction::Has { owner, attribute } => {
                    self.filter_has(context, row, owner, attribute, storage_counters.clone())
                }
                CheckInstruction::Links { relation, player, role } => {
                    self.filter_links(context, row, relation, player, role, storage_counters.clone())
                }
                CheckInstruction::IndexedRelation { start_player, end_player, relation, start_role, end_role } => self
                    .filter_indexed_relation(
                        context,
                        row,
                        start_player,
                        end_player,
                        relation,
                        start_role,
                        end_role,
                        storage_counters.clone(),
                    ),
                &CheckInstruction::LinksDeduplication { role1, player1, role2, player2 } => {
                    self.filter_links_dedup(row, role1, player1, role2, player2)
                }
                &CheckInstruction::Is { lhs, rhs } => self.filter_is(row, lhs, rhs),
                CheckInstruction::Comparison { lhs, rhs, comparator } => {
                    self.filter_comparison(context, row, lhs, rhs, comparator, storage_counters.clone())
                }
                CheckInstruction::Unsatisfiable => Box::new(|_: &T| Ok(false)),
            };
            filters.push(filter);
        }

        Box::new(move |res| {
            let Ok(value) = res else { return Ok(true) };
            for filter in &filters {
                if !filter(value)? {
                    return Ok(false);
                }
            }
            Ok(true)
        })
    }

    fn filter_iid(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: &MaybeOwnedRow<'_>,
        var: ExecutorVariable,
        iid: ir::pattern::ParameterID,
    ) -> Box<dyn Fn(&T) -> Result<bool, Box<ConceptReadError>>> {
        let maybe_var_extractor = self.extractors.get(&var);
        let var: BoxExtractor<T> = match maybe_var_extractor {
            Some(&subtype) => Box::new(subtype),
            None => make_const_extractor(&CheckVertex::Variable(var), row, context),
        };
        let iid = context.parameters().iid(iid).unwrap().clone();
        Box::new(move |value: &T| {
            let value = var(value);
            match value {
                VariableValue::Thing(thing) => match thing {
                    Thing::Entity(entity) => Ok(*iid == *entity.vertex().to_bytes()),
                    Thing::Relation(relation) => Ok(*iid == *relation.vertex().to_bytes()),
                    Thing::Attribute(attribute) => Ok(*iid == *attribute.vertex().to_bytes()),
                },
                VariableValue::None => Ok(false),
                VariableValue::Type(_) => Ok(false),
                VariableValue::Value(_) => Ok(false), // or unreachable?
                VariableValue::ThingList(_) | VariableValue::ValueList(_) => unimplemented_feature!(Lists),
            }
        })
    }

    fn filter_type_list(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: &MaybeOwnedRow<'_>,
        type_var: ExecutorVariable,
        types: &std::sync::Arc<std::collections::BTreeSet<Type>>,
    ) -> Box<dyn Fn(&T) -> Result<bool, Box<ConceptReadError>>> {
        let maybe_type_extractor = self.extractors.get(&type_var);
        let type_: BoxExtractor<T> = match maybe_type_extractor {
            Some(&subtype) => Box::new(subtype),
            None => make_const_extractor(&CheckVertex::Variable(type_var), row, context),
        };
        let types = types.clone();
        Box::new(move |value: &T| Ok(types.contains(&unwrap_or_bail!(type_(value) => Type))))
    }

    fn filter_thing_type_list(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: &MaybeOwnedRow<'_>,
        thing_var: ExecutorVariable,
        types: &std::sync::Arc<std::collections::BTreeSet<Type>>,
    ) -> Box<dyn Fn(&T) -> Result<bool, Box<ConceptReadError>>> {
        let maybe_type_extractor = self.extractors.get(&thing_var);
        let thing: BoxExtractor<T> = match maybe_type_extractor {
            Some(&subtype) => Box::new(subtype),
            None => make_const_extractor(&CheckVertex::Variable(thing_var), row, context),
        };
        let types = types.clone();
        Box::new(move |value: &T| Ok(types.contains(&unwrap_or_bail!(thing(value) => Thing).type_())))
    }

    fn filter_sub(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: &MaybeOwnedRow<'_>,
        sub_kind: SubKind,
        subtype: &CheckVertex<ExecutorVariable>,
        supertype: &CheckVertex<ExecutorVariable>,
    ) -> Box<dyn Fn(&T) -> Result<bool, Box<ConceptReadError>>> {
        let maybe_subtype_extractor = subtype.as_variable().and_then(|var| self.extractors.get(&var));
        let maybe_supertype_extractor = supertype.as_variable().and_then(|var| self.extractors.get(&var));
        let snapshot = context.snapshot.clone();
        let thing_manager = context.thing_manager.clone();
        let subtype: BoxExtractor<T> = match maybe_subtype_extractor {
            Some(&subtype) => Box::new(subtype),
            None => make_const_extractor(subtype, row, context),
        };
        let supertype: BoxExtractor<T> = match maybe_supertype_extractor {
            Some(&supertype) => Box::new(supertype),
            None => make_const_extractor(supertype, row, context),
        };
        Box::new(move |value: &T| {
            let subtype = unwrap_or_bail!(subtype(value) => Type);
            let supertype = unwrap_or_bail!(supertype(value) => Type);
            match sub_kind {
                SubKind::Subtype => {
                    subtype.is_transitive_subtype_of(supertype, &*snapshot, thing_manager.type_manager())
                }
                SubKind::Exact => subtype.is_direct_subtype_of(supertype, &*snapshot, thing_manager.type_manager()),
            }
        })
    }

    fn filter_owns(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: &MaybeOwnedRow<'_>,
        owner: &CheckVertex<ExecutorVariable>,
        attribute: &CheckVertex<ExecutorVariable>,
    ) -> Box<dyn Fn(&T) -> Result<bool, Box<ConceptReadError>>> {
        let maybe_owner_extractor = owner.as_variable().and_then(|var| self.extractors.get(&var));
        let maybe_attribute_extractor = attribute.as_variable().and_then(|var| self.extractors.get(&var));
        let snapshot = context.snapshot.clone();
        let thing_manager = context.thing_manager.clone();
        let owner: BoxExtractor<T> = match maybe_owner_extractor {
            Some(&owner) => Box::new(owner),
            None => make_const_extractor(owner, row, context),
        };
        let attribute: BoxExtractor<T> = match maybe_attribute_extractor {
            Some(&attribute) => Box::new(attribute),
            None => make_const_extractor(attribute, row, context),
        };
        Box::new(move |value: &T| {
            let owner = unwrap_or_bail!(owner(value) => Type).as_object_type();
            let attribute = unwrap_or_bail!(attribute(value) => Type).as_attribute_type();
            owner.get_owns_attribute(&*snapshot, thing_manager.type_manager(), attribute).map(|owns| owns.is_some())
        })
    }

    fn filter_relates(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: &MaybeOwnedRow<'_>,
        relation: &CheckVertex<ExecutorVariable>,
        role_type: &CheckVertex<ExecutorVariable>,
    ) -> Box<dyn Fn(&T) -> Result<bool, Box<ConceptReadError>>> {
        let maybe_relation_extractor = relation.as_variable().and_then(|var| self.extractors.get(&var));
        let maybe_role_type_extractor = role_type.as_variable().and_then(|var| self.extractors.get(&var));
        let snapshot = context.snapshot.clone();
        let thing_manager = context.thing_manager.clone();
        let relation: BoxExtractor<T> = match maybe_relation_extractor {
            Some(&relation) => Box::new(relation),
            None => make_const_extractor(relation, row, context),
        };
        let role_type: BoxExtractor<T> = match maybe_role_type_extractor {
            Some(&role_type) => Box::new(role_type),
            None => make_const_extractor(role_type, row, context),
        };
        Box::new(move |value: &T| {
            let relation_type = unwrap_or_bail!(relation(value) => Type).as_relation_type();
            let role_type = unwrap_or_bail!(role_type(value) => Type).as_role_type();
            relation_type
                .get_relates_role(&*snapshot, thing_manager.type_manager(), role_type)
                .map(|relates| relates.is_some())
        })
    }

    fn filter_plays(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: &MaybeOwnedRow<'_>,
        player: &CheckVertex<ExecutorVariable>,
        role_type: &CheckVertex<ExecutorVariable>,
    ) -> Box<dyn Fn(&T) -> Result<bool, Box<ConceptReadError>>> {
        let maybe_player_extractor = player.as_variable().and_then(|var| self.extractors.get(&var));
        let maybe_role_type_extractor = role_type.as_variable().and_then(|var| self.extractors.get(&var));
        let snapshot = context.snapshot.clone();
        let thing_manager = context.thing_manager.clone();
        let player: BoxExtractor<T> = match maybe_player_extractor {
            Some(&player) => Box::new(player),
            None => make_const_extractor(player, row, context),
        };
        let role_type: BoxExtractor<T> = match maybe_role_type_extractor {
            Some(&role_type) => Box::new(role_type),
            None => make_const_extractor(role_type, row, context),
        };
        Box::new({
            move |value: &T| {
                let object_type = unwrap_or_bail!(player(value) => Type).as_object_type();
                let role_type = unwrap_or_bail!(role_type(value) => Type).as_role_type();
                object_type
                    .get_plays_role(&*snapshot, thing_manager.type_manager(), role_type)
                    .map(|plays| plays.is_some())
            }
        })
    }

    fn filter_isa(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: &MaybeOwnedRow<'_>,
        isa_kind: IsaKind,
        type_: &CheckVertex<ExecutorVariable>,
        thing: &CheckVertex<ExecutorVariable>,
    ) -> Box<dyn Fn(&T) -> Result<bool, Box<ConceptReadError>>> {
        let maybe_thing_extractor = thing.as_variable().and_then(|var| self.extractors.get(&var));
        let maybe_type_extractor = type_.as_variable().and_then(|var| self.extractors.get(&var));
        let snapshot = context.snapshot.clone();
        let thing_manager = context.thing_manager.clone();
        let thing: BoxExtractor<T> = match maybe_thing_extractor {
            Some(&thing) => Box::new(thing),
            None => make_const_extractor(thing, row, context),
        };
        let type_: BoxExtractor<T> = match maybe_type_extractor {
            Some(&type_) => Box::new(type_),
            None => make_const_extractor(type_, row, context),
        };
        Box::new({
            move |value: &T| {
                let actual = unwrap_or_bail!(thing(value) => Thing).type_();
                let expected = unwrap_or_bail!(type_(value) => Type);
                if isa_kind == IsaKind::Exact {
                    Ok(actual == expected)
                } else {
                    actual.is_transitive_subtype_of(expected, &*snapshot, thing_manager.type_manager())
                }
            }
        })
    }

    fn filter_has(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: &MaybeOwnedRow<'_>,
        owner: &CheckVertex<ExecutorVariable>,
        attribute: &CheckVertex<ExecutorVariable>,
        storage_counters: StorageCounters,
    ) -> Box<dyn Fn(&T) -> Result<bool, Box<ConceptReadError>>> {
        let maybe_owner_extractor = owner.as_variable().and_then(|var| self.extractors.get(&var));
        let maybe_attribute_extractor = attribute.as_variable().and_then(|var| self.extractors.get(&var));
        let snapshot = context.snapshot.clone();
        let thing_manager = context.thing_manager.clone();
        let owner: BoxExtractor<T> = match maybe_owner_extractor {
            Some(&owner) => Box::new(owner),
            None => make_const_extractor(owner, row, context),
        };
        let attribute: BoxExtractor<T> = match maybe_attribute_extractor {
            Some(&attribute) => Box::new(attribute),
            None => make_const_extractor(attribute, row, context),
        };
        Box::new({
            move |value: &T| {
                let owner = unwrap_or_bail!(owner(value) => Thing).as_object();
                let attribute = attribute(value);
                let attribute = unwrap_or_bail!(&attribute => Thing).as_attribute();
                owner.has_attribute(&*snapshot, &thing_manager, attribute, storage_counters.clone())
            }
        })
    }

    fn filter_links(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: &MaybeOwnedRow<'_>,
        relation: &CheckVertex<ExecutorVariable>,
        player: &CheckVertex<ExecutorVariable>,
        role: &CheckVertex<ExecutorVariable>,
        storage_counters: StorageCounters,
    ) -> Box<dyn Fn(&T) -> Result<bool, Box<ConceptReadError>>> {
        let maybe_relation_extractor = relation.as_variable().and_then(|var| self.extractors.get(&var));
        let maybe_player_extractor = player.as_variable().and_then(|var| self.extractors.get(&var));
        let maybe_role_extractor = role.as_variable().and_then(|var| self.extractors.get(&var));
        let snapshot = context.snapshot.clone();
        let thing_manager = context.thing_manager.clone();
        let relation: BoxExtractor<T> = match maybe_relation_extractor {
            Some(&relation) => Box::new(relation),
            None => make_const_extractor(relation, row, context),
        };
        let player: BoxExtractor<T> = match maybe_player_extractor {
            Some(&player) => Box::new(player),
            None => make_const_extractor(player, row, context),
        };
        let role: BoxExtractor<T> = match maybe_role_extractor {
            Some(&role) => Box::new(role),
            None => make_const_extractor(role, row, context),
        };
        Box::new({
            move |value: &T| {
                let relation = unwrap_or_bail!(relation(value) => Thing).as_relation();
                let player = unwrap_or_bail!(player(value) => Thing).as_object();
                let role = unwrap_or_bail!(role(value) => Type).as_role_type();
                relation.has_role_player(&*snapshot, &thing_manager, player, role, storage_counters.clone())
            }
        })
    }

    fn filter_indexed_relation(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: &MaybeOwnedRow<'_>,
        start_player: &CheckVertex<ExecutorVariable>,
        end_player: &CheckVertex<ExecutorVariable>,
        relation: &CheckVertex<ExecutorVariable>,
        start_role: &CheckVertex<ExecutorVariable>,
        end_role: &CheckVertex<ExecutorVariable>,
        storage_counters: StorageCounters,
    ) -> Box<dyn Fn(&T) -> Result<bool, Box<ConceptReadError>>> {
        let maybe_start_player_extractor = start_player.as_variable().and_then(|var| self.extractors.get(&var));
        let maybe_end_player_extractor = end_player.as_variable().and_then(|var| self.extractors.get(&var));
        let maybe_relation_extractor = relation.as_variable().and_then(|var| self.extractors.get(&var));
        let maybe_start_role_extractor = start_role.as_variable().and_then(|var| self.extractors.get(&var));
        let maybe_end_role_extractor = end_role.as_variable().and_then(|var| self.extractors.get(&var));
        let snapshot = context.snapshot.clone();
        let thing_manager = context.thing_manager.clone();
        let start_player_extractor: BoxExtractor<T> = match maybe_start_player_extractor {
            Some(&player) => Box::new(player),
            None => make_const_extractor(start_player, row, context),
        };
        let end_player_extractor: BoxExtractor<T> = match maybe_end_player_extractor {
            Some(&player) => Box::new(player),
            None => make_const_extractor(end_player, row, context),
        };
        let relation_extractor: BoxExtractor<T> = match maybe_relation_extractor {
            Some(&relation) => Box::new(relation),
            None => make_const_extractor(relation, row, context),
        };
        let start_role_extractor: BoxExtractor<T> = match maybe_start_role_extractor {
            Some(&role) => Box::new(role),
            None => make_const_extractor(start_role, row, context),
        };
        let end_role_extractor: BoxExtractor<T> = match maybe_end_role_extractor {
            Some(&role) => Box::new(role),
            None => make_const_extractor(end_role, row, context),
        };
        Box::new({
            move |value: &T| {
                let object = unwrap_or_bail!(start_player_extractor(value) => Thing).as_object();
                let end_player = unwrap_or_bail!(end_player_extractor(value) => Thing).as_object();
                let relation = unwrap_or_bail!(relation_extractor(value) => Thing).as_relation();
                let start_role = unwrap_or_bail!(start_role_extractor(value) => Type).as_role_type();
                let end_role = unwrap_or_bail!(end_role_extractor(value) => Type).as_role_type();
                object.has_indexed_relation_player(
                    &*snapshot,
                    &thing_manager,
                    end_player,
                    relation,
                    start_role,
                    end_role,
                    storage_counters.clone(),
                )
            }
        })
    }

    fn filter_is(
        &self,
        row: &MaybeOwnedRow<'_>,
        lhs: ExecutorVariable,
        rhs: ExecutorVariable,
    ) -> Box<dyn Fn(&T) -> Result<bool, Box<ConceptReadError>>> {
        let maybe_lhs_extractor = self.extractors.get(&lhs);
        let lhs: BoxExtractor<T> = match maybe_lhs_extractor {
            Some(&lhs) => Box::new(lhs),
            None => {
                let ExecutorVariable::RowPosition(pos) = lhs else { unreachable!() };
                let value = row.get(pos).as_reference().into_owned();
                Box::new(move |_| value.clone())
            }
        };
        let maybe_rhs_extractor = self.extractors.get(&rhs);
        let rhs: BoxExtractor<T> = match maybe_rhs_extractor {
            Some(&rhs) => Box::new(rhs),
            None => {
                let ExecutorVariable::RowPosition(pos) = rhs else { unreachable!() };
                let value = row.get(pos).as_reference().into_owned();
                Box::new(move |_| value.clone())
            }
        };
        // NOTE: Empty is Empty matches
        Box::new(move |value: &T| Ok(lhs(value) == rhs(value)))
    }

    fn filter_links_dedup(
        &self,
        row: &MaybeOwnedRow<'_>,
        role1: ExecutorVariable,
        player1: ExecutorVariable,
        role2: ExecutorVariable,
        player2: ExecutorVariable,
    ) -> Box<dyn Fn(&T) -> Result<bool, Box<ConceptReadError>>> {
        let maybe_role1_extractor = self.extractors.get(&role1);
        let role1: BoxExtractor<T> = match maybe_role1_extractor {
            Some(&role1) => Box::new(role1),
            None => {
                let ExecutorVariable::RowPosition(pos) = role1 else { unreachable!() };
                let value = row.get(pos).as_reference().into_owned();
                Box::new(move |_| value.clone())
            }
        };
        let maybe_player1_extractor = self.extractors.get(&player1);
        let player1: BoxExtractor<T> = match maybe_player1_extractor {
            Some(&player1) => Box::new(player1),
            None => {
                let ExecutorVariable::RowPosition(pos) = player1 else { unreachable!() };
                let value = row.get(pos).as_reference().into_owned();
                Box::new(move |_| value.clone())
            }
        };
        let maybe_role2_extractor = self.extractors.get(&role2);
        let role2: BoxExtractor<T> = match maybe_role2_extractor {
            Some(&role2) => Box::new(role2),
            None => {
                let ExecutorVariable::RowPosition(pos) = role2 else { unreachable!() };
                let value = row.get(pos).as_reference().into_owned();
                Box::new(move |_| value.clone())
            }
        };
        let maybe_player2_extractor = self.extractors.get(&player2);
        let player2: BoxExtractor<T> = match maybe_player2_extractor {
            Some(&player2) => Box::new(player2),
            None => {
                let ExecutorVariable::RowPosition(pos) = player2 else { unreachable!() };
                let value = row.get(pos).as_reference().into_owned();
                Box::new(move |_| value.clone())
            }
        };
        Box::new(move |value: &T| Ok(!(role1(value) == role2(value) && player1(value) == player2(value))))
    }

    fn filter_comparison(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: &MaybeOwnedRow<'_>,
        lhs: &CheckVertex<ExecutorVariable>,
        rhs: &CheckVertex<ExecutorVariable>,
        comparator: &Comparator,
        storage_counters: StorageCounters,
    ) -> Box<dyn Fn(&T) -> Result<bool, Box<ConceptReadError>>> {
        let maybe_lhs_extractor = lhs.as_variable().and_then(|var| self.extractors.get(&var));
        let lhs: BoxExtractor<T> = match maybe_lhs_extractor {
            Some(&lhs) => Box::new(lhs),
            None => make_const_extractor(lhs, row, context),
        };
        let rhs = match rhs {
            &CheckVertex::Variable(ExecutorVariable::RowPosition(pos)) => row.get(pos).as_reference(),
            &CheckVertex::Variable(_) => unreachable!(),
            &CheckVertex::Parameter(param) => {
                VariableValue::Value(context.parameters().value_unchecked(param).as_reference())
            }
            CheckVertex::Type(_) => unreachable!(),
        };
        let snapshot = context.snapshot.clone();
        let thing_manager = context.thing_manager.clone();
        let rhs = match rhs {
            VariableValue::Thing(Thing::Attribute(attr)) => {
                attr.get_value(&*snapshot, &thing_manager, storage_counters.clone()).map(Value::into_owned)
            }
            VariableValue::Value(value) => Ok(value.into_owned()),
            VariableValue::ThingList(_) | VariableValue::ValueList(_) => unimplemented_feature!(Lists),
            VariableValue::None | VariableValue::Type(_) | VariableValue::Thing(_) => unreachable!(),
        };
        let cmp: fn(&Value<'_>, &Value<'_>) -> bool = match comparator {
            Comparator::Equal => |a, b| a == b,
            Comparator::NotEqual => |a, b| a != b,
            Comparator::Less => |a, b| a < b,
            Comparator::Greater => |a, b| a > b,
            Comparator::LessOrEqual => |a, b| a <= b,
            Comparator::GreaterOrEqual => |a, b| a >= b,
            Comparator::Like => |a, b| {
                // TODO: Avoid recompiling the regex every time.
                regex::Regex::new(b.unwrap_string_ref())
                    .expect("Invalid regex should have been caught at compile time")
                    .is_match(a.unwrap_string_ref())
            },
            Comparator::Contains => |a, b| {
                let a_unicase = UniCase::new(a.unwrap_string_ref()).to_folded_case();
                let b_unicase = UniCase::new(b.unwrap_string_ref()).to_folded_case();
                a_unicase.contains(b_unicase.as_str())
            },
        };
        Box::new(move |value: &T| {
            // NOTE: Empty <op> Empty never matches
            let lhs = lhs(value);
            let lhs = match lhs {
                VariableValue::Thing(Thing::Attribute(attr)) => {
                    attr.get_value(&*snapshot, &thing_manager, storage_counters.clone())?.into_owned()
                }
                VariableValue::Value(value) => value,
                VariableValue::ThingList(_) | VariableValue::ValueList(_) => unimplemented_feature!(Lists),
                VariableValue::None | VariableValue::Type(_) | VariableValue::Thing(_) => unreachable!(),
            };
            let rhs = rhs.clone()?;
            if rhs.value_type().is_trivially_castable_to(lhs.value_type().category()) {
                Ok(cmp(&lhs, &rhs.cast(lhs.value_type().category()).unwrap()))
            } else if lhs.value_type().is_trivially_castable_to(rhs.value_type().category()) {
                Ok(cmp(&lhs.cast(rhs.value_type().category()).unwrap(), &rhs))
            } else {
                Ok(false)
            }
        })
    }
}

fn make_const_extractor<T>(
    vertex: &CheckVertex<ExecutorVariable>,
    row: &MaybeOwnedRow<'_>,
    context: &ExecutionContext<impl ReadableSnapshot + 'static>,
) -> Box<dyn for<'a> Fn(&'a T) -> VariableValue<'a>> {
    let value = get_vertex_value(vertex, Some(row), &context.parameters);
    let owned_value = value.into_owned();
    Box::new(move |_| owned_value.clone())
}

fn get_vertex_value<'a>(
    vertex: &'a CheckVertex<ExecutorVariable>,
    row: Option<&'a MaybeOwnedRow<'a>>,
    parameters: &'a ParameterRegistry,
) -> VariableValue<'a> {
    match vertex {
        CheckVertex::Variable(var) => match var {
            ExecutorVariable::RowPosition(position) => {
                row.expect("CheckVertex::Variable requires a row to take from").get(*position).as_reference()
            }
            ExecutorVariable::Internal(_) => {
                unreachable!("Check variables without an extractor must have been recorded in the row.")
            }
        },
        CheckVertex::Type(type_) => VariableValue::Type(*type_),
        CheckVertex::Parameter(parameter_id) => {
            VariableValue::Value(parameters.value_unchecked(*parameter_id).as_reference())
        }
    }
}

fn min_max_types<'a>(types: impl IntoIterator<Item = &'a Type>) -> (&'a Type, &'a Type) {
    minmax_or!(types.into_iter(), unreachable!("Empty type iterator"))
}
