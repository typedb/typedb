/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::fmt;

use ::iterator::minmax_or;
use answer::{variable_value::VariableValue, Type};
use compiler::{
    executable::match_::instructions::{
        ConstraintInstruction, VariableMode, VariableModes,
    },
    ExecutorVariable,
};
use concept::{
    error::ConceptReadError,
    thing::{object::ObjectAPI, thing_manager::ThingManager, ThingAPI},
    type_::{OwnerAPI, PlayerAPI},
};
use encoding::{
    value::ValueEncodable,
    AsBytes,
};
use ir::pattern::Vertex;
use itertools::Itertools;
use resource::profile::StorageCounters;
use storage::snapshot::ReadableSnapshot;
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
pub(crate) mod checker;

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


fn min_max_types<'a>(types: impl IntoIterator<Item = &'a Type>) -> (&'a Type, &'a Type) {
    minmax_or!(types.into_iter(), unreachable!("Empty type iterator"))
}
