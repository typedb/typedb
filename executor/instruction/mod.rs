/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::HashMap,
    fmt,
    marker::PhantomData,
    ops::{Bound, RangeBounds},
};

use answer::{variable_value::VariableValue, Thing, Type};
use compiler::{
    executable::match_::instructions::{
        CheckInstruction, CheckVertex, ConstraintInstruction, VariableMode, VariableModes,
    },
    ExecutorVariable,
};
use concept::{
    error::ConceptReadError,
    thing::{object::ObjectAPI, thing_manager::ThingManager},
    type_::{OwnerAPI, PlayerAPI},
};
use encoding::value::{value::Value, ValueEncodable};
use ir::{
    pattern::{
        constraint::{Comparator, IsaKind, SubKind},
        ParameterID, Vertex,
    },
    pipeline::ParameterRegistry,
};
use itertools::Itertools;
use lending_iterator::higher_order::{FnHktHelper, Hkt};
use storage::snapshot::ReadableSnapshot;
use tracing::field::debug;

use crate::{
    instruction::{
        function_call_binding_executor::FunctionCallBindingIteratorExecutor, has_executor::HasExecutor,
        has_reverse_executor::HasReverseExecutor, is_executor::IsExecutor, isa_executor::IsaExecutor,
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

mod function_call_binding_executor;
mod has_executor;
mod has_reverse_executor;
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

pub(crate) enum InstructionExecutor {
    Is(IsExecutor),
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

    // RolePlayerIndex(RolePlayerIndexExecutor),
    FunctionCallBinding(FunctionCallBindingIteratorExecutor),
}

impl InstructionExecutor {
    pub(crate) fn new(
        instruction: ConstraintInstruction<ExecutorVariable>,
        variable_modes: VariableModes,
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        sort_by: ExecutorVariable,
    ) -> Result<Self, ConceptReadError> {
        match instruction {
            ConstraintInstruction::Is(is) => Ok(Self::Is(IsExecutor::new(is, variable_modes, sort_by))),
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
            ConstraintInstruction::FunctionCallBinding(_function_call) => todo!(),
            ConstraintInstruction::ComparisonCheck(_comparison) => todo!(),
            ConstraintInstruction::ExpressionBinding(_expression_binding) => todo!(),
        }
    }

    pub(crate) fn get_iterator(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: MaybeOwnedRow<'_>,
    ) -> Result<TupleIterator, ConceptReadError> {
        match self {
            Self::Is(executor) => executor.get_iterator(context, row),
            Self::TypeList(executor) => executor.get_iterator(context, row),
            Self::Sub(executor) => executor.get_iterator(context, row),
            Self::SubReverse(executor) => executor.get_iterator(context, row),
            Self::Owns(executor) => executor.get_iterator(context, row),
            Self::OwnsReverse(executor) => executor.get_iterator(context, row),
            Self::Relates(executor) => executor.get_iterator(context, row),
            Self::RelatesReverse(executor) => executor.get_iterator(context, row),
            Self::Plays(executor) => executor.get_iterator(context, row),
            Self::PlaysReverse(executor) => executor.get_iterator(context, row),
            Self::Isa(executor) => executor.get_iterator(context, row),
            Self::IsaReverse(executor) => executor.get_iterator(context, row),
            Self::Has(executor) => executor.get_iterator(context, row),
            Self::HasReverse(executor) => executor.get_iterator(context, row),
            Self::Links(executor) => executor.get_iterator(context, row),
            Self::LinksReverse(executor) => executor.get_iterator(context, row),
            Self::FunctionCallBinding(_executor) => todo!(),
        }
    }

    pub(crate) const fn name(&self) -> &'static str {
        match self {
            InstructionExecutor::Is(_) => "is",
            InstructionExecutor::Isa(_) => "isa",
            InstructionExecutor::IsaReverse(_) => "isa_reverse",
            InstructionExecutor::Has(_) => "has",
            InstructionExecutor::HasReverse(_) => "has_reverse",
            InstructionExecutor::Links(_) => "links",
            InstructionExecutor::LinksReverse(_) => "links_reverse",
            InstructionExecutor::FunctionCallBinding(_) => "fn_call_binding",
            InstructionExecutor::TypeList(_) => "[internal]type_list",
            InstructionExecutor::Sub(_) => "sub",
            InstructionExecutor::SubReverse(_) => "sub_reverse",
            InstructionExecutor::Owns(_) => "owns",
            InstructionExecutor::OwnsReverse(_) => "owns_reverse",
            InstructionExecutor::Relates(_) => "relates",
            InstructionExecutor::RelatesReverse(_) => "relates_reverse",
            InstructionExecutor::Plays(_) => "plays",
            InstructionExecutor::PlaysReverse(_) => "plays_reverse",
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

    pub(crate) fn is_inverted(&self) -> bool {
        self == &Self::UnboundInverted
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(crate) enum TernaryIterateMode {
    // [x, y, z] = standard sort order
    Unbound,
    // [y, x, z] sort order
    UnboundInverted,
    // [X, y, z] sort order
    BoundFrom,
    // [X, Y, z]
    BoundFromBoundTo,
}

impl TernaryIterateMode {
    pub(crate) fn new(
        from_vertex: &Vertex<ExecutorVariable>,
        to_vertex: &Vertex<ExecutorVariable>,
        var_modes: &VariableModes,
        sort_by: ExecutorVariable,
    ) -> TernaryIterateMode {
        // TODO
        // debug_assert!(var_modes.len() == 3);

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

fn type_from_row_or_annotations<'a>(
    vertex: &Vertex<ExecutorVariable>,
    row: MaybeOwnedRow<'_>,
    annos: impl Iterator<Item = &'a Type> + fmt::Debug,
) -> Type {
    match vertex {
        &Vertex::Variable(ExecutorVariable::RowPosition(var)) => {
            debug_assert!(row.len() > var.as_usize());
            let VariableValue::Type(type_) = row.get(var).to_owned() else { unreachable!("Supertype must be a type") };
            type_
        }
        &Vertex::Variable(ExecutorVariable::Internal(_)) => unreachable!("an internal variable cannot be an input"),
        Vertex::Label(_) => annos.cloned().exactly_one().expect("multiple types for fixed label?"),
        Vertex::Parameter(_) => unreachable!(),
    }
}

type FilterFn<T> =
    dyn for<'a, 'b> FnHktHelper<&'a Result<<T as Hkt>::HktSelf<'b>, ConceptReadError>, Result<bool, ConceptReadError>>;

pub(crate) struct Checker<T: Hkt> {
    extractors: HashMap<ExecutorVariable, for<'a, 'b> fn(&'a T::HktSelf<'b>) -> VariableValue<'a>>,
    checks: Vec<CheckInstruction<ExecutorVariable>>,
    _phantom_data: PhantomData<T>,
}

impl<T: Hkt> Checker<T> {
    pub(crate) fn new(
        checks: Vec<CheckInstruction<ExecutorVariable>>,
        extractors: HashMap<ExecutorVariable, for<'a, 'b> fn(&'a T::HktSelf<'b>) -> VariableValue<'a>>,
    ) -> Self {
        Self { extractors, checks, _phantom_data: PhantomData }
    }

    pub(crate) fn value_range_for(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: Option<MaybeOwnedRow<'_>>,
        target_variable: ExecutorVariable,
    ) -> Result<(Bound<Value<'_>>, Bound<Value<'_>>), ConceptReadError> {
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
                    if lhs.as_variable().is_some_and(|var| var == target_variable) {
                        let rhs_variable_value = Self::get_vertex_value(rhs, row.as_ref(), &context.parameters);
                        let rhs_value =
                            Self::read_value(context.snapshot.as_ref(), &context.thing_manager, &rhs_variable_value)?;
                        // let rhs_value = Self::read_value(context.snapshot.as_ref(), &context.thing_manager, &rhs_variable_value)?;
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
                        let lhs_variable_value = Self::get_vertex_value(lhs, row.as_ref(), &context.parameters);
                        let lhs_value =
                            Self::read_value(context.snapshot.as_ref(), &context.thing_manager, &lhs_variable_value)?;
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
                _ => (),
            }
        }
        let range = (range.0.map(|value| value.into_owned()), range.1.map(|value| value.into_owned()));
        Ok(range)
    }

    fn get_vertex_value<'b>(
        vertex: &'b CheckVertex<ExecutorVariable>,
        row: Option<&'b MaybeOwnedRow<'b>>,
        parameters: &'b ParameterRegistry,
    ) -> VariableValue<'b> {
        match vertex {
            CheckVertex::Variable(var) => match var {
                ExecutorVariable::RowPosition(position) => {
                    row.expect("CheckVertex::Variable requires a row to take from").get(*position).as_reference()
                }
                ExecutorVariable::Internal(_) => {
                    unreachable!("Comparator check variables must have been recorded in the row.")
                }
            },
            CheckVertex::Type(type_) => VariableValue::Type(type_.clone()),
            CheckVertex::Parameter(parameter_id) => {
                VariableValue::Value(parameters.value_unchecked(*parameter_id).as_reference())
            }
        }
    }

    fn read_value<'b, 'a>(
        snapshot: &'a impl ReadableSnapshot,
        thing_manager: &'a ThingManager,
        variable_value: &'a VariableValue<'a>,
    ) -> Result<Option<Value<'static>>, ConceptReadError> {
        // TODO: is there a way to do this without cloning the value?
        match variable_value {
            VariableValue::Thing(Thing::Attribute(attribute)) => {
                let value = attribute.get_value(snapshot, thing_manager)?;
                Ok(Some(value.into_owned()))
            }
            VariableValue::Value(value) => {
                let value = value.as_reference();
                Ok(Some(value.into_owned()))
            }
            _ => Ok(None),
        }
    }

    pub(crate) fn filter_for_row(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: &MaybeOwnedRow<'_>,
    ) -> Box<FilterFn<T>> {
        type BoxExtractor<T> = Box<dyn for<'a> Fn(&'a <T as Hkt>::HktSelf<'_>) -> VariableValue<'a>>;
        let mut filters: Vec<Box<dyn Fn(&T::HktSelf<'_>) -> Result<bool, ConceptReadError>>> =
            Vec::with_capacity(self.checks.len());

        for check in &self.checks {
            match check {
                &CheckInstruction::TypeList { type_var, ref types } => {
                    let maybe_type_extractor = self.extractors.get(&type_var);
                    let type_: BoxExtractor<T> = match maybe_type_extractor {
                        Some(&subtype) => Box::new(subtype),
                        None => make_const_extractor(&CheckVertex::Variable(type_var), context, row),
                    };
                    let types = types.clone();
                    filters.push(Box::new(move |value| Ok(types.contains(type_(value).as_type()))));
                }

                &CheckInstruction::Sub { sub_kind, ref subtype, ref supertype } => {
                    let maybe_subtype_extractor = subtype.as_variable().and_then(|var| self.extractors.get(&var));
                    let maybe_supertype_extractor = supertype.as_variable().and_then(|var| self.extractors.get(&var));
                    let snapshot = context.snapshot.clone();
                    let thing_manager = context.thing_manager.clone();
                    let subtype: BoxExtractor<T> = match maybe_subtype_extractor {
                        Some(&subtype) => Box::new(subtype),
                        None => make_const_extractor(subtype, context, row),
                    };
                    let supertype: BoxExtractor<T> = match maybe_supertype_extractor {
                        Some(&supertype) => Box::new(supertype),
                        None => make_const_extractor(supertype, context, row),
                    };
                    filters.push(Box::new({
                        move |value| {
                            let subtype = subtype(value);
                            let supertype = supertype(value);
                            match sub_kind {
                                SubKind::Subtype => subtype.as_type().is_transitive_subtype_of(
                                    supertype.as_type(),
                                    &*snapshot,
                                    thing_manager.type_manager(),
                                ),
                                SubKind::Exact => subtype.as_type().is_direct_subtype_of(
                                    subtype.as_type(),
                                    &*snapshot,
                                    thing_manager.type_manager(),
                                ),
                            }
                        }
                    }));
                }

                CheckInstruction::Owns { owner, attribute } => {
                    let maybe_owner_extractor = owner.as_variable().and_then(|var| self.extractors.get(&var));
                    let maybe_attribute_extractor = attribute.as_variable().and_then(|var| self.extractors.get(&var));
                    let snapshot = context.snapshot.clone();
                    let thing_manager = context.thing_manager.clone();
                    let owner: BoxExtractor<T> = match maybe_owner_extractor {
                        Some(&owner) => Box::new(owner),
                        None => make_const_extractor(owner, context, row),
                    };
                    let attribute: BoxExtractor<T> = match maybe_attribute_extractor {
                        Some(&attribute) => Box::new(attribute),
                        None => make_const_extractor(attribute, context, row),
                    };
                    filters.push(Box::new({
                        move |value| {
                            (owner(value).as_type().as_object_type())
                                .get_owns_attribute(
                                    &*snapshot,
                                    thing_manager.type_manager(),
                                    attribute(value).as_type().as_attribute_type(),
                                )
                                .map(|owns| owns.is_some())
                        }
                    }));
                }

                CheckInstruction::Relates { relation, role_type } => {
                    let maybe_relation_extractor = relation.as_variable().and_then(|var| self.extractors.get(&var));
                    let maybe_role_type_extractor = role_type.as_variable().and_then(|var| self.extractors.get(&var));
                    let snapshot = context.snapshot.clone();
                    let thing_manager = context.thing_manager.clone();
                    let relation: BoxExtractor<T> = match maybe_relation_extractor {
                        Some(&relation) => Box::new(relation),
                        None => make_const_extractor(relation, context, row),
                    };
                    let role_type: BoxExtractor<T> = match maybe_role_type_extractor {
                        Some(&role_type) => Box::new(role_type),
                        None => make_const_extractor(role_type, context, row),
                    };
                    filters.push(Box::new({
                        move |value| {
                            (relation(value).as_type().as_relation_type())
                                .get_relates_role(
                                    &*snapshot,
                                    thing_manager.type_manager(),
                                    role_type(value).as_type().as_role_type(),
                                )
                                .map(|relates| relates.is_some())
                        }
                    }));
                }

                CheckInstruction::Plays { player, role_type } => {
                    let maybe_player_extractor = player.as_variable().and_then(|var| self.extractors.get(&var));
                    let maybe_role_type_extractor = role_type.as_variable().and_then(|var| self.extractors.get(&var));
                    let snapshot = context.snapshot.clone();
                    let thing_manager = context.thing_manager.clone();
                    let player: BoxExtractor<T> = match maybe_player_extractor {
                        Some(&player) => Box::new(player),
                        None => make_const_extractor(player, context, row),
                    };
                    let role_type: BoxExtractor<T> = match maybe_role_type_extractor {
                        Some(&role_type) => Box::new(role_type),
                        None => make_const_extractor(role_type, context, row),
                    };
                    filters.push(Box::new({
                        move |value| {
                            (player(value).as_type().as_object_type())
                                .get_plays_role(
                                    &*snapshot,
                                    thing_manager.type_manager(),
                                    role_type(value).as_type().as_role_type(),
                                )
                                .map(|plays| plays.is_some())
                        }
                    }));
                }

                &CheckInstruction::Isa { isa_kind, ref type_, ref thing } => {
                    let maybe_thing_extractor = thing.as_variable().and_then(|var| self.extractors.get(&var));
                    let maybe_type_extractor = type_.as_variable().and_then(|var| self.extractors.get(&var));
                    let snapshot = context.snapshot.clone();
                    let thing_manager = context.thing_manager.clone();
                    let thing: BoxExtractor<T> = match maybe_thing_extractor {
                        Some(&thing) => Box::new(thing),
                        None => make_const_extractor(thing, context, row),
                    };
                    let type_: BoxExtractor<T> = match maybe_type_extractor {
                        Some(&type_) => Box::new(type_),
                        None => make_const_extractor(type_, context, row),
                    };
                    filters.push(Box::new({
                        move |value| {
                            let actual = thing(value).as_thing().type_();
                            let expected = type_(value);
                            if isa_kind == IsaKind::Exact && &actual != expected.as_type() {
                                Ok(false)
                            } else {
                                actual.is_transitive_subtype_of(
                                    expected.as_type(),
                                    &*snapshot,
                                    thing_manager.type_manager(),
                                )
                            }
                        }
                    }));
                }

                CheckInstruction::Has { owner, attribute } => {
                    let maybe_owner_extractor = owner.as_variable().and_then(|var| self.extractors.get(&var));
                    let maybe_attribute_extractor = attribute.as_variable().and_then(|var| self.extractors.get(&var));
                    let snapshot = context.snapshot.clone();
                    let thing_manager = context.thing_manager.clone();
                    let owner: BoxExtractor<T> = match maybe_owner_extractor {
                        Some(&owner) => Box::new(owner),
                        None => make_const_extractor(owner, context, row),
                    };
                    let attribute: BoxExtractor<T> = match maybe_attribute_extractor {
                        Some(&attribute) => Box::new(attribute),
                        None => make_const_extractor(attribute, context, row),
                    };
                    filters.push(Box::new({
                        move |value| {
                            owner(value).as_thing().as_object().has_attribute(
                                &*snapshot,
                                &thing_manager,
                                attribute(value).as_thing().as_attribute().as_reference(),
                            )
                        }
                    }));
                }

                CheckInstruction::Links { relation, player, role } => {
                    let maybe_relation_extractor = relation.as_variable().and_then(|var| self.extractors.get(&var));
                    let maybe_player_extractor = player.as_variable().and_then(|var| self.extractors.get(&var));
                    let maybe_role_extractor = role.as_variable().and_then(|var| self.extractors.get(&var));
                    let snapshot = context.snapshot.clone();
                    let thing_manager = context.thing_manager.clone();
                    let relation: BoxExtractor<T> = match maybe_relation_extractor {
                        Some(&relation) => Box::new(relation),
                        None => make_const_extractor(relation, context, row),
                    };
                    let player: BoxExtractor<T> = match maybe_player_extractor {
                        Some(&player) => Box::new(player),
                        None => make_const_extractor(player, context, row),
                    };
                    let role: BoxExtractor<T> = match maybe_role_extractor {
                        Some(&role) => Box::new(role),
                        None => make_const_extractor(role, context, row),
                    };
                    filters.push(Box::new({
                        move |value| {
                            relation(value).as_thing().as_relation().has_role_player(
                                &*snapshot,
                                &thing_manager,
                                &player(value).as_thing().as_object(),
                                role(value).as_type().as_role_type().clone(),
                            )
                        }
                    }));
                }

                &CheckInstruction::Is { lhs, rhs } => {
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
                    filters.push(Box::new(move |value| Ok(lhs(value) == rhs(value))));
                }
                CheckInstruction::Comparison { lhs, rhs, comparator } => {
                    let maybe_lhs_extractor = lhs.as_variable().and_then(|var| self.extractors.get(&var));
                    let lhs: BoxExtractor<T> = match maybe_lhs_extractor {
                        Some(&lhs) => Box::new(lhs),
                        None => make_const_extractor(lhs, context, row),
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
                            attr.get_value(&*snapshot, &thing_manager).map(Value::into_owned)
                        }
                        VariableValue::Value(value) => Ok(value.into_owned()),
                        VariableValue::ThingList(_) | VariableValue::ValueList(_) => todo!(),
                        VariableValue::Empty | VariableValue::Type(_) | VariableValue::Thing(_) => unreachable!(),
                    };
                    let cmp: fn(&Value<'_>, &Value<'_>) -> bool = match comparator {
                        Comparator::Equal => |a, b| a == b,
                        Comparator::NotEqual => |a, b| a != b,
                        Comparator::Less => |a, b| a < b,
                        Comparator::Greater => |a, b| a > b,
                        Comparator::LessOrEqual => |a, b| a <= b,
                        Comparator::GreaterOrEqual => |a, b| a >= b,
                        Comparator::Like => todo!("like"),
                        Comparator::Contains => todo!("contains"),
                    };
                    filters.push(Box::new(move |value| {
                        let lhs = lhs(value);
                        let lhs = match lhs {
                            VariableValue::Thing(Thing::Attribute(attr)) => {
                                attr.get_value(&*snapshot, &thing_manager)?.into_owned()
                            }
                            VariableValue::Value(value) => value,
                            VariableValue::ThingList(_) | VariableValue::ValueList(_) => todo!(),
                            VariableValue::Empty | VariableValue::Type(_) | VariableValue::Thing(_) => unreachable!(),
                        };
                        let rhs = rhs.clone()?;
                        if rhs.value_type().is_trivially_castable_to(&lhs.value_type()) {
                            Ok(cmp(&lhs, &rhs.cast(&lhs.value_type()).unwrap()))
                        } else if lhs.value_type().is_trivially_castable_to(&rhs.value_type()) {
                            Ok(cmp(&lhs.cast(&rhs.value_type()).unwrap(), &rhs))
                        } else {
                            return Ok(false);
                        }
                    }));
                }
            }
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
}

fn make_const_extractor<T: Hkt>(
    vertex: &CheckVertex<ExecutorVariable>,
    context: &ExecutionContext<impl ReadableSnapshot + 'static>,
    row: &MaybeOwnedRow<'_>,
) -> Box<dyn for<'a> Fn(&'a <T as Hkt>::HktSelf<'_>) -> VariableValue<'a>> {
    let value = match vertex {
        &CheckVertex::Variable(ExecutorVariable::RowPosition(pos)) => row.get(pos).as_reference(),
        &CheckVertex::Variable(ExecutorVariable::Internal(_)) => unreachable!(),
        &CheckVertex::Parameter(param) => {
            VariableValue::Value(context.parameters().value_unchecked(param).as_reference())
        }
        CheckVertex::Type(type_) => VariableValue::Type(type_.clone()),
    };
    let owned_value = value.into_owned();
    Box::new(move |_| owned_value.clone())
}
