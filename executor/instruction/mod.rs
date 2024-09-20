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
use compiler::match_::instructions::{CheckInstruction, ConstraintInstruction};
use concept::{
    error::ConceptReadError,
    thing::{object::ObjectAPI, thing_manager::ThingManager},
    type_::{OwnerAPI, PlayerAPI},
};
use encoding::value::value::Value;
use ir::pattern::{
    constraint::{Comparator, IsaKind, SubKind},
    Vertex,
};
use itertools::Itertools;
use lending_iterator::higher_order::{FnHktHelper, Hkt};
use storage::snapshot::ReadableSnapshot;

use crate::{
    instruction::{
        constant_executor::ConstantExecutor, function_call_binding_executor::FunctionCallBindingIteratorExecutor,
        has_executor::HasExecutor, has_reverse_executor::HasReverseExecutor, isa_executor::IsaExecutor,
        isa_reverse_executor::IsaReverseExecutor, iterator::TupleIterator, links_executor::LinksExecutor,
        links_reverse_executor::LinksReverseExecutor, owns_executor::OwnsExecutor,
        owns_reverse_executor::OwnsReverseExecutor, plays_executor::PlaysExecutor,
        plays_reverse_executor::PlaysReverseExecutor, relates_executor::RelatesExecutor,
        relates_reverse_executor::RelatesReverseExecutor, sub_executor::SubExecutor,
        sub_reverse_executor::SubReverseExecutor, type_list_executor::TypeListExecutor,
    },
    pipeline::stage::ExecutionContext,
    row::MaybeOwnedRow,
    VariablePosition,
};

mod constant_executor;
mod function_call_binding_executor;
mod has_executor;
mod has_reverse_executor;
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

pub(crate) enum InstructionExecutor {
    TypeList(TypeListExecutor),

    Sub(SubExecutor),
    SubReverse(SubReverseExecutor),

    Owns(OwnsExecutor),
    OwnsReverse(OwnsReverseExecutor),

    Relates(RelatesExecutor),
    RelatesReverse(RelatesReverseExecutor),

    Plays(PlaysExecutor),
    PlaysReverse(PlaysReverseExecutor),

    Constant(ConstantExecutor),

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
        instruction: ConstraintInstruction<VariablePosition>,
        selected: &[VariablePosition],
        named: &HashMap<VariablePosition, String>,
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        sort_by: Option<VariablePosition>,
    ) -> Result<Self, ConceptReadError> {
        let variable_modes = VariableModes::new_for(&instruction, selected, named);
        match instruction {
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
            Self::Constant(executor) => executor.get_iterator(context, row),
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
            InstructionExecutor::Constant(_) => "constant",
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
pub(crate) enum VariableMode {
    Input,
    Output,
    Count,
    Check,
}

impl VariableMode {
    pub(crate) const fn new(is_input: bool, is_selected: bool, is_named: bool) -> VariableMode {
        if is_input {
            Self::Input
        } else if is_selected {
            Self::Output
        } else if is_named {
            Self::Count
        } else {
            Self::Check
        }
    }
}

pub(crate) struct VariableModes {
    modes: HashMap<VariablePosition, VariableMode>,
}

impl VariableModes {
    fn new() -> Self {
        VariableModes { modes: HashMap::new() }
    }

    pub(crate) fn new_for(
        instruction: &ConstraintInstruction<VariablePosition>,
        selected: &[VariablePosition],
        named: &HashMap<VariablePosition, String>,
    ) -> Self {
        let mut modes = Self::new();
        instruction.ids_foreach(|id| {
            let var_mode =
                VariableMode::new(instruction.is_input_variable(id), selected.contains(&id), named.contains_key(&id));
            modes.insert(id, var_mode)
        });
        modes
    }

    fn insert(&mut self, variable_position: VariablePosition, mode: VariableMode) {
        let existing = self.modes.insert(variable_position, mode);
        debug_assert!(existing.is_none())
    }

    pub(crate) fn get(&self, variable_position: VariablePosition) -> Option<&VariableMode> {
        self.modes.get(&variable_position)
    }

    pub(crate) fn all_inputs(&self) -> bool {
        self.modes.values().all(|mode| mode == &VariableMode::Input)
    }

    pub(crate) fn none_inputs(&self) -> bool {
        self.modes.values().all(|mode| mode != &VariableMode::Input)
    }

    fn len(&self) -> usize {
        self.modes.len()
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
        from_vertex: &Vertex<VariablePosition>,
        to_vertex: &Vertex<VariablePosition>,
        var_modes: &VariableModes,
        sort_by: Option<VariablePosition>,
    ) -> BinaryIterateMode {
        // TODO
        // debug_assert!(var_modes.len() == 2);
        debug_assert!(!var_modes.all_inputs());

        let is_from_bound = match from_vertex {
            &Vertex::Variable(from_var) => var_modes.get(from_var) == Some(&VariableMode::Input),
            Vertex::Label(_) | Vertex::Parameter(_) => true,
        };

        // TODO
        // debug_assert!(var_modes.get(to_var) != Some(&VariableMode::Input));

        if is_from_bound {
            Self::BoundFrom
        } else if sort_by.is_some_and(|sort_var| Some(sort_var) == to_vertex.as_variable()) {
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
        from_vertex: &Vertex<VariablePosition>,
        to_vertex: &Vertex<VariablePosition>,
        var_modes: &VariableModes,
        sort_by: Option<VariablePosition>,
    ) -> TernaryIterateMode {
        // TODO
        // debug_assert!(var_modes.len() == 3);

        debug_assert!(!var_modes.all_inputs());

        let is_from_bound = match from_vertex {
            &Vertex::Variable(from_var) => var_modes.get(from_var) == Some(&VariableMode::Input),
            Vertex::Label(_) | Vertex::Parameter(_) => true,
        };

        let is_to_bound = match to_vertex {
            &Vertex::Variable(to_var) => var_modes.get(to_var) == Some(&VariableMode::Input),
            Vertex::Label(_) | Vertex::Parameter(_) => true,
        };

        if is_to_bound {
            assert!(is_from_bound);
            Self::BoundFromBoundTo
        } else if is_from_bound {
            Self::BoundFrom
        } else if sort_by.is_some_and(|sort_var| Some(sort_var) == to_vertex.as_variable()) {
            Self::UnboundInverted
        } else {
            Self::Unbound
        }
    }
}

fn type_from_row_or_annotations<'a>(
    vertex: &Vertex<VariablePosition>,
    row: MaybeOwnedRow<'_>,
    annos: impl Iterator<Item = &'a Type> + fmt::Debug,
) -> Type {
    match vertex {
        &Vertex::Variable(var) => {
            debug_assert!(row.len() > var.as_usize());
            let VariableValue::Type(type_) = row.get(var).to_owned() else { unreachable!("Supertype must be a type") };
            type_
        }
        Vertex::Label(_) => annos.cloned().exactly_one().expect("multiple types for fixed label?"),
        Vertex::Parameter(_) => unreachable!(),
    }
}

type FilterFn<T> =
    dyn for<'a, 'b> FnHktHelper<&'a Result<<T as Hkt>::HktSelf<'b>, ConceptReadError>, Result<bool, ConceptReadError>>;

struct Checker<T: Hkt> {
    extractors: HashMap<VariablePosition, for<'a, 'b> fn(&'a T::HktSelf<'b>) -> VariableValue<'a>>,
    checks: Vec<CheckInstruction<VariablePosition>>,
    _phantom_data: PhantomData<T>,
}

impl<T: Hkt> Checker<T> {
    fn range_for<const N: usize>(
        &self,
        row: MaybeOwnedRow<'_>,
        target: VariablePosition,
    ) -> impl RangeBounds<VariableValue<'_>> {
        fn intersect<'a>(
            (a_min, a_max): (Bound<VariableValue<'a>>, Bound<VariableValue<'a>>),
            (b_min, b_max): (Bound<VariableValue<'a>>, Bound<VariableValue<'a>>),
        ) -> (Bound<VariableValue<'a>>, Bound<VariableValue<'a>>) {
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

        /*
        let mut range = (Bound::Unbounded, Bound::Unbounded);
        for check in &self.checks {
            match *check {
                CheckInstruction::Comparison { lhs, rhs, comparator } if lhs == Vertex::Variable(target) => {
                    let rhs = row.get(rhs).to_owned();
                    let comp_range = match comparator {
                        Comparator::Equal => (Bound::Included(rhs.clone()), Bound::Included(rhs)),
                        Comparator::Less => (Bound::Unbounded, Bound::Excluded(rhs)),
                        Comparator::LessOrEqual => (Bound::Unbounded, Bound::Included(rhs)),
                        Comparator::Greater => (Bound::Excluded(rhs), Bound::Unbounded),
                        Comparator::GreaterOrEqual => (Bound::Included(rhs), Bound::Unbounded),
                        Comparator::Like => continue,
                        Comparator::Contains => continue,
                    };
                    range = intersect(range, comp_range);
                }
                _ => (),
            }
        }
        range
        */
        todo!() as (Bound<VariableValue<'_>>, Bound<VariableValue<'_>>)
    }

    fn filter_for_row(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: &MaybeOwnedRow<'_>,
    ) -> Box<FilterFn<T>> {
        type BoxExtractor<T> = Box<dyn for<'a> Fn(&'a <T as Hkt>::HktSelf<'_>) -> VariableValue<'a>>;
        let mut filters: Vec<Box<dyn Fn(&T::HktSelf<'_>) -> Result<bool, ConceptReadError>>> =
            Vec::with_capacity(self.checks.len());

        for check in &self.checks {
            match check {
                &CheckInstruction::Sub { sub_kind, ref subtype, ref supertype } => {
                    let maybe_subtype_extractor = subtype.as_variable().and_then(|var| self.extractors.get(&var));
                    let maybe_supertype_extractor = supertype.as_variable().and_then(|var| self.extractors.get(&var));
                    let snapshot = context.snapshot.clone();
                    let thing_manager = context.thing_manager.clone();
                    let subtype: BoxExtractor<T> = match maybe_subtype_extractor {
                        Some(&subtype) => Box::new(subtype),
                        None => make_const_extractor(subtype, row),
                    };
                    let supertype: BoxExtractor<T> = match maybe_supertype_extractor {
                        Some(&supertype) => Box::new(supertype),
                        None => make_const_extractor(supertype, row),
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
                        None => make_const_extractor(owner, row),
                    };
                    let attribute: BoxExtractor<T> = match maybe_attribute_extractor {
                        Some(&attribute) => Box::new(attribute),
                        None => make_const_extractor(attribute, row),
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
                        None => make_const_extractor(relation, row),
                    };
                    let role_type: BoxExtractor<T> = match maybe_role_type_extractor {
                        Some(&role_type) => Box::new(role_type),
                        None => make_const_extractor(role_type, row),
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
                        None => make_const_extractor(player, row),
                    };
                    let role_type: BoxExtractor<T> = match maybe_role_type_extractor {
                        Some(&role_type) => Box::new(role_type),
                        None => make_const_extractor(role_type, row),
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
                        None => make_const_extractor(thing, row),
                    };
                    let type_: BoxExtractor<T> = match maybe_type_extractor {
                        Some(&type_) => Box::new(type_),
                        None => make_const_extractor(type_, row),
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
                        None => make_const_extractor(owner, row),
                    };
                    let attribute: BoxExtractor<T> = match maybe_attribute_extractor {
                        Some(&attribute) => Box::new(attribute),
                        None => make_const_extractor(attribute, row),
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
                        None => make_const_extractor(relation, row),
                    };
                    let player: BoxExtractor<T> = match maybe_player_extractor {
                        Some(&player) => Box::new(player),
                        None => make_const_extractor(player, row),
                    };
                    let role: BoxExtractor<T> = match maybe_role_extractor {
                        Some(&role) => Box::new(role),
                        None => make_const_extractor(role, row),
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

                CheckInstruction::Comparison { lhs, rhs, comparator } => {
                    let lhs_extractor = self.extractors[&lhs.as_variable().unwrap()];
                    let rhs = match rhs {
                        &Vertex::Variable(pos) => row.get(pos).to_owned(),
                        &Vertex::Parameter(param) => VariableValue::Value(context.parameters()[param].to_owned()),
                        Vertex::Label(_) => unreachable!(),
                    };
                    let cmp: fn(&Value<'_>, &Value<'_>) -> bool = match comparator {
                        Comparator::Equal => |a, b| a == b,
                        Comparator::Less => |a, b| a < b,
                        Comparator::Greater => |a, b| a > b,
                        Comparator::LessOrEqual => |a, b| a <= b,
                        Comparator::GreaterOrEqual => |a, b| a >= b,
                        Comparator::Like => todo!("like"),
                        Comparator::Contains => todo!("contains"),
                    };
                    let snapshot = context.snapshot.clone();
                    let thing_manager = context.thing_manager.clone();
                    filters.push(Box::new(move |value| {
                        let lhs = lhs_extractor(value);
                        let lhs = match lhs {
                            VariableValue::Thing(Thing::Attribute(attr)) => {
                                attr.get_value(&*snapshot, &thing_manager)?.into_owned()
                            }
                            VariableValue::Value(value) => value,
                            VariableValue::ThingList(_) | VariableValue::ValueList(_) => todo!(),
                            VariableValue::Empty | VariableValue::Type(_) | VariableValue::Thing(_) => unreachable!(),
                        };
                        let rhs = match &rhs {
                            VariableValue::Thing(Thing::Attribute(attr)) => {
                                &attr.get_value(&*snapshot, &thing_manager)?
                            }
                            VariableValue::Value(value) => value,
                            VariableValue::ThingList(_) | VariableValue::ValueList(_) => todo!(),
                            VariableValue::Empty | VariableValue::Type(_) | VariableValue::Thing(_) => unreachable!(),
                        };
                        Ok(cmp(&lhs, rhs))
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
    relation: &Vertex<VariablePosition>,
    row: &MaybeOwnedRow<'_>,
) -> Box<dyn for<'a> Fn(&'a <T as Hkt>::HktSelf<'_>) -> VariableValue<'a>> {
    match relation {
        &Vertex::Variable(var) => {
            let relation = row.get(var).to_owned();
            Box::new(move |_| relation.clone())
        }
        Vertex::Parameter(_) => todo!(),
        Vertex::Label(_) => unreachable!(),
    }
}
