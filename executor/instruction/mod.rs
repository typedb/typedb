/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::HashMap,
    marker::PhantomData,
    ops::{Bound, RangeBounds},
    sync::Arc,
};

use answer::variable_value::VariableValue;
use compiler::match_::instructions::{CheckInstruction, ConstraintInstruction};
use concept::{
    error::ConceptReadError,
    thing::{object::ObjectAPI, thing_manager::ThingManager},
};
use ir::pattern::constraint::Comparator;
use lending_iterator::higher_order::{FnHktHelper, Hkt};
use storage::snapshot::ReadableSnapshot;

use crate::{
    instruction::{
        function_call_binding_executor::FunctionCallBindingIteratorExecutor, has_executor::HasExecutor,
        has_reverse_executor::HasReverseExecutor, isa_executor::IsaExecutor, isa_reverse_executor::IsaReverseExecutor,
        iterator::TupleIterator, links_executor::LinksExecutor, links_reverse_executor::LinksReverseExecutor,
        owns_executor::OwnsExecutor, owns_reverse_executor::OwnsReverseExecutor, plays_executor::PlaysExecutor,
        plays_reverse_executor::PlaysReverseExecutor, relates_executor::RelatesExecutor,
        relates_reverse_executor::RelatesReverseExecutor, sub_executor::SubExecutor,
        sub_reverse_executor::SubReverseExecutor, type_list_executor::TypeListExecutor,
    },
    row::MaybeOwnedRow,
    VariablePosition,
};

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
            ConstraintInstruction::ComparisonGenerator(_comparison) => todo!(),
            ConstraintInstruction::ComparisonGeneratorReverse(_comparison) => todo!(),
            ConstraintInstruction::ComparisonCheck(_comparison) => todo!(),
            ConstraintInstruction::ExpressionBinding(_expression_binding) => todo!(),
        }
    }

    pub(crate) fn get_iterator(
        &self,
        snapshot: &Arc<impl ReadableSnapshot + 'static>,
        thing_manager: &Arc<ThingManager>,
        row: MaybeOwnedRow<'_>,
    ) -> Result<TupleIterator, ConceptReadError> {
        match self {
            Self::TypeList(executor) => executor.get_iterator(snapshot, thing_manager, row),
            Self::Sub(executor) => executor.get_iterator(snapshot, thing_manager, row),
            Self::SubReverse(executor) => executor.get_iterator(snapshot, thing_manager, row),
            Self::Owns(executor) => executor.get_iterator(snapshot, thing_manager, row),
            Self::OwnsReverse(executor) => executor.get_iterator(snapshot, thing_manager, row),
            Self::Relates(executor) => executor.get_iterator(snapshot, thing_manager, row),
            Self::RelatesReverse(executor) => executor.get_iterator(snapshot, thing_manager, row),
            Self::Plays(executor) => executor.get_iterator(snapshot, thing_manager, row),
            Self::PlaysReverse(executor) => executor.get_iterator(snapshot, thing_manager, row),
            Self::Isa(executor) => executor.get_iterator(snapshot, thing_manager, row),
            Self::IsaReverse(executor) => executor.get_iterator(snapshot, thing_manager, row),
            Self::Has(executor) => executor.get_iterator(snapshot, thing_manager, row),
            Self::HasReverse(executor) => executor.get_iterator(snapshot, thing_manager, row),
            Self::Links(executor) => executor.get_iterator(snapshot, thing_manager, row),
            Self::LinksReverse(executor) => executor.get_iterator(snapshot, thing_manager, row),
            Self::FunctionCallBinding(_executor) => todo!(),
        }
    }

    pub(crate) const fn name(&self) -> &'static str {
        match self {
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
        from_var: VariablePosition,
        to_var: VariablePosition,
        var_modes: &VariableModes,
        sort_by: Option<VariablePosition>,
    ) -> BinaryIterateMode {
        debug_assert!(var_modes.len() == 2);
        debug_assert!(!var_modes.all_inputs());

        let is_from_bound = var_modes.get(from_var) == Some(&VariableMode::Input);
        debug_assert!(var_modes.get(to_var) != Some(&VariableMode::Input));

        if is_from_bound {
            Self::BoundFrom
        } else if sort_by == Some(to_var) {
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
        from_var: VariablePosition,
        to_var: VariablePosition,
        var_modes: &VariableModes,
        sort_by: Option<VariablePosition>,
    ) -> TernaryIterateMode {
        debug_assert!(var_modes.len() == 3);
        debug_assert!(!var_modes.all_inputs());
        let is_from_bound = var_modes.get(from_var) == Some(&VariableMode::Input);
        let is_to_bound = var_modes.get(to_var) == Some(&VariableMode::Input);

        if is_to_bound {
            assert!(is_from_bound);
            Self::BoundFromBoundTo
        } else if is_from_bound {
            Self::BoundFrom
        } else if sort_by == Some(to_var) {
            Self::UnboundInverted
        } else {
            Self::Unbound
        }
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

        let mut range = (Bound::Unbounded, Bound::Unbounded);
        for check in &self.checks {
            match *check {
                CheckInstruction::Comparison { lhs, rhs, comparator } if lhs == target => {
                    let rhs = row.get(rhs).to_owned();
                    let comp_range = match comparator {
                        Comparator::Equal => (Bound::Included(rhs.clone()), Bound::Included(rhs)),
                        Comparator::Less => (Bound::Unbounded, Bound::Excluded(rhs)),
                        Comparator::LessOrEqual => (Bound::Unbounded, Bound::Included(rhs)),
                        Comparator::Greater => (Bound::Excluded(rhs), Bound::Unbounded),
                        Comparator::GreaterOrEqual => (Bound::Included(rhs), Bound::Unbounded),
                        Comparator::Like => continue,
                        Comparator::Cointains => continue,
                    };
                    range = intersect(range, comp_range);
                }
                _ => (),
            }
        }
        range
    }

    fn filter_for_row(
        &self,
        snapshot: &Arc<impl ReadableSnapshot + 'static>,
        thing_manager: &Arc<ThingManager>,
        row: &MaybeOwnedRow<'_>,
    ) -> Box<FilterFn<T>> {
        type BoxExtractor<T> = Box<dyn for<'a> Fn(&'a <T as Hkt>::HktSelf<'_>) -> VariableValue<'a>>;
        let mut filters: Vec<Box<dyn Fn(&T::HktSelf<'_>) -> Result<bool, ConceptReadError>>> =
            Vec::with_capacity(self.checks.len());
        for check in &self.checks {
            match *check {
                CheckInstruction::Comparison { lhs, rhs, comparator } => {
                    let lhs_extractor = self.extractors[&lhs];
                    let rhs = row.get(rhs).to_owned();
                    let cmp: fn(&VariableValue<'_>, &VariableValue<'_>) -> bool = match comparator {
                        Comparator::Equal => |a, b| a == b,
                        Comparator::Less => |a, b| a < b,
                        Comparator::Greater => |a, b| a > b,
                        Comparator::LessOrEqual => |a, b| a <= b,
                        Comparator::GreaterOrEqual => |a, b| a >= b,
                        Comparator::Like => todo!("like"),
                        Comparator::Cointains => todo!("contains"),
                    };
                    filters.push(Box::new(move |value| Ok(cmp(&lhs_extractor(value), &rhs))));
                }
                CheckInstruction::Has { owner, attribute } => {
                    let maybe_owner_extractor = self.extractors.get(&owner);
                    let maybe_attribute_extractor = self.extractors.get(&attribute);
                    let snapshot = snapshot.clone();
                    let thing_manager = thing_manager.clone();
                    let owner: BoxExtractor<T> = match maybe_owner_extractor {
                        Some(&owner) => Box::new(owner),
                        None => {
                            let owner = row.get(owner).to_owned();
                            Box::new(move |_| owner.clone())
                        }
                    };
                    let attribute: BoxExtractor<T> = match maybe_attribute_extractor {
                        Some(&attribute) => Box::new(attribute),
                        None => {
                            let attribute = row.get(attribute).to_owned();
                            Box::new(move |_| attribute.clone())
                        }
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
                    let maybe_relation_extractor = self.extractors.get(&relation);
                    let maybe_player_extractor = self.extractors.get(&player);
                    let maybe_role_extractor = self.extractors.get(&role);
                    let snapshot = snapshot.clone();
                    let thing_manager = thing_manager.clone();
                    let relation: BoxExtractor<T> = match maybe_relation_extractor {
                        Some(&relation) => Box::new(relation),
                        None => {
                            let relation = row.get(relation).to_owned();
                            Box::new(move |_| relation.clone())
                        }
                    };
                    let player: BoxExtractor<T> = match maybe_player_extractor {
                        Some(&player) => Box::new(player),
                        None => {
                            let player = row.get(player).to_owned();
                            Box::new(move |_| player.clone())
                        }
                    };
                    let role: BoxExtractor<T> = match maybe_role_extractor {
                        Some(&role) => Box::new(role),
                        None => {
                            let role = row.get(role).to_owned();
                            Box::new(move |_| role.clone())
                        }
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
                _ => todo!(),
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
