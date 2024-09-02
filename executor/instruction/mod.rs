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
use compiler::match_::{
    instructions::{CheckInstruction, ConstraintInstruction},
    planner::pattern_plan::InstructionAPI,
};
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
pub(crate) mod tuple;

pub(crate) enum InstructionExecutor {
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
            ConstraintInstruction::Isa(isa) => {
                let executor = IsaExecutor::new(isa, variable_modes, sort_by);
                Ok(Self::Isa(executor))
            }
            ConstraintInstruction::IsaReverse(isa_reverse) => {
                let executor = IsaReverseExecutor::new(isa_reverse, variable_modes, sort_by);
                Ok(Self::IsaReverse(executor))
            }
            ConstraintInstruction::Has(has) => {
                let executor = HasExecutor::new(has, variable_modes, sort_by, snapshot, thing_manager)?;
                Ok(Self::Has(executor))
            }
            ConstraintInstruction::HasReverse(has_reverse) => {
                let executor = HasReverseExecutor::new(has_reverse, variable_modes, sort_by, snapshot, thing_manager)?;
                Ok(Self::HasReverse(executor))
            }
            ConstraintInstruction::Links(links) => {
                let executor = LinksExecutor::new(links, variable_modes, sort_by, snapshot, thing_manager)?;
                Ok(Self::Links(executor))
            }
            ConstraintInstruction::LinksReverse(links_reverse) => {
                let executor =
                    LinksReverseExecutor::new(links_reverse, variable_modes, sort_by, snapshot, thing_manager)?;
                Ok(Self::LinksReverse(executor))
            }
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
            InstructionExecutor::Isa(executor) => executor.get_iterator(snapshot, thing_manager, row),
            InstructionExecutor::IsaReverse(executor) => executor.get_iterator(snapshot, thing_manager, row),
            InstructionExecutor::Has(executor) => executor.get_iterator(snapshot, thing_manager, row),
            InstructionExecutor::HasReverse(executor) => executor.get_iterator(snapshot, thing_manager, row),
            InstructionExecutor::Links(executor) => executor.get_iterator(snapshot, thing_manager, row),
            InstructionExecutor::LinksReverse(executor) => executor.get_iterator(snapshot, thing_manager, row),
            InstructionExecutor::FunctionCallBinding(_executor) => todo!(),
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
        let constraint = instruction.constraint();
        let mut modes = Self::new();
        constraint.ids_foreach(|id, _| {
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
                    match (maybe_owner_extractor, maybe_attribute_extractor) {
                        (None, None) => unreachable!("filter unrelated to the iterator"),
                        (None, Some(&attr)) => {
                            let owner = row.get(owner).to_owned();
                            filters.push(Box::new({
                                move |value| {
                                    owner.as_thing().as_object().has_attribute(
                                        &*snapshot,
                                        &thing_manager,
                                        attr(value).as_thing().as_attribute().as_reference(),
                                    )
                                }
                            }));
                        }
                        (Some(&owner), None) => {
                            let attr = row.get(attribute).to_owned();
                            filters.push(Box::new({
                                move |value| {
                                    owner(value).as_thing().as_object().has_attribute(
                                        &*snapshot,
                                        &thing_manager,
                                        attr.as_thing().as_attribute().as_reference(),
                                    )
                                }
                            }));
                        }
                        (Some(&owner), Some(&attr)) => {
                            filters.push(Box::new({
                                move |value| {
                                    owner(value).as_thing().as_object().has_attribute(
                                        &*snapshot,
                                        &thing_manager,
                                        attr(value).as_thing().as_attribute().as_reference(),
                                    )
                                }
                            }));
                        }
                    }
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
