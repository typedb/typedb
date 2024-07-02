/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */


use std::{
    collections::HashMap,
    sync::Arc,
};

use answer::Thing;

use answer::variable::Variable;
use answer::variable_value::VariableValue;
use concept::error::{ConceptError, ConceptReadError};
use concept::thing::has::Has;
use concept::thing::thing_manager::ThingManager;
use ir::inference::type_inference::TypeAnnotations;
use lending_iterator::LendingIterator;
use storage::snapshot::ReadableSnapshot;

use crate::{
    executor::{pattern_executor::Row, Position},
    planner::pattern_plan::Iterate,
};
use crate::executor::iterator::comparison_provider::ComparisonProvider;
use crate::executor::iterator::comparison_reverse_provider::ComparisonReverseProvider;
use crate::executor::iterator::function_call_binding_provier::FunctionCallBindingProvider;
use crate::executor::iterator::has_provider::{HasBoundedSortedToIterator, HasProvider, HasUnboundedSortedFromIterator, HasUnboundedSortedToSingleIterator};
use crate::executor::iterator::has_reverse_provider::HasReverseProvider;
use crate::executor::iterator::role_player_provider::RolePlayerProvider;
use crate::executor::iterator::role_player_reverse_provider::RolePlayerReverseProvider;
use crate::planner::pattern_plan::IterateMode;

mod has_reverse_provider;
mod role_player_provider;
mod role_player_reverse_provider;
mod function_call_binding_provier;
mod comparison_provider;
mod comparison_reverse_provider;
mod has_provider;

pub(crate) enum ConstraintIteratorProvider {
    Has(HasProvider),
    HasReverse(HasReverseProvider),

    RolePlayer(RolePlayerProvider),
    RolePlayerReverse(RolePlayerReverseProvider),

    // RelationIndex(RelationIndexProvider)
    // RelationIndexReverse(RelationIndexReverseProvider)
    FunctionCallBinding(FunctionCallBindingProvider),

    Comparison(ComparisonProvider),
    ComparisonReverse(ComparisonReverseProvider),
}

impl ConstraintIteratorProvider {
    pub(crate) fn new<Snapshot: ReadableSnapshot>(
        iterate: Iterate,
        variable_to_position: &HashMap<Variable, Position>,
        type_annotations: &TypeAnnotations,
        snapshot: &Snapshot,
        thing_manager: &ThingManager<Snapshot>
    ) -> Result<Self, ConceptReadError> {
        match iterate {
            Iterate::Has(has, mode) => {
                let has_attribute = has.attribute();
                let provider = HasProvider::new(
                    has.clone().into_ids(variable_to_position),
                    mode,
                    type_annotations.constraint_annotations(has.into()).unwrap().get_left_right().left_to_right(),
                    type_annotations.variable_annotations(has_attribute).unwrap(),
                    snapshot,
                    thing_manager,
                )?;
                Ok(Self::Has(provider))
            }
            Iterate::HasReverse(has, mode) => {
                Ok(Self::HasReverse(HasReverseProvider::new(has.into_ids(variable_to_position), mode)))
            }
            Iterate::RolePlayer(rp, mode) => {
                Ok(Self::RolePlayer(RolePlayerProvider::new(rp.into_ids(variable_to_position), mode)))
            }
            Iterate::RolePlayerReverse(rp, mode) => {
                Ok(Self::RolePlayerReverse(RolePlayerReverseProvider::new(
                    rp.into_ids(variable_to_position),
                    mode,
                )))
            },
            Iterate::FunctionCallBinding(function_call) => {
                todo!()
            }
            Iterate::Comparison(comparison) => {
                todo!()
            }
            Iterate::ComparisonReverse(comparison) => {
                todo!()
            }
        }
    }

    pub(crate) fn get_iterator<Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        thing_manager: &ThingManager<Snapshot>,
        row: &Row
    ) -> Result<ConstraintIterator, ConceptReadError> {
        match self {
            ConstraintIteratorProvider::Has(provider) => provider.get_iterator(snapshot, thing_manager, row),
            ConstraintIteratorProvider::HasReverse(provider) => todo!(),
            ConstraintIteratorProvider::RolePlayer(provider) => todo!(),
            ConstraintIteratorProvider::RolePlayerReverse(provider) => todo!(),
            ConstraintIteratorProvider::FunctionCallBinding(provider) => todo!(),
            ConstraintIteratorProvider::Comparison(provider) => todo!(),
            ConstraintIteratorProvider::ComparisonReverse(provider) => todo!(),
        }
    }
}

pub(crate) enum ConstraintIterator {
    HasUnboundedSortedOwner(HasUnboundedSortedFromIterator, ir::pattern::constraint::Has<Position>),
    // HasUnboundedSortedAttributeMulti(HasUnboundedSortedAttributeMultiIterator),
    // TODO: perhaps we can merge with HasBoundedSortedTo?
    HasUnboundedSortedAttributeSingle(HasUnboundedSortedToSingleIterator, ir::pattern::constraint::Has<Position>),
    HasBoundedSortedAttribute(HasBoundedSortedToIterator, ir::pattern::constraint::Has<Position>),
}

impl ConstraintIterator {
    pub(crate) fn peek_sorted_value(&mut self) -> Option<Result<VariableValue<'_>, &ConceptReadError>> {
        debug_assert!(self.is_sorted());
        match self {
            ConstraintIterator::HasUnboundedSortedOwner(iter, _) => {
                iter.peek().map(|result| result.as_ref().map(|(has, count)| VariableValue::Thing(Thing::from(has.owner()))))
            }
            // ConstraintIterator::HasUnboundedSortedAttributeMulti(iter) => {}
            ConstraintIterator::HasUnboundedSortedAttributeSingle(iter, _) => {
                iter.peek().map(|result| result.as_ref().map(|(has, count)| VariableValue::Thing(Thing::Attribute(has.attribute()))))
            }
            ConstraintIterator::HasBoundedSortedAttribute(iter, _) => {
                iter.peek().map(|result| result.as_ref().map(|(has, count)| VariableValue::Thing(Thing::Attribute(has.attribute()))))
            }
        }
    }

    pub(crate) fn advance(&mut self) {
        assert!(self.has_value());
        match self {
            ConstraintIterator::HasUnboundedSortedOwner(iter, _) => {
                let (_, count) = iter.next().unwrap().unwrap();
                // TODO: how to handle multiple answers found in an iterator, eg (_, count > 1)?
                debug_assert!(count == 1)
            }
            ConstraintIterator::HasUnboundedSortedAttributeSingle(iter, _) => {
                let (_, count) = iter.next().unwrap().unwrap();
                debug_assert!(count == 1)
            }
            ConstraintIterator::HasBoundedSortedAttribute(iter, _) => {
                let (_, count) = iter.next().unwrap().unwrap();
                debug_assert!(count == 1)
            }
        }
    }

    pub(crate) fn write_values(&mut self, row: &mut Row) -> Result<(), &ConceptReadError> {
        debug_assert!(self.has_value());
        // TODO: how to handle multiple answers found in an iterator?
        match self {
            ConstraintIterator::HasUnboundedSortedOwner(iter, has) => {
                let (has_value, _): &(Has<'_>, u64) = iter.peek().unwrap().as_ref()?;
                row.set(has.owner(), VariableValue::Thing(Thing::from(has_value.owner().clone().into_owned())));
                row.set(has.attribute(), VariableValue::Thing(Thing::Attribute(has_value.attribute().clone().into_owned())));
                Ok(())
            }
            ConstraintIterator::HasUnboundedSortedAttributeSingle(iter, has) => {
                let (has_value, _): &(Has<'_>, u64) = iter.peek().unwrap().as_ref()?;
                row.set(has.owner(), VariableValue::Thing(Thing::from(has_value.owner().clone().into_owned())));
                row.set(has.attribute(), VariableValue::Thing(Thing::Attribute(has_value.attribute().clone().into_owned())));
                Ok(())
            }
            ConstraintIterator::HasBoundedSortedAttribute(iter, has) => {
                let (has_value, _): &(Has<'_>, u64) = iter.peek().unwrap().as_ref()?;
                debug_assert!(*row.get(has.owner()) == VariableValue::Thing(Thing::from(has_value.owner())));
                row.set(has.attribute(), VariableValue::Thing(Thing::Attribute(has_value.attribute().clone().into_owned())));
                Ok(())
            }
        }
    }

    fn has_value(&mut self) -> bool {
        match self {
            ConstraintIterator::HasUnboundedSortedOwner(iter, _) => {
                iter.peek().is_some()
            }
            ConstraintIterator::HasUnboundedSortedAttributeSingle(iter, _) => {
                iter.peek().is_some()
            }
            ConstraintIterator::HasBoundedSortedAttribute(iter, _) => {
                iter.peek().is_some()
            }
        }
    }

    const fn is_sorted(&self) -> bool {
        match self {
            ConstraintIterator::HasUnboundedSortedOwner(_, _)
            // | ConstraintIterator::HasUnboundedSortedAttributeMulti(_)
            | ConstraintIterator::HasUnboundedSortedAttributeSingle(_, _)
            | ConstraintIterator::HasBoundedSortedAttribute(_, _) => true,
        }
    }
}
