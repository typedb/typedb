/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    sync::Arc,
    vec,
};

use answer::{Thing, Type, variable_value::VariableValue};
use compiler::{
    ExecutorVariable,
    executable::match_::instructions::{Inputs, VariableMode, VariableModes, thing::HasOrderedInstruction},
};
use concept::{
    error::ConceptReadError,
    thing::object::{Object, ObjectAPI},
};
use lending_iterator::LendingIterator;
use resource::profile::StorageCounters;
use storage::snapshot::ReadableSnapshot;

use crate::{
    instruction::{
        iterator::{NaiiveSeekable, SortedTupleIterator, TupleIterator},
        tuple::{Tuple, TuplePositions, TupleResult},
    },
    pipeline::stage::ExecutionContext,
    row::MaybeOwnedRow,
};

#[derive(Debug)]
pub(crate) struct HasOrderedExecutor {
    has: ir::pattern::constraint::Has<ExecutorVariable>,
    inputs: Inputs<ExecutorVariable>,
    variable_modes: VariableModes,
    tuple_positions: TuplePositions,
    owner_attribute_types: Arc<BTreeMap<Type, Vec<Type>>>,
    attribute_types: Arc<BTreeSet<Type>>,
}

pub(crate) type HasOrderedTupleIterator = NaiiveSeekable<HasOrderedIterator>;

impl HasOrderedExecutor {
    pub(crate) fn new(
        has: HasOrderedInstruction<ExecutorVariable>,
        variable_modes: VariableModes,
    ) -> Result<Self, Box<ConceptReadError>> {
        debug_assert!(!variable_modes.all_inputs());
        debug_assert!(has.checks.is_empty());
        let owner_attribute_types = has.owner_to_attribute_types().clone();
        let attribute_types = has.attribute_types().clone();
        let HasOrderedInstruction { has, inputs, .. } = has;
        let owner = has.owner().as_variable().unwrap();
        let attribute = has.attribute().as_variable().unwrap();
        let owner_is_input = variable_modes.get(owner) == Some(VariableMode::Input);
        let attribute_is_input = variable_modes.get(attribute) == Some(VariableMode::Input);
        let tuple_positions = match (owner_is_input, attribute_is_input) {
            (true, false) => TuplePositions::Single([Some(attribute)]),
            (false, true) => TuplePositions::Single([Some(owner)]),
            (false, false) => TuplePositions::Pair([Some(owner), Some(attribute)]),
            (true, true) => unreachable!("ordered has should be planned as a check when both variables are inputs"),
        };
        Ok(Self { has, inputs, variable_modes, tuple_positions, owner_attribute_types, attribute_types })
    }

    pub(crate) fn get_iterator(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: MaybeOwnedRow<'_>,
        storage_counters: StorageCounters,
    ) -> Result<TupleIterator, Box<ConceptReadError>> {
        let mut tuples = Vec::new();
        let owner_var = self.has.owner().as_variable().unwrap();
        let attribute_var = self.has.attribute().as_variable().unwrap();
        let owner_is_input = self.is_input(owner_var);
        let attribute_is_input = self.is_input(attribute_var);

        if owner_is_input {
            let owner_pos = owner_var.as_position().unwrap();
            let owner = row.get(owner_pos).as_thing().as_object();
            self.push_owner_tuples(context, &row, owner, attribute_is_input, &mut tuples, storage_counters)?;
        } else {
            for owner_type in self.owner_attribute_types.keys().map(|type_| type_.as_object_type()) {
                let objects = context.thing_manager.get_objects_in(
                    context.snapshot.as_ref(),
                    owner_type,
                    storage_counters.clone(),
                );
                for object in objects {
                    let object = object?;
                    self.push_owner_tuples(
                        context,
                        &row,
                        object,
                        attribute_is_input,
                        &mut tuples,
                        storage_counters.clone(),
                    )?;
                }
            }
        }

        tuples.sort_by(|left, right| match (left, right) {
            (Ok(left), Ok(right)) => left.partial_cmp(right).unwrap_or(std::cmp::Ordering::Equal),
            _ => std::cmp::Ordering::Equal,
        });
        let iterator = NaiiveSeekable::new(HasOrderedIterator::new(tuples));
        Ok(TupleIterator::HasOrdered(SortedTupleIterator::new(
            iterator,
            self.tuple_positions.clone(),
            &self.variable_modes,
        )))
    }

    fn push_owner_tuples(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: &MaybeOwnedRow<'_>,
        owner: Object,
        attribute_is_input: bool,
        tuples: &mut Vec<TupleResult<'static>>,
        storage_counters: StorageCounters,
    ) -> Result<(), Box<ConceptReadError>> {
        let Some(attribute_types) = self.owner_attribute_types.get(&Type::from(owner.type_())) else {
            return Ok(());
        };
        for attribute_type in attribute_types {
            let attribute_type = attribute_type.as_attribute_type();
            if !self.attribute_types.contains(&Type::Attribute(attribute_type)) {
                continue;
            }
            let attributes = owner.get_has_type_ordered(
                context.snapshot.as_ref(),
                context.thing_manager.as_ref(),
                attribute_type,
                storage_counters.clone(),
            )?;
            if attributes.is_empty() {
                continue;
            }
            let things = attributes.into_iter().map(Thing::Attribute).collect::<Arc<[_]>>();
            if attribute_is_input && !self.attribute_input_matches(row, &things) {
                continue;
            }
            tuples.push(Ok(self.output_tuple(owner, things)));
        }
        Ok(())
    }

    fn attribute_input_matches(&self, row: &MaybeOwnedRow<'_>, attributes: &Arc<[Thing]>) -> bool {
        let attribute_var = self.has.attribute().as_variable().unwrap();
        let attribute_pos = attribute_var.as_position().unwrap();
        matches!(row.get(attribute_pos), VariableValue::ThingList(input) if input == attributes)
    }

    fn output_tuple(&self, owner: Object, attributes: Arc<[Thing]>) -> Tuple<'static> {
        let owner_var = self.has.owner().as_variable().unwrap();
        let attribute_var = self.has.attribute().as_variable().unwrap();
        let owner_is_input = self.is_input(owner_var);
        let attribute_is_input = self.is_input(attribute_var);
        let owner = VariableValue::Thing(Thing::from(owner));
        let attributes = VariableValue::ThingList(attributes);
        match (owner_is_input, attribute_is_input) {
            (true, false) => Tuple::Single([attributes]),
            (false, true) => Tuple::Single([owner]),
            (false, false) => Tuple::Pair([owner, attributes]),
            (true, true) => unreachable!("ordered has should be planned as a check when both variables are inputs"),
        }
    }

    fn is_input(&self, variable: ExecutorVariable) -> bool {
        self.inputs.iter().any(|&input| input == variable)
    }
}

impl fmt::Display for HasOrderedExecutor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "[{}], mode=ordered", &self.has)
    }
}

pub(crate) struct HasOrderedIterator {
    inner: vec::IntoIter<TupleResult<'static>>,
}

impl HasOrderedIterator {
    fn new(tuples: Vec<TupleResult<'static>>) -> Self {
        Self { inner: tuples.into_iter() }
    }
}

impl LendingIterator for HasOrderedIterator {
    type Item<'a> = TupleResult<'static>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        self.inner.next()
    }
}
