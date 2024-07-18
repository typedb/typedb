/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::cmp::Ordering;

use answer::variable_value::VariableValue;
use concept::{
    error::ConceptReadError,
    thing::{attribute::AttributeIterator, entity::EntityIterator, relation::RelationIterator},
};
use lending_iterator::{LendingIterator, Peekable};

use crate::executor::instruction::has_executor::{
    HasBoundedSortedAttributeIterator, HasUnboundedSortedAttributeMergedIterator,
    HasUnboundedSortedAttributeSingleIterator, HasUnboundedSortedOwnerIterator,
};

macro_rules! counting_advance_to_thing_iterator {
    ($name: ident, $iterator_type: ty) => {
        pub(crate) fn $name(
            iterator: &mut Peekable<$iterator_type>,
            target: &VariableValue<'_>,
        ) -> Result<(usize, Option<Ordering>), ConceptReadError> {
            let mut count = 0;
            loop {
                let peek = iterator.peek();
                match peek {
                    None => return Ok((count, None)),
                    Some(Ok(value)) => {
                        let entity = VariableValue::Thing(value.as_reference().into());
                        let cmp = entity.partial_cmp(&target).unwrap();
                        match cmp {
                            Ordering::Less => {}
                            Ordering::Equal => return Ok((count, Some(Ordering::Equal))),
                            Ordering::Greater => return Ok((count, Some(Ordering::Greater))),
                        }
                    }
                    Some(Err(err)) => return Err(err.clone()),
                }
                let _ = iterator.next();
                count += 1;
            }
        }
    };
}

counting_advance_to_thing_iterator!(counting_advance_entity_iterator, EntityIterator);
counting_advance_to_thing_iterator!(counting_advance_relation_iterator, RelationIterator);
counting_advance_to_thing_iterator!(counting_advance_attribute_iterator, AttributeIterator);

macro_rules! counting_advance_to_has_iterator_owner {
    ($name: ident, $iterator_type: ty) => {
        pub(crate) fn $name(
            iterator: &mut Peekable<$iterator_type>,
            target: &VariableValue<'_>,
        ) -> Result<(usize, Option<Ordering>), ConceptReadError> {
            let mut count = 0;
            loop {
                let peek = iterator.peek();
                match peek {
                    None => return Ok((count, None)),
                    Some(Ok((has, has_count))) => {
                        // TODO: incorporate edge count
                        let owner = VariableValue::Thing(has.owner().into());
                        let cmp = owner.partial_cmp(&target).unwrap();
                        match cmp {
                            Ordering::Less => {}
                            Ordering::Equal => return Ok((count, Some(Ordering::Equal))),
                            Ordering::Greater => return Ok((count, Some(Ordering::Greater))),
                        }
                    }
                    Some(Err(err)) => return Err(err.clone()),
                }
                let _ = iterator.next();
                // TODO: incorporate edge count
                count += 1;
            }
        }
    };
}

counting_advance_to_has_iterator_owner!(
    counting_advance_has_unbounded_sorted_owner_iterator,
    HasUnboundedSortedOwnerIterator
);

macro_rules! counting_advance_to_has_iterator_attribute {
    ($name: ident, $iterator_type: ty) => {
        pub(crate) fn $name(
            iterator: &mut Peekable<$iterator_type>,
            target: &VariableValue<'_>,
        ) -> Result<(usize, Option<Ordering>), ConceptReadError> {
            let mut count = 0;
            loop {
                let peek = iterator.peek();
                match peek {
                    None => return Ok((count, None)),
                    Some(Ok((has, has_count))) => {
                        // TODO: incorporate edge count
                        let attribute = VariableValue::Thing(has.attribute().into());
                        let cmp = attribute.partial_cmp(&target).unwrap();
                        match cmp {
                            Ordering::Less => {}
                            Ordering::Equal => return Ok((count, Some(Ordering::Equal))),
                            Ordering::Greater => return Ok((count, Some(Ordering::Greater))),
                        }
                    }
                    Some(Err(err)) => return Err(err.clone()),
                }
                let _ = iterator.next();
                // TODO: incorporate edge count
                count += 1;
            }
        }
    };
}

counting_advance_to_has_iterator_owner!(
    counting_advance_has_unbounded_sorted_attribute_merged_iterator,
    HasUnboundedSortedAttributeMergedIterator
);

counting_advance_to_has_iterator_owner!(
    counting_advance_has_unbounded_sorted_attribute_single_iterator,
    HasUnboundedSortedAttributeSingleIterator
);
counting_advance_to_has_iterator_owner!(
    counting_advance_has_bounded_sorted_attribute_iterator,
    HasBoundedSortedAttributeIterator
);
