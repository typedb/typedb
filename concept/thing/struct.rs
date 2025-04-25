/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::cmp::Ordering;

use encoding::{
    graph::{thing::vertex_generator::ThingVertexGenerator, type_::vertex::TypeVertexEncoding},
    layout::prefix::Prefix,
    value::{
        value::Value,
        value_struct::{StructIndexEntry, StructIndexEntryKey},
    },
};
use lending_iterator::{LendingIterator, Peekable, Seekable};
use resource::{
    constants::{encoding::StructFieldIDUInt, snapshot::BUFFER_KEY_INLINE},
    profile::StorageCounters,
};
use storage::{
    key_range::KeyRange,
    key_value::StorageKey,
    snapshot::{iterator::SnapshotRangeIterator, ReadableSnapshot},
};

use crate::{
    error::ConceptReadError,
    thing::{attribute::Attribute, ThingAPI},
    type_::attribute_type::AttributeType,
};

pub struct StructIndexForAttributeTypeIterator {
    prefix: StorageKey<'static, { BUFFER_KEY_INLINE }>,
    iterator: SnapshotRangeIterator,
}

impl StructIndexForAttributeTypeIterator {
    pub(crate) fn new(
        snapshot: &impl ReadableSnapshot,
        vertex_generator: &ThingVertexGenerator,
        attribute_type: AttributeType,
        path_to_field: &[StructFieldIDUInt],
        value: Value<'_>,
    ) -> Result<Self, Box<ConceptReadError>> {
        let prefix = StructIndexEntry::build_prefix_typeid_path_value(
            snapshot,
            vertex_generator,
            path_to_field,
            &value,
            &attribute_type.vertex(),
        )
        .map_err(|source| Box::new(ConceptReadError::SnapshotIterate { source }))?;
        let iterator = snapshot.iterate_range(
            &KeyRange::new_within(prefix.clone(), Prefix::IndexValueToStruct.fixed_width_keys()),
            StorageCounters::DISABLED,
        );
        Ok(Self { prefix, iterator })
    }
}

impl LendingIterator for StructIndexForAttributeTypeIterator {
    type Item<'a> = Result<Attribute, Box<ConceptReadError>>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        self.iterator.next().map(|result| {
            result
                .map(|(key, _)| {
                    Attribute::new(
                        StructIndexEntry::new(StructIndexEntryKey::new(key.into_bytes()), None).attribute_vertex(),
                    )
                })
                .map_err(|err| Box::new(ConceptReadError::SnapshotIterate { source: err }))
        })
    }
}

impl Seekable<Attribute> for Peekable<StructIndexForAttributeTypeIterator> {
    fn seek(&mut self, target: &Attribute) {
        // can we guarantee that the PATH is complete and that we will therefore generate in-order attributes?
        // use simple looping seek for now...
        while let Some(Ok(attribute)) = self.peek() {
            if attribute.cmp(target) == Ordering::Less {
                continue;
            } else {
                break;
            }
        }
    }

    fn compare_key(&self, attribute: &Self::Item<'_>, other_attribute: &Attribute) -> Ordering {
        if let Ok(attribute) = attribute {
            attribute.cmp(other_attribute)
        } else {
            // arbitrarily choose equal
            Ordering::Equal
        }
    }
}
