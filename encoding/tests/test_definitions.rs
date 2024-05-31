/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#![deny(unused_must_use)]

use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;
use bytes::byte_array::ByteArray;
use bytes::Bytes;
use durability::wal::WAL;
use encoding::{AsBytes, EncodingKeyspace, Keyable};
use encoding::graph::definition::definition_key::DefinitionKey;
use encoding::graph::definition::definition_key_generator::DefinitionKeyGenerator;
use encoding::graph::definition::DefinitionValueEncoding;
use encoding::graph::definition::r#struct::StructDefinition;
use encoding::graph::type_::index::LabelToStructDefinitionIndex;
use encoding::value::label::Label;
use encoding::value::value_type::ValueType;
use storage::durability_client::WALClient;
use storage::MVCCStorage;
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};
use test_utils::{create_tmp_dir, init_logging};
use resource::constants::snapshot::BUFFER_VALUE_INLINE;


fn create_struct<Snapshot: WritableSnapshot>(
    snapshot: &mut Snapshot,
    definition_key_generator: &DefinitionKeyGenerator,
    label: &Label<'static>,
    definition: StructDefinition
) -> DefinitionKey<'static>  {
    let definition_key = definition_key_generator.create_struct(snapshot).unwrap();
    // Store definition
    snapshot.put_val(definition_key.clone().into_storage_key().into_owned_array(), definition.to_bytes().unwrap().into_array());
    let index_key = LabelToStructDefinitionIndex::build(&label);
    snapshot.put_val(index_key.into_storage_key().into_owned_array(), ByteArray::copy(definition_key.clone().into_bytes().bytes()));
    definition_key
}

fn get_struct_key<Snapshot: ReadableSnapshot>(
    snapshot: &Snapshot,
    label: &Label<'static>
) -> Option<DefinitionKey<'static>> {
    let index_key = LabelToStructDefinitionIndex::build(&label);
    let bytes = snapshot.get(index_key.into_storage_key().as_reference()).unwrap();
    bytes.map(|value| DefinitionKey::new(Bytes::Array(value)))
}

fn get_struct_definition<'a, Snapshot: ReadableSnapshot>(
    snapshot: &Snapshot,
    definition_key: &DefinitionKey<'a>
) -> StructDefinition {
    let bytes = snapshot.get::<BUFFER_VALUE_INLINE>(definition_key.clone().into_storage_key().as_reference()).unwrap();
    StructDefinition::from_bytes(bytes.unwrap().as_ref())
}

fn struct_definitions_equal(first: &StructDefinition, second: &StructDefinition) -> bool {
    let mut all_match = true;
    all_match = all_match && first.field_names.len() == second.field_names.len() && first.fields.len() == second.fields.len();
    all_match = all_match && first.field_names.iter().all(|(k,v)| {
            second.field_names.contains_key(k) && v == second.field_names.get(k).unwrap()
        });
    all_match = all_match && std::iter::zip(first.fields.iter(), second.fields.iter()).all(|(f1,f2)|{
        f1.index == f2.index && f1.optional == f2.optional && f1.value_type == f2.value_type
    });
    all_match
}

#[test]
fn test_struct_definition() {
    init_logging();
    let storage_path = create_tmp_dir();
    let wal = WAL::create(&storage_path).unwrap();
    let storage = Arc::new(
        MVCCStorage::<WALClient>::create::<EncodingKeyspace>(Rc::from("storage"), &storage_path, WALClient::new(wal))
            .unwrap(),
    );
    let mut snapshot = storage.clone().open_snapshot_write();
    let definition_key_generator = DefinitionKeyGenerator::new();


    let struct_0_label = Label::build("struct_0");
    let struct_0_definition = StructDefinition::new(HashMap::from([
        ("f0_bool".into(), (ValueType::Boolean, false)),
        ("f1_long".into(), (ValueType::Long, false)),
    ]));
    let struct_0_key = create_struct(&mut snapshot, &definition_key_generator, &struct_0_label, struct_0_definition.clone());
    assert_eq!(0, struct_0_key.definition_id().as_uint());
    // Read back:
    let read_0_key = get_struct_key(&snapshot, &struct_0_label).unwrap();
    assert_eq!(struct_0_key.definition_id().as_uint(), read_0_key.definition_id().as_uint());
    let read_0_definition = get_struct_definition(&snapshot, &read_0_key);
    assert!(struct_definitions_equal(&struct_0_definition, &read_0_definition));

    let struct_1_label = Label::build("struct_1");
    let struct_1_definition = StructDefinition::new(HashMap::from([
        ("f0_nested".into(), (ValueType::Struct(struct_0_key), false)),
    ]));
    let struct_1_key = create_struct(&mut snapshot, &definition_key_generator, &struct_1_label, struct_1_definition.clone());
    assert_eq!(1, struct_1_key.definition_id().as_uint());
    let read_1_key = get_struct_key(&snapshot, &struct_1_label).unwrap();
    assert_eq!(struct_1_key.definition_id().as_uint(), read_1_key.definition_id().as_uint());
    let read_1_definition = get_struct_definition(&snapshot, &read_1_key);
    assert!(struct_definitions_equal(&struct_1_definition, &read_1_definition));
}

