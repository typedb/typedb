/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#![deny(unused_must_use)]

use std::{rc::Rc, sync::Arc};

use bytes::{byte_array::ByteArray, Bytes};
use durability::wal::WAL;
use encoding::{
    graph::{
        definition::{
            definition_key::DefinitionKey, definition_key_generator::DefinitionKeyGenerator,
            r#struct::StructDefinition, DefinitionValueEncoding,
        },
        type_::index::NameToStructDefinitionIndex,
    },
    value::{string_bytes::StringBytes, value_type::ValueType},
    AsBytes, EncodingKeyspace, Keyable,
};
use resource::constants::snapshot::{BUFFER_KEY_INLINE, BUFFER_VALUE_INLINE};
use storage::{
    durability_client::WALClient,
    snapshot::{CommittableSnapshot, ReadableSnapshot, WritableSnapshot},
    MVCCStorage,
};
use test_utils::{create_tmp_dir, init_logging};

fn define_struct<Snapshot: WritableSnapshot>(
    snapshot: &mut Snapshot,
    definition_key_generator: &DefinitionKeyGenerator,
    definition: StructDefinition,
) -> DefinitionKey<'static> {
    let definition_key = definition_key_generator.create_struct(snapshot).unwrap();
    // Store definition
    snapshot.put_val(
        definition_key.clone().into_storage_key().into_owned_array(),
        definition.clone().into_bytes().unwrap().into_array(),
    );
    let index_key =
        NameToStructDefinitionIndex::build(StringBytes::<BUFFER_KEY_INLINE>::build_ref(definition.name.as_str()));
    snapshot.put_val(
        index_key.into_storage_key().into_owned_array(),
        ByteArray::copy(definition_key.clone().into_bytes().bytes()),
    );
    definition_key
}

fn get_struct_key(snapshot: &impl ReadableSnapshot, name: String) -> Option<DefinitionKey<'static>> {
    let index_key = NameToStructDefinitionIndex::build(StringBytes::<BUFFER_KEY_INLINE>::build_ref(name.as_str()));
    let bytes = snapshot.get(index_key.into_storage_key().as_reference()).unwrap();
    bytes.map(|value| DefinitionKey::new(Bytes::Array(value)))
}

fn get_struct_definition<'a>(snapshot: &impl ReadableSnapshot, definition_key: &DefinitionKey<'a>) -> StructDefinition {
    let bytes = snapshot.get::<BUFFER_VALUE_INLINE>(definition_key.clone().into_storage_key().as_reference()).unwrap();
    StructDefinition::from_bytes(bytes.unwrap().as_ref())
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

    let mut struct_0_definition = StructDefinition::new("struct_0".to_owned());
    struct_0_definition.add_field("f0_bool".to_owned(), ValueType::Boolean, false).unwrap();
    struct_0_definition.add_field("f1_long".to_owned(), ValueType::Long, false).unwrap();
    let struct_0_key = define_struct(&mut snapshot, &definition_key_generator, struct_0_definition.clone());

    let mut struct_1_definition = StructDefinition::new("struct_1".to_owned());
    struct_1_definition.add_field("f0_nested".into(), ValueType::Struct(struct_0_key), false).unwrap();
    define_struct(&mut snapshot, &definition_key_generator, struct_1_definition.clone());

    // Read back buffered
    {
        let read_0_key = get_struct_key(&snapshot, struct_0_definition.name.clone()).unwrap();
        let read_0_definition = get_struct_definition(&snapshot, &read_0_key);
        assert_eq!(struct_0_definition, read_0_definition);

        let read_1_key = get_struct_key(&snapshot, struct_1_definition.name.clone()).unwrap();
        let read_1_definition = get_struct_definition(&snapshot, &read_1_key);
        assert_eq!(struct_1_definition, read_1_definition);
    }
    snapshot.commit().unwrap();

    // Read back commmitted
    {
        let snapshot = storage.clone().open_snapshot_read();
        let read_0_key = get_struct_key(&snapshot, struct_0_definition.name.clone()).unwrap();
        let read_0_definition = get_struct_definition(&snapshot, &read_0_key);
        assert_eq!(struct_0_definition, read_0_definition);

        let read_1_key = get_struct_key(&snapshot, struct_1_definition.name.clone()).unwrap();
        let read_1_definition = get_struct_definition(&snapshot, &read_1_key);
        assert_eq!(struct_1_definition, read_1_definition);
        snapshot.close_resources();
    }
}
