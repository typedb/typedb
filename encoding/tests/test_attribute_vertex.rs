/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#![deny(unused_must_use)]

use std::sync::Arc;

use bytes::{Bytes, byte_array::ByteArray};
use durability::wal::WAL;
use encoding::{
    EncodingKeyspace,
    graph::{
        Typed,
        thing::{
            vertex_attribute::{AttributeID, StringAttributeID, StructAttributeID},
            vertex_generator::ThingVertexGenerator,
        },
        type_::{vertex::TypeID, vertex_generator::TypeVertexGenerator},
    },
    value::{string_bytes::StringBytes, struct_bytes::StructBytes, value::Value},
};
use resource::{constants::snapshot::BUFFER_KEY_INLINE, profile::CommitProfile};
use storage::{MVCCStorage, durability_client::WALClient, snapshot::CommittableSnapshot};
use test_utils::{create_tmp_storage_dir, init_logging};
use test_utils_encoding::create_core_storage;

#[test]
fn generate_string_attribute_vertex() {
    let (_tmp_dir, storage) = create_core_storage();

    let mut snapshot = storage.clone().open_snapshot_write();
    let type_id = TypeID::new(0);

    let thing_vertex_generator = ThingVertexGenerator::new();

    // 1: vertex for short string that is stored inline
    {
        let short_string = "Hello";
        let short_string_bytes: StringBytes<BUFFER_KEY_INLINE> = StringBytes::build_ref(short_string);
        let vertex = thing_vertex_generator
            .create_attribute_string(type_id, short_string_bytes.as_reference(), &mut snapshot)
            .unwrap();
        let vertex_id = vertex.attribute_id().unwrap_string();
        assert!(vertex_id.is_inline());
        assert_eq!(vertex_id.get_inline_length() as usize, short_string_bytes.len());
        assert_eq!(vertex_id.get_inline_id_value().bytes(), short_string_bytes.bytes());
    }

    // 2: vertex for long string that does not exist beforehand with default hasher
    {
        let string = "Hello world, this is a long attribute string to be encoded.";
        let string_bytes: StringBytes<BUFFER_KEY_INLINE> = StringBytes::build_ref(string);
        let vertex = thing_vertex_generator
            .create_attribute_string(type_id, string_bytes.as_reference(), &mut snapshot)
            .unwrap();
        let vertex_id = vertex.attribute_id().unwrap_string();
        assert!(!vertex_id.is_inline());
        assert_eq!(vertex_id.get_hash_prefix(), string_bytes.bytes()[0..StringAttributeID::HASHED_PREFIX_LENGTH]);
        assert_eq!(
            vertex_id.get_hash_hash(),
            seahash::hash(string_bytes.bytes()).to_be_bytes()[0..StringAttributeID::HASHED_HASH_LENGTH]
        );
        assert_eq!(vertex_id.get_hash_disambiguator(), 0u8);
    }

    // 3. use a constant hasher to force collisions
    const CONSTANT_HASH: u64 = 0;
    let thing_vertex_generator = ThingVertexGenerator::new_with_hasher(|_bytes| CONSTANT_HASH);

    {
        let string = "Hello world, this is a long attribute string to be encoded with a constant hash.";
        let string_bytes: StringBytes<BUFFER_KEY_INLINE> = StringBytes::build_ref(string);
        let vertex = thing_vertex_generator
            .create_attribute_string(type_id, string_bytes.as_reference(), &mut snapshot)
            .unwrap();

        let vertex_id = vertex.attribute_id().unwrap_string();
        assert!(!vertex_id.is_inline());
        assert_eq!(vertex_id.get_hash_prefix(), string_bytes.bytes()[0..StringAttributeID::HASHED_PREFIX_LENGTH]);
        assert_eq!(vertex_id.get_hash_hash(), CONSTANT_HASH.to_be_bytes()[0..StringAttributeID::HASHED_HASH_LENGTH]);
        assert_eq!(vertex_id.get_hash_disambiguator(), 0u8);
        {
            let string_collide = "Hello world, this is using the same prefix and will collide.";
            let string_collide_bytes: StringBytes<BUFFER_KEY_INLINE> = StringBytes::build_ref(string_collide);
            let collide_vertex = thing_vertex_generator
                .create_attribute_string(type_id, string_collide_bytes.as_reference(), &mut snapshot)
                .unwrap();

            let collide_id = collide_vertex.attribute_id().unwrap_string();
            assert!(!collide_id.is_inline());
            assert_eq!(
                collide_id.get_hash_prefix(),
                string_collide_bytes.bytes()[0..StringAttributeID::HASHED_PREFIX_LENGTH]
            );
            assert_eq!(
                collide_id.get_hash_hash(),
                CONSTANT_HASH.to_be_bytes()[0..StringAttributeID::HASHED_HASH_LENGTH]
            );
            assert_eq!(collide_id.get_hash_disambiguator(), 1u8);
        }
        {
            let string_collide = "Hello world, this is using the same prefix and will collide AGAIN!.";
            let string_collide_bytes: StringBytes<BUFFER_KEY_INLINE> = StringBytes::build_ref(string_collide);
            let collide_vertex = thing_vertex_generator
                .create_attribute_string(type_id, string_collide_bytes.as_reference(), &mut snapshot)
                .unwrap();

            let collide_id = collide_vertex.attribute_id().unwrap_string();
            assert!(!collide_id.is_inline());
            assert_eq!(
                collide_id.get_hash_prefix(),
                string_collide_bytes.bytes()[0..StringAttributeID::HASHED_PREFIX_LENGTH]
            );
            assert_eq!(
                collide_id.get_hash_hash(),
                CONSTANT_HASH.to_be_bytes()[0..StringAttributeID::HASHED_HASH_LENGTH]
            );
            assert_eq!(collide_id.get_hash_disambiguator(), 2u8);
        }
    }
}

#[test]
fn generate_struct_attribute_vertex() {
    let (_tmp_dir, storage) = create_core_storage();

    let mut snapshot = storage.clone().open_snapshot_write();
    let type_id = TypeID::new(0);

    let thing_vertex_generator = ThingVertexGenerator::new();

    // 1. vertex for long string that does not exist beforehand with default hasher
    {
        let struct_bytes_raw: [u8; 4] = [1, 2, 3, 4];
        let struct_bytes: StructBytes<'_, BUFFER_KEY_INLINE> =
            StructBytes::new(Bytes::Array(ByteArray::copy(&struct_bytes_raw)));
        let vertex = thing_vertex_generator
            .create_attribute_struct(type_id, struct_bytes.as_reference(), &mut snapshot)
            .unwrap();
        let vertex_id = vertex.attribute_id().unwrap_struct();
        assert_eq!(
            vertex_id.get_hash_hash(),
            seahash::hash(struct_bytes.bytes()).to_be_bytes()[0..StructAttributeID::HASH_LENGTH]
        );
        assert_eq!(vertex_id.get_hash_disambiguator(), 0u8);
    }

    // 2. use a constant hasher to force collisions
    const CONSTANT_HASH: u64 = 0;
    let thing_vertex_generator = ThingVertexGenerator::new_with_hasher(|_bytes| CONSTANT_HASH);

    {
        let struct_bytes_raw: [u8; 4] = [5, 6, 7, 8];
        let struct_bytes: StructBytes<'_, BUFFER_KEY_INLINE> =
            StructBytes::new(Bytes::Array(ByteArray::copy(&struct_bytes_raw)));
        let vertex = thing_vertex_generator
            .create_attribute_struct(type_id, struct_bytes.as_reference(), &mut snapshot)
            .unwrap();
        let vertex_id = vertex.attribute_id().unwrap_struct();
        assert_eq!(vertex_id.get_hash_hash(), CONSTANT_HASH.to_be_bytes()[0..StructAttributeID::HASH_LENGTH]);
        assert_eq!(vertex_id.get_hash_disambiguator(), 0u8);
        {
            let struct_collide_raw: [u8; 4] = [9, 10, 11, 12];
            let struct_collide_bytes: StructBytes<'_, BUFFER_KEY_INLINE> =
                StructBytes::new(Bytes::Array(ByteArray::copy(&struct_collide_raw)));
            let collide_vertex = thing_vertex_generator
                .create_attribute_struct(type_id, struct_collide_bytes.as_reference(), &mut snapshot)
                .unwrap();

            let collide_id = collide_vertex.attribute_id().unwrap_struct();
            assert_eq!(collide_id.get_hash_hash(), CONSTANT_HASH.to_be_bytes()[0..StructAttributeID::HASH_LENGTH]);
            assert_eq!(collide_id.get_hash_disambiguator(), 1u8);
        }
        assert_eq!(vertex_id.get_hash_disambiguator(), 0u8);
        {
            let struct_collide_raw: [u8; 4] = [13, 14, 15, 16];
            let struct_collide_bytes: StructBytes<'_, BUFFER_KEY_INLINE> =
                StructBytes::new(Bytes::Array(ByteArray::copy(&struct_collide_raw)));
            let collide_vertex = thing_vertex_generator
                .create_attribute_struct(type_id, struct_collide_bytes.as_reference(), &mut snapshot)
                .unwrap();

            let collide_id = collide_vertex.attribute_id().unwrap_struct();
            assert_eq!(collide_id.get_hash_hash(), CONSTANT_HASH.to_be_bytes()[0..StructAttributeID::HASH_LENGTH]);
            assert_eq!(collide_id.get_hash_disambiguator(), 2u8);
        }
    }
}

#[test]
fn attribute_id_deterministic_bytes_strips_disambiguator_for_hashed_ids() {
    // For inlineable value types (boolean, integer, ..., inline strings, ...) deterministic_bytes()
    // must equal bytes() - the entire ID identifies the value.
    //
    // For hashed types (long strings, structs) deterministic_bytes() must drop the trailing
    // disambiguator byte so that different hash-collided values produce equal lock keys. This is
    // load-bearing for the unique-constraint commit lock - if disambiguator were included, two
    // concurrent writes of two different values with equal hashes wouldn't contend on the same
    // lock and could both pass through.

    let (_tmp_dir, storage) = create_core_storage();
    let mut snapshot = storage.clone().open_snapshot_write();
    let type_id = TypeID::new(0);

    // 1. Inline value types and short strings (built via the public AttributeID::build_inline):
    //    deterministic_bytes == bytes (the entire ID identifies the value).
    {
        let inline_cases = [
            Value::Boolean(true),
            Value::Boolean(false),
            Value::Integer(42),
            Value::Integer(-1),
            Value::Double(3.14),
            Value::String(std::borrow::Cow::Borrowed("hi")),
        ];
        for value in inline_cases {
            let id = AttributeID::build_inline(value.as_reference());
            assert_eq!(
                id.deterministic_bytes(),
                id.bytes(),
                "inline value {:?} must have deterministic_bytes == bytes",
                value
            );
        }
        // The short-string case must hit the inline branch of StringAttributeID.
        let short_id = AttributeID::build_inline(Value::String(std::borrow::Cow::Borrowed("hi")));
        assert!(short_id.unwrap_string().is_inline());
    }

    // 3. Hashed strings, default hasher: deterministic_bytes drops the disambiguator byte.
    {
        let long = "Hello world, this is a long attribute string to be encoded.";
        let long_bytes: StringBytes<BUFFER_KEY_INLINE> = StringBytes::build_ref(long);
        let vertex = ThingVertexGenerator::new()
            .create_attribute_string(type_id, long_bytes.as_reference(), &mut snapshot)
            .unwrap();
        let id = vertex.attribute_id().unwrap_string();
        assert!(!id.is_inline());
        assert_eq!(id.deterministic_bytes_ref().len(), StringAttributeID::HASHED_HASH_RANGE.end);
        assert_eq!(id.deterministic_bytes_ref(), &id.bytes_ref()[..StringAttributeID::HASHED_HASH_RANGE.end]);
        // The trailing disambiguator byte must be excluded.
        assert!(id.deterministic_bytes_ref().len() < id.bytes_ref().len());
    }

    // 4. Hashed strings, forced hash collision: deterministic_bytes are equal for distinct
    //    values that share the prefix and (forced) hash, even though disambiguators differ.
    {
        const CONSTANT_HASH: u64 = 0;
        let generator = ThingVertexGenerator::new_with_hasher(|_bytes| CONSTANT_HASH);

        let s_a = "Hello world, this is using the same prefix - alpha branch.";
        let s_b = "Hello world, this is using the same prefix - bravo branch.";
        let s_a_bytes: StringBytes<BUFFER_KEY_INLINE> = StringBytes::build_ref(s_a);
        let s_b_bytes: StringBytes<BUFFER_KEY_INLINE> = StringBytes::build_ref(s_b);
        // Sanity: prefixes are equal so the hashed IDs share everything but the disambiguator.
        assert_eq!(
            s_a_bytes.bytes()[..StringAttributeID::HASHED_PREFIX_LENGTH],
            s_b_bytes.bytes()[..StringAttributeID::HASHED_PREFIX_LENGTH]
        );

        let id_a = generator
            .create_attribute_string(type_id, s_a_bytes.as_reference(), &mut snapshot)
            .unwrap()
            .attribute_id()
            .unwrap_string();
        let id_b = generator
            .create_attribute_string(type_id, s_b_bytes.as_reference(), &mut snapshot)
            .unwrap()
            .attribute_id()
            .unwrap_string();
        assert_ne!(id_a.get_hash_disambiguator(), id_b.get_hash_disambiguator());
        assert_ne!(id_a.bytes_ref(), id_b.bytes_ref());
        assert_eq!(
            id_a.deterministic_bytes_ref(),
            id_b.deterministic_bytes_ref(),
            "hash-collided strings with equal prefixes must produce equal deterministic_bytes"
        );
    }

    // 5. Hashed strings, distinct hashes: deterministic_bytes differ.
    {
        let s_a = "Long alpha string used to distinguish hashes - branch A.";
        let s_b = "Different long bravo content for a different hash - branch B.";
        let s_a_bytes: StringBytes<BUFFER_KEY_INLINE> = StringBytes::build_ref(s_a);
        let s_b_bytes: StringBytes<BUFFER_KEY_INLINE> = StringBytes::build_ref(s_b);
        let generator = ThingVertexGenerator::new();
        let id_a = generator
            .create_attribute_string(type_id, s_a_bytes.as_reference(), &mut snapshot)
            .unwrap()
            .attribute_id()
            .unwrap_string();
        let id_b = generator
            .create_attribute_string(type_id, s_b_bytes.as_reference(), &mut snapshot)
            .unwrap()
            .attribute_id()
            .unwrap_string();
        assert_ne!(id_a.deterministic_bytes_ref(), id_b.deterministic_bytes_ref());
    }

    // 6. Structs, forced collision: deterministic_bytes are equal across disambiguators.
    {
        const CONSTANT_HASH: u64 = 0;
        let generator = ThingVertexGenerator::new_with_hasher(|_bytes| CONSTANT_HASH);

        let raw_a: [u8; 4] = [1, 2, 3, 4];
        let raw_b: [u8; 4] = [9, 8, 7, 6];
        let bytes_a: StructBytes<'_, BUFFER_KEY_INLINE> = StructBytes::new(Bytes::Array(ByteArray::copy(&raw_a)));
        let bytes_b: StructBytes<'_, BUFFER_KEY_INLINE> = StructBytes::new(Bytes::Array(ByteArray::copy(&raw_b)));

        let id_a = generator
            .create_attribute_struct(type_id, bytes_a.as_reference(), &mut snapshot)
            .unwrap()
            .attribute_id()
            .unwrap_struct();
        let id_b = generator
            .create_attribute_struct(type_id, bytes_b.as_reference(), &mut snapshot)
            .unwrap()
            .attribute_id()
            .unwrap_struct();
        assert_ne!(id_a.get_hash_disambiguator(), id_b.get_hash_disambiguator());
        assert_ne!(id_a.bytes_ref(), id_b.bytes_ref());
        assert_eq!(
            id_a.deterministic_bytes_ref(),
            id_b.deterministic_bytes_ref(),
            "hash-collided structs must produce equal deterministic_bytes"
        );
        assert_eq!(id_a.deterministic_bytes_ref().len(), id_a.bytes_ref().len() - 1);
    }
}

#[test]
fn next_entity_and_relation_ids_are_determined_from_storage() {
    init_logging();
    let storage_path = create_tmp_storage_dir();
    let type_id = TypeID::new(0);
    {
        let wal = WAL::create(&storage_path).unwrap();
        let storage = Arc::new(
            MVCCStorage::<WALClient>::create::<EncodingKeyspace>("storage", &storage_path, WALClient::new(wal))
                .unwrap(),
        );
        let mut snapshot = storage.clone().open_snapshot_write();
        let generator = TypeVertexGenerator::new();

        let entity_type_vertex = generator.create_entity_type(&mut snapshot).unwrap();
        debug_assert_eq!(type_id, entity_type_vertex.type_id_());

        let relation_type_vertex = generator.create_relation_type(&mut snapshot).unwrap();
        debug_assert_eq!(type_id, relation_type_vertex.type_id_());

        snapshot.commit(&mut CommitProfile::DISABLED).unwrap();
    }

    for i in 0..5 {
        let wal = WAL::load(&storage_path).unwrap();
        let storage = Arc::new(
            MVCCStorage::<WALClient>::load::<EncodingKeyspace>("storage", &storage_path, WALClient::new(wal), &None)
                .unwrap(),
        );
        let mut snapshot = storage.clone().open_snapshot_write();
        let generator = ThingVertexGenerator::load(storage.clone()).unwrap();
        let vertex = generator.create_entity(type_id, &mut snapshot);
        assert_eq!(type_id, vertex.type_id_());
        assert_eq!(i as u64, vertex.object_id().as_u64());
        snapshot.commit(&mut CommitProfile::DISABLED).unwrap();
    }

    for i in 0..5 {
        let wal = WAL::load(&storage_path).unwrap();
        let storage = Arc::new(
            MVCCStorage::<WALClient>::load::<EncodingKeyspace>("storage", &storage_path, WALClient::new(wal), &None)
                .unwrap(),
        );
        let mut snapshot = storage.clone().open_snapshot_write();
        let generator = ThingVertexGenerator::load(storage.clone()).unwrap();
        let vertex = generator.create_relation(type_id, &mut snapshot);
        assert_eq!(type_id, vertex.type_id_());
        assert_eq!(i as u64, vertex.object_id().as_u64());
        snapshot.commit(&mut CommitProfile::DISABLED).unwrap();
    }
}
