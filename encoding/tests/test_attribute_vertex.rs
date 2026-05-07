/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#![deny(unused_must_use)]

use std::sync::Arc;

use bytes::{Bytes, byte_array::ByteArray};
use diagnostics::metrics::FsyncMetrics;
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
use storage::{
    MVCCStorage, StorageCommitError,
    durability_client::WALClient,
    isolation_manager::IsolationConflict,
    snapshot::{CommittableSnapshot, SnapshotError},
};
use test_utils::{create_tmp_storage_dir, init_logging};
use test_utils_encoding::create_core_storage;
use test_utils_storage::create_rocks_resources;

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
fn attribute_id_deterministic_bytes() {
    let (_tmp_dir, storage) = create_core_storage();
    let mut snapshot = storage.clone().open_snapshot_write();
    let type_id = TypeID::new(0);

    // Inline value types and short strings (built via the public AttributeID::build_inline):
    // deterministic_bytes == bytes (the entire ID identifies the value).
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

    // Hashed strings, default hasher: deterministic_bytes drops the disambiguator byte.
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

    // Hashed strings, forced hash collision: deterministic_bytes are equal for distinct
    // values that share the prefix and (forced) hash, even though disambiguators differ.
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

    // Structs, forced collision: deterministic_bytes are equal across disambiguators.
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
fn same_string_in_two_concurrent_snapshots_produces_equal_deterministic_bytes() {
    // The unique/key commit-lock keys are derived from `AttributeID::deterministic_bytes()`. For
    // two concurrent transactions writing `has X "shared"` to land on the same lock, the
    // AttributeID computed independently by each snapshot for the same string value must produce
    // equal `deterministic_bytes`. This test pins that property at the encoding layer:
    // independently-opened snapshots, sharing only the underlying storage, must agree on the
    // deterministic prefix - both for the easy case (no prior collisions) and for the
    // forced-collision case where the disambiguator could otherwise diverge.
    let (_tmp_dir, storage) = create_core_storage();
    let type_id = TypeID::new(0);

    // Inline string: trivially equal (the entire ID is the value bytes).
    {
        let mut snapshot_a = storage.clone().open_snapshot_write();
        let mut snapshot_b = storage.clone().open_snapshot_write();
        let generator = ThingVertexGenerator::new();
        let s = "hi";
        let s_bytes: StringBytes<BUFFER_KEY_INLINE> = StringBytes::build_ref(s);
        let id_a = generator
            .create_attribute_string(type_id, s_bytes.as_reference(), &mut snapshot_a)
            .unwrap()
            .attribute_id()
            .unwrap_string();
        let id_b = generator
            .create_attribute_string(type_id, s_bytes.as_reference(), &mut snapshot_b)
            .unwrap()
            .attribute_id()
            .unwrap_string();
        assert_eq!(id_a.deterministic_bytes_ref(), id_b.deterministic_bytes_ref());
    }

    // Hashed identical strings, default hasher should have identical prefixes and hashes,
    // However, only one can commit to prevent races in setting the disambiguator
    {
        let mut snapshot_a = storage.clone().open_snapshot_write();
        let mut snapshot_b = storage.clone().open_snapshot_write();
        let generator = ThingVertexGenerator::new();
        let s = "Hello world, this is a long attribute string to be encoded.";
        let s_bytes: StringBytes<BUFFER_KEY_INLINE> = StringBytes::build_ref(s);
        let id_a = generator
            .create_attribute_string(type_id, s_bytes.as_reference(), &mut snapshot_a)
            .unwrap()
            .attribute_id()
            .unwrap_string();
        let id_b = generator
            .create_attribute_string(type_id, s_bytes.as_reference(), &mut snapshot_b)
            .unwrap()
            .attribute_id()
            .unwrap_string();
        assert!(!id_a.is_inline() && !id_b.is_inline());
        assert_eq!(id_a.deterministic_bytes_ref(), id_b.deterministic_bytes_ref());

        snapshot_a.commit(&mut CommitProfile::DISABLED).expect("First commit should succeed");
        match snapshot_b.commit(&mut CommitProfile::DISABLED) {
            Ok(_) => panic!("Expected hash collision to cause isolation error"),
            Err(err) => {
                assert!(matches!(
                    err,
                    SnapshotError::Commit {
                        typedb_source: StorageCommitError::Isolation { conflict: IsolationConflict::ExclusiveLock, .. }
                    }
                ));
            }
        }
    }

    // Forced hash collision: two snapshots, each independently inserts a different long string
    // sharing the same hash prefix. Each snapshot allocates disambiguator=0 (since neither sees
    // the other's pending writes). The full IDs may differ in disambiguator across runs, but
    // `deterministic_bytes_ref` strips that byte and must be equal.
    {
        const CONSTANT_HASH: u64 = 0;
        let generator = ThingVertexGenerator::new_with_hasher(|_bytes| CONSTANT_HASH);
        let mut snapshot_a = storage.clone().open_snapshot_write();
        let mut snapshot_b = storage.clone().open_snapshot_write();
        let s_a = "Hello world, this is using the same prefix - alpha branch.";
        let s_b = "Hello world, this is using the same prefix - bravo branch.";
        let s_a_bytes: StringBytes<BUFFER_KEY_INLINE> = StringBytes::build_ref(s_a);
        let s_b_bytes: StringBytes<BUFFER_KEY_INLINE> = StringBytes::build_ref(s_b);
        let id_a = generator
            .create_attribute_string(type_id, s_a_bytes.as_reference(), &mut snapshot_a)
            .unwrap()
            .attribute_id()
            .unwrap_string();
        let id_b = generator
            .create_attribute_string(type_id, s_b_bytes.as_reference(), &mut snapshot_b)
            .unwrap()
            .attribute_id()
            .unwrap_string();
        assert_eq!(
            id_a.deterministic_bytes_ref(),
            id_b.deterministic_bytes_ref(),
            "hash-collided strings encoded in independent snapshots must agree on deterministic_bytes"
        );

        snapshot_a.commit(&mut CommitProfile::DISABLED).expect("First commit should succeed");
        match snapshot_b.commit(&mut CommitProfile::DISABLED) {
            Ok(_) => panic!("Expected hash collision to cause isolation error"),
            Err(err) => {
                assert!(matches!(
                    err,
                    SnapshotError::Commit {
                        typedb_source: StorageCommitError::Isolation { conflict: IsolationConflict::ExclusiveLock, .. }
                    }
                ));
            }
        }
    }
}

#[test]
fn next_entity_and_relation_ids_are_determined_from_storage() {
    init_logging();
    let storage_path = create_tmp_storage_dir();
    let type_id = TypeID::new(0);
    {
        let wal = WAL::create(&storage_path, FsyncMetrics::disabled()).unwrap();
        let resources = create_rocks_resources();
        let storage = Arc::new(
            MVCCStorage::<WALClient>::create::<EncodingKeyspace>(
                "storage",
                &storage_path,
                WALClient::new(wal),
                &resources,
            )
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
        let wal = WAL::load(&storage_path, FsyncMetrics::disabled()).unwrap();
        let resources = create_rocks_resources();
        let storage = Arc::new(
            MVCCStorage::<WALClient>::load::<EncodingKeyspace>(
                "storage",
                &storage_path,
                WALClient::new(wal),
                &None,
                &resources,
            )
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
        let wal = WAL::load(&storage_path, FsyncMetrics::disabled()).unwrap();
        let resources = create_rocks_resources();
        let storage = Arc::new(
            MVCCStorage::<WALClient>::load::<EncodingKeyspace>(
                "storage",
                &storage_path,
                WALClient::new(wal),
                &None,
                &resources,
            )
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

#[test]
fn sync_from_storage_lifts_counters_to_match_storage() {
    init_logging();
    let storage_path = create_tmp_storage_dir();
    let type_id = TypeID::new(0);

    let stale_generator = {
        let wal = WAL::create(&storage_path, FsyncMetrics::disabled()).unwrap();
        let storage = Arc::new(
            MVCCStorage::<WALClient>::create::<EncodingKeyspace>("storage", &storage_path, WALClient::new(wal))
                .unwrap(),
        );
        let mut snapshot = storage.clone().open_snapshot_write();
        let type_generator = TypeVertexGenerator::new();
        type_generator.create_entity_type(&mut snapshot).unwrap();
        type_generator.create_relation_type(&mut snapshot).unwrap();
        snapshot.commit(&mut CommitProfile::DISABLED).unwrap();

        let primary_generator = ThingVertexGenerator::load(storage.clone()).unwrap();
        let mut snapshot = storage.clone().open_snapshot_write();
        for _ in 0..8 {
            primary_generator.create_entity(type_id, &mut snapshot);
        }
        for _ in 0..4 {
            primary_generator.create_relation(type_id, &mut snapshot);
        }
        snapshot.commit(&mut CommitProfile::DISABLED).unwrap();

        let stale_generator = ThingVertexGenerator::new();
        stale_generator.sync_from_storage(storage.clone()).unwrap();

        let mut snapshot = storage.clone().open_snapshot_write();
        for _ in 0..2 {
            primary_generator.create_entity(type_id, &mut snapshot);
        }
        for _ in 0..2 {
            primary_generator.create_relation(type_id, &mut snapshot);
        }
        snapshot.commit(&mut CommitProfile::DISABLED).unwrap();

        stale_generator
    };

    let wal = WAL::load(&storage_path, FsyncMetrics::disabled()).unwrap();
    let storage = Arc::new(
        MVCCStorage::<WALClient>::load::<EncodingKeyspace>("storage", &storage_path, WALClient::new(wal), &None)
            .unwrap(),
    );

    let mut snapshot = storage.clone().open_snapshot_write();
    let stale_entity = stale_generator.create_entity(type_id, &mut snapshot);
    assert_eq!(stale_entity.object_id().as_u64(), 8, "stale generator must refer to the partially counted set");
    let stale_relation = stale_generator.create_relation(type_id, &mut snapshot);
    assert_eq!(stale_relation.object_id().as_u64(), 4, "stale generator must refer to the partially counted sete");
    drop(snapshot);

    stale_generator.sync_from_storage(storage.clone()).unwrap();
    let stale_generator_2 = ThingVertexGenerator::new();
    stale_generator_2.sync_from_storage(storage.clone()).unwrap();

    let mut snapshot = storage.clone().open_snapshot_write();
    let next_entity = stale_generator.create_entity(type_id, &mut snapshot);
    assert_eq!(next_entity.object_id().as_u64(), 10, "next entity must skip past the 10 already in storage");
    let next_relation = stale_generator.create_relation(type_id, &mut snapshot);
    assert_eq!(next_relation.object_id().as_u64(), 6, "next relation must skip past the 6 already in storage");

    let next_entity = stale_generator_2.create_entity(type_id, &mut snapshot);
    assert_eq!(
        next_entity.object_id().as_u64(),
        10,
        "next entity must skip past the 10 already in storage for generator 2"
    );
    let next_relation = stale_generator_2.create_relation(type_id, &mut snapshot);
    assert_eq!(
        next_relation.object_id().as_u64(),
        6,
        "next relation must skip past the 6 already in storage for generator 2"
    );
}

#[test]
fn sync_from_storage_never_lowers_a_counter() {
    init_logging();
    let storage_path = create_tmp_storage_dir();
    let type_id = TypeID::new(0);

    let wal = WAL::create(&storage_path, FsyncMetrics::disabled()).unwrap();
    let storage = Arc::new(
        MVCCStorage::<WALClient>::create::<EncodingKeyspace>("storage", &storage_path, WALClient::new(wal)).unwrap(),
    );
    let mut snapshot = storage.clone().open_snapshot_write();
    let type_generator = TypeVertexGenerator::new();
    type_generator.create_entity_type(&mut snapshot).unwrap();
    snapshot.commit(&mut CommitProfile::DISABLED).unwrap();

    let generator = ThingVertexGenerator::load(storage.clone()).unwrap();
    // Commit 3 entities so storage has IIDs 0, 1, 2.
    let mut snapshot = storage.clone().open_snapshot_write();
    for _ in 0..3 {
        generator.create_entity(type_id, &mut snapshot);
    }
    snapshot.commit(&mut CommitProfile::DISABLED).unwrap();

    // Allocate another 5 in memory without committing — counter goes to 8.
    let mut snapshot = storage.clone().open_snapshot_write();
    for _ in 0..5 {
        generator.create_entity(type_id, &mut snapshot);
    }
    // Don't commit. The 5 extra IIDs are in-memory only; storage still holds 3.
    drop(snapshot);

    // Re-seed from storage — would set counter to 3 if it was broken
    generator.sync_from_storage(storage.clone()).unwrap();

    let mut snapshot = storage.clone().open_snapshot_write();
    let next = generator.create_entity(type_id, &mut snapshot);
    assert_eq!(next.object_id().as_u64(), 8, "counter must not be rolled back to storage's max");
}
