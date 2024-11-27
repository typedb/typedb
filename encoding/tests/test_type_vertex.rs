/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;

use durability::wal::WAL;
use encoding::{
    error::EncodingError,
    graph::{
        type_::{
            vertex::{PrefixedTypeVertexEncoding, TypeID, TypeVertex, TypeVertexEncoding},
            vertex_generator::TypeVertexGenerator,
        },
        Typed,
    },
    layout::prefix::Prefix,
    AsBytes, EncodingKeyspace, Keyable,
};
use storage::{
    durability_client::WALClient,
    key_value::StorageKeyReference,
    recovery::checkpoint::Checkpoint,
    snapshot::{CommittableSnapshot, WritableSnapshot},
    MVCCStorage,
};
use test_utils::{create_tmp_dir, init_logging};
use test_utils_encoding::create_core_storage;

pub struct MockEntityType {
    vertex: TypeVertex,
}

impl<'a> TypeVertexEncoding<'a> for MockEntityType {
    fn from_vertex(vertex: TypeVertex) -> Result<MockEntityType, EncodingError> {
        Ok(MockEntityType { vertex })
    }

    fn vertex(&self) -> TypeVertex {
        self.vertex
    }

    fn into_vertex(self) -> TypeVertex {
        self.vertex
    }
}

impl<'a> PrefixedTypeVertexEncoding<'a> for MockEntityType {
    const PREFIX: Prefix = Prefix::VertexEntityType;
}

// TODO: Update all tests with higher level APIs
#[test]
fn entity_type_vertexes_are_reused() {
    let (_tmp_dir, storage) = create_core_storage();

    // If we don't commit, it doesn't move.
    {
        for _ in 0..5 {
            let mut snapshot = storage.clone().open_snapshot_write();
            let generator = TypeVertexGenerator::new();
            let vertex = generator.create_entity_type(&mut snapshot).unwrap();
            assert_eq!(0, vertex.type_id_().as_u16());
        }
    }

    // create a bunch of types, delete, and assert that the IDs are re-used
    let create_till = 32;
    {
        let mut snapshot = storage.clone().open_snapshot_write();
        let generator = TypeVertexGenerator::new();
        for i in 0..=create_till {
            let vertex = generator.create_entity_type(&mut snapshot).unwrap();
            assert_eq!(i, vertex.type_id_().as_u16());
        }
        snapshot.commit().unwrap();
    }

    {
        let mut snapshot = storage.clone().open_snapshot_write();
        for i in 0..=create_till {
            if i % 2 == 0 {
                let vertex = MockEntityType::build_from_type_id(TypeID::build(i)).vertex;
                snapshot.delete(StorageKeyReference::new(vertex.keyspace(), &vertex.into_bytes()).into());
                // TODO: replace with type api call.
            }
        }
        snapshot.commit().unwrap();
    }

    {
        let generator = TypeVertexGenerator::new();
        let mut snapshot = storage.clone().open_snapshot_write();
        for i in 0..=create_till {
            if i % 2 == 0 {
                let vertex = generator.create_entity_type(&mut snapshot).unwrap();
                assert_eq!(i, vertex.type_id_().as_u16());
            }
        }
    }
}

#[test]
fn max_entity_type_vertexes() {
    let (_tmp_dir, storage) = create_core_storage();

    let create_till = u16::MAX;
    {
        let mut snapshot = storage.clone().open_snapshot_write();
        let generator = TypeVertexGenerator::new();
        for i in 0..=create_till {
            let vertex = generator.create_entity_type(&mut snapshot).unwrap();
            assert_eq!(i, vertex.type_id_().as_u16());
        }
        snapshot.commit().unwrap();
    }

    {
        let mut snapshot = storage.clone().open_snapshot_write();
        let generator = TypeVertexGenerator::new();

        let res = generator.create_entity_type(&mut snapshot); // Crashes
        assert!(matches!(res, Err(EncodingError::TypeIDsExhausted { kind: encoding::graph::type_::Kind::Entity })));
    }
}

#[test]
fn loading_storage_assigns_next_vertex() {
    init_logging();
    let storage_path = create_tmp_dir();
    {
        let wal = WAL::create(&storage_path).unwrap();
        let _ = Arc::new(
            MVCCStorage::<WALClient>::create::<EncodingKeyspace>("storage", &storage_path, WALClient::new(wal))
                .unwrap(),
        );
    }
    let create_till = 5;

    for i in 0..create_till {
        let wal = WAL::load(&storage_path).unwrap();
        let storage = Arc::new(
            MVCCStorage::<WALClient>::load::<EncodingKeyspace>("storage", &storage_path, WALClient::new(wal), &None)
                .unwrap(),
        );
        let mut snapshot = storage.clone().open_snapshot_write();
        let generator = TypeVertexGenerator::new();

        let vertex = generator.create_entity_type(&mut snapshot).unwrap();
        assert_eq!(i, vertex.type_id_().as_u16());
        snapshot.commit().unwrap();
    }

    for i in 0..create_till {
        let wal = WAL::load(&storage_path).unwrap();
        let storage = Arc::new(
            MVCCStorage::<WALClient>::load::<EncodingKeyspace>("storage", &storage_path, WALClient::new(wal), &None)
                .unwrap(),
        );
        let mut snapshot = storage.clone().open_snapshot_write();
        let generator = TypeVertexGenerator::new();

        let vertex = generator.create_attribute_type(&mut snapshot).unwrap();
        assert_eq!(i, vertex.type_id_().as_u16());
        snapshot.commit().unwrap();
    }

    // try with checkpoints
    let mut checkpoint = None;
    for i in 0..create_till {
        let wal = WAL::load(&storage_path).unwrap();
        let storage = match checkpoint {
            None => Arc::new(
                MVCCStorage::<WALClient>::load::<EncodingKeyspace>(
                    "storage",
                    &storage_path,
                    WALClient::new(wal),
                    &None,
                )
                .unwrap(),
            ),
            Some(checkpoint) => Arc::new(
                MVCCStorage::<WALClient>::load::<EncodingKeyspace>(
                    "storage",
                    &storage_path,
                    WALClient::new(wal),
                    &Some(checkpoint),
                )
                .unwrap(),
            ),
        };

        let mut snapshot = storage.clone().open_snapshot_write();
        let generator = TypeVertexGenerator::new();

        let vertex = generator.create_relation_type(&mut snapshot).unwrap();
        assert_eq!(i, vertex.type_id_().as_u16());
        snapshot.commit().unwrap();

        let check = Checkpoint::new(&storage_path).unwrap();
        storage.checkpoint(&check).unwrap();
        check.finish().unwrap();
        checkpoint = Some(check);
    }

    for i in 0..create_till {
        let wal = WAL::load(&storage_path).unwrap();
        let storage = Arc::new(
            MVCCStorage::<WALClient>::load::<EncodingKeyspace>("storage", &storage_path, WALClient::new(wal), &None)
                .unwrap(),
        );
        let mut snapshot = storage.clone().open_snapshot_write();
        let generator = TypeVertexGenerator::new();

        let vertex = generator.create_role_type(&mut snapshot).unwrap();
        assert_eq!(i, vertex.type_id_().as_u16());
        snapshot.commit().unwrap();
    }
}
