/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;
use concept::thing::ObjectAPI;
use concept::type_::TypeAPI;
use durability::wal::WAL;
use encoding::{AsBytes, EncodingKeyspace, Keyable};
use encoding::error::{EncodingError, EncodingErrorKind};
use encoding::graph::thing::vertex_object::ObjectID;
use encoding::graph::type_::vertex::{build_vertex_entity_type, TypeID};
use encoding::graph::type_::vertex_generator::TypeVertexGenerator;
use storage::key_value::StorageKeyReference;
use storage::MVCCStorage;
use storage::snapshot::{CommittableSnapshot, WritableSnapshot};
use test_utils::{create_tmp_dir, init_logging};

use database::Database;
use database::transaction::TransactionWrite;
use encoding::graph::Typed;
use encoding::value::label::Label;

// TODO: Update all tests with higher level APIs
#[test]
fn entity_type_vertexes_are_reused() {
    init_logging();
    let storage_path = create_tmp_dir();
    let mut storage = Arc::new(MVCCStorage::<WAL>::recover::<EncodingKeyspace>("storage", &storage_path).unwrap());
    // If we don't commit, it doesn't move.
    {
        for i in 0..5 {
            let snapshot = storage.clone().open_snapshot_write();
            let generator = TypeVertexGenerator::new();
            let vertex = generator.create_entity_type(&snapshot).unwrap();
            assert_eq!(0, vertex.type_id_().as_u16());
        }
    }

    // create a bunch of types, delete, and assert that the IDs are re-used
    let create_till = 32;
    {
        let snapshot = storage.clone().open_snapshot_write();
        let generator = TypeVertexGenerator::new();
        for i in 0..=create_till {
            let vertex = generator.create_entity_type(&snapshot).unwrap();
            assert_eq!(i, vertex.type_id_().as_u16());
        }
        snapshot.commit().unwrap();
    }

    {
        let snapshot = storage.clone().open_snapshot_write();
        for i in 0..=create_till {
            if i % 2 == 0 {
                let vertex = build_vertex_entity_type(TypeID::build(i));
                snapshot.delete(StorageKeyReference::new(vertex.keyspace(), vertex.bytes()).into()); // TODO: replace with type api call.
            }
        }
        snapshot.commit().unwrap();
    }

    {
        let generator = TypeVertexGenerator::new();
        let snapshot = storage.clone().open_snapshot_write();
        for i in 0..=create_till {
            if i % 2 == 0 {
                let vertex = generator.create_entity_type(&snapshot).unwrap();
                assert_eq!(i, vertex.type_id_().as_u16());
            }
        }
    }

}

#[test]
fn entity_type_vertex_reuse_resets_thing_vertex() {
    init_logging();
    let storage_path = create_tmp_dir();
    let db_name = "encoding_tests__entity_type_vertex_reuse_resets_thing_vertex";
    let db: Arc<Database<WAL>> = Arc::new(Database::recover(&storage_path.join(db_name), &db_name).unwrap());

    let nthings = 5;

    let entity_type = {
        let tx = TransactionWrite::open(db.clone());
        let entity_type = tx.type_manager().create_entity_type(&Label::build("type_0"), false).unwrap();
        assert_eq!(1, entity_type.vertex().type_id_().as_u16()); // 1 because the root entity type exists.

        for i in 0..nthings {
            let entity = tx.thing_manager().create_entity(entity_type.clone()).unwrap();
            let expected_object_id = ObjectID::build(i as u64);
            assert_eq!(expected_object_id, entity.vertex().object_id());
        }
        tx.commit();
        entity_type
    };

    {
        let tx = TransactionWrite::open(db.clone());
        todo!(); // entity_type.delete(tx.type_manager()); // TODO: EntityType::delete is not implemented
        tx.commit();
    };

    {
        let tx = TransactionWrite::open(db.clone());
        let entity_type = tx.type_manager().create_entity_type(&Label::build("type_0"), false).unwrap();
        assert_eq!(1, entity_type.vertex().type_id_().as_u16()); // 1 because the root entity type exists.

        for i in 0..nthings {
            let entity = tx.thing_manager().create_entity(entity_type.clone()).unwrap();
            let thing_vertex = entity.vertex();
            let expected_object_id = ObjectID::build(i as u64);
            assert_eq!(expected_object_id, thing_vertex.object_id());
        }
        tx.commit();
        entity_type
    };
}

#[test]
fn max_entity_type_vertexes() {
    init_logging();
    let storage_path = create_tmp_dir();
    let mut storage = Arc::new(MVCCStorage::<WAL>::recover::<EncodingKeyspace>("storage", &storage_path).unwrap());
    let create_till = u16::MAX;
    {
        let snapshot = storage.clone().open_snapshot_write();
        let generator = TypeVertexGenerator::new();
        for i in 0..=create_till {
            let vertex = generator.create_entity_type(&snapshot).unwrap();
            assert_eq!(i, vertex.type_id_().as_u16());
        }
        snapshot.commit().unwrap();
    }

    {
        let snapshot = storage.clone().open_snapshot_write();
        let generator = TypeVertexGenerator::new();

        let res = generator.create_entity_type(&snapshot); // Crashes
        assert!(matches!(res, Err(EncodingError { kind: EncodingErrorKind::ExhaustedTypeIDs } ))); // TODO: We don't specify what type (entity) ran out of IDs
    }
}

#[test]
fn loading_storage_assigns_next_vertex() {
    init_logging();
    let storage_path = create_tmp_dir();
    let create_till = 5;

    for i in 0..=create_till {
        let mut storage = Arc::new(MVCCStorage::<WAL>::recover::<EncodingKeyspace>("storage", &storage_path).unwrap());
        let snapshot = storage.clone().open_snapshot_write();
        let generator = TypeVertexGenerator::new();

        let vertex = generator.create_entity_type(&snapshot).unwrap();
        assert_eq!(i, vertex.type_id_().as_u16());
        snapshot.commit().unwrap();
    }
}
