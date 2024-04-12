/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use durability::wal::WAL;
use encoding::{AsBytes, EncodingKeyspace, Keyable};
use encoding::error::{EncodingError, EncodingErrorKind};
use encoding::graph::type_::vertex::{build_vertex_entity_type, TypeID};
use encoding::graph::type_::vertex_generator::TypeVertexGenerator;
use storage::key_value::StorageKeyReference;
use storage::MVCCStorage;
use storage::snapshot::{CommittableSnapshot, WritableSnapshot};
use test_utils::{create_tmp_dir, init_logging};

#[test]
fn entity_type_vertexes_are_reused() {
    init_logging();
    let storage_path = create_tmp_dir();
    let mut storage = MVCCStorage::<WAL>::recover::<EncodingKeyspace>("storage", &storage_path).unwrap();

    // create a bunch of types, delete, and assert that the IDs are re-used
    let create_till = 255;
    {
        let snapshot = storage.open_snapshot_write();
        let generator = TypeVertexGenerator::new();
        for i in 0..=create_till {
            let vertex = generator.create_entity_type(&snapshot).unwrap();
            let b = vertex.bytes().into_bytes();
            let type_id_as_u16 = u16::from_be_bytes([b[b.len() - 2], b[b.len() - 1]]);
            assert_eq!(i, type_id_as_u16);
        }
        snapshot.commit().unwrap();
    }

    {
        let snapshot = storage.open_snapshot_write();
        for i in 0..=create_till {
            if i % 2 == 0 {
                let vertex = build_vertex_entity_type(TypeID::build(i));
                snapshot.delete(StorageKeyReference::new(vertex.keyspace(), vertex.bytes()).into())
            }
        }
        snapshot.commit().unwrap();
    }

    {
        let generator = TypeVertexGenerator::new();
        let snapshot = storage.open_snapshot_write();
        for i in 0..=create_till {
            if i % 2 == 0 {
                let vertex = generator.create_entity_type(&snapshot).unwrap();
                let b = vertex.bytes().into_bytes();
                let type_id_as_u16 = u16::from_be_bytes([b[b.len() - 2], b[b.len() - 1]]);
                assert_eq!(i, type_id_as_u16);
            }
        }
    }

}

#[test]
fn entity_type_vertex_reuse_resets_thing_vertex() {

    // TODO: when deleting an existing type vertex that had instances (which are deleted as well),
    //       and this type vertex ID is re-used, confirm that new Thing ID's start from 0 again for this type.
}

#[test]
fn max_entity_type_vertexes() {
    // TODO: test that the maximum number of type vertices for one group (entity type) is actually u16::MAX
    //       and that we throw a good error message if exceeded.
    use std::time::Instant;
    init_logging();
    let storage_path = create_tmp_dir();
    let mut storage = MVCCStorage::<WAL>::recover::<EncodingKeyspace>("storage", &storage_path).unwrap();
    let create_till = u16::MAX;
    {
        let snapshot = storage.open_snapshot_write();
        let generator = TypeVertexGenerator::new();
        for i in 0..=create_till {
            let vertex = generator.create_entity_type(&snapshot).unwrap();
            let b = vertex.bytes().into_bytes();
            let type_id_as_u16 = u16::from_be_bytes([b[b.len() - 2], b[b.len() - 1]]);
            assert_eq!(i, type_id_as_u16);
        }
        snapshot.commit().unwrap();
    }

    {
        let snapshot = storage.open_snapshot_write();
        let generator = TypeVertexGenerator::new();

        let res = generator.create_entity_type(&snapshot); // Crashes
        assert!(matches!(res, Err(EncodingError { kind: EncodingErrorKind::ExhaustedTypeIDs } )));
    }
}

#[test]
fn loading_storage_assigns_next_vertex() {
    // TODO: test that when loading an existing database (create database, create types, close database, then re-open)
    //       that the next Type vertex ID is the expected one

}
