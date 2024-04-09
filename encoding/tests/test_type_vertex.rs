/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use durability::wal::WAL;
use encoding::EncodingKeyspace;
use storage::MVCCStorage;
use test_utils::{create_tmp_dir, init_logging};

#[test]
fn entity_type_vertexes_are_reused() {
    init_logging();
    let storage_path = create_tmp_dir();
    #[allow(unused)] // TODO
    let mut storage = MVCCStorage::<WAL>::recover::<EncodingKeyspace>("storage", &storage_path).unwrap();

    // TODO: create a bunch of types, delete, and assert that the IDs are re-used
}

#[test]
fn entity_type_vertex_reuse_resets_thing_vertex() {

    // TODO: when deleting an existing type vertex that had instances (which are deleted as well),
    //       and this type vertex ID is re-used, confirm that new Thing ID's start from 0 again for this type.
}

#[test]
fn max_entity_type_vertexes() {

    // TODO: test that the maximum number of type vertices for one group (entity type) is actually U16.MAX_INT
    //       and that we throw a good error message if exceeded.
}

#[test]
fn loading_storage_assigns_next_vertex() {

    // TODO: test that when loading an existing database (create database, create types, close database, then re-open)
    //       that the next Type vertex ID is the expected one
}
