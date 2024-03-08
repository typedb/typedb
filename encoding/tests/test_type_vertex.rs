/*
 * Copyright (C) 2023 Vaticle
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */


use std::rc::Rc;

use encoding::initialise_storage;
use storage::MVCCStorage;
use test_utils::{create_tmp_dir, delete_dir, init_logging};

#[test]
fn entity_type_vertexes_are_reused() {
    init_logging();
    let storage_path = create_tmp_dir();
    let options = MVCCStorage::new_db_options();
    let mut storage = MVCCStorage::new(Rc::from("storage"), &storage_path).unwrap();
    initialise_storage(&mut storage);

    // TODO: create a bunch of types, delete, and assert that the IDs are re-used

    delete_dir(storage_path)
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


