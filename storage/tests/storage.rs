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
 *
 */


use std::path::{Path, PathBuf};
use storage::{Storage};
use logger::{initialise_logging, error};

#[test]
fn create_delete() {
    initialise_logging();
    let path = PathBuf::from("/tmp/testing");
    let storage = Storage::new("create_new_storage_db", path);
    assert!(storage.is_ok());
    let delete = storage.unwrap().delete();
    assert!(delete.is_ok());
}
