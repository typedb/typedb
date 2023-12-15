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

use std::fs;
use std::path::PathBuf;
use std::rc::Rc;
use storage::error::StorageError;
use storage::{Section, Storage};

struct Database {
    name: Rc<str>,
    path: PathBuf,
}

impl Database {
    fn new(path: &PathBuf, name: String) -> Database {
        let database_path = path.with_extension(&name);
        fs::create_dir(database_path.as_path());
        let storage = Database::create_storage(...);
        Database {
            name: Rc::from(name),
            path: database_path,
        }
    }

    fn create_storage(database_name: Rc<str>, path: &PathBuf) -> Result<Storage, StorageError> {
        let mut storage = Storage::new(database_name, path)?;
        let options = Section::new_options();
        // TODO: add expected key length
        storage.create_section("entity", 0x0, &options)?;
        storage.create_section("relation", 0x1, &options)?;
        storage.create_section("short_attribute", 0x2, &options)?;
        storage.create_section("long_attribute", 0x3, &options)?;
        storage.create_section("schema", 0xf0, &options)?;
        Ok(storage)
    }
}


