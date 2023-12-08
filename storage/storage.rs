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

use speedb::{DB, DBCommon, DBIteratorWithThreadMode, DBWithThreadMode, Direction, Error, IteratorMode, Options, SingleThreaded, SstFileWriter, WriteBatch, WriteOptions};

use std::path::{Path};

struct Storage {
    path: Box<Path>,
    kv_storage: DB,
}

impl Storage {
    // fn new(path: Box<Path>) -> Self {
        // let kv_storage = DB::new();
        // Storage {
        //     path: path,
        //     kv_storage: kv_storage
        // }
    // }
}

struct Store {

}
