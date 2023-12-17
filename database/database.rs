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
use logger::result::ResultExt;

use storage::Storage;
use encoding::thing::thing_encoding::{ThingEncoder};
use crate::transaction::{TransactionRead};

struct Database {
    name: Rc<str>,
    path: PathBuf,
    storage: Storage,
}

impl Database {
    fn new(path: &PathBuf, name: String) -> Database {
        let database_path = path.with_extension(&name);
        fs::create_dir(database_path.as_path());
        let database_name: Rc<str> = Rc::from(name);
        let mut storage = Storage::new(database_name.clone(), path)
            .unwrap_or_log(); // TODO we don't want to panic here

        let thing_encoder = ThingEncoder::new(&mut storage);

        Database {
            name: database_name,
            path: database_path,
            storage: storage,
        }
    }

    fn transaction_read(&self) -> TransactionRead {
        let snapshot = self.storage.snapshot_read();
        TransactionRead {
            snapshot: snapshot,
        }
    }
}


