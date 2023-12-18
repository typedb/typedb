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

use std::fs;
use std::path::PathBuf;
use std::rc::Rc;

use concept::type_manager::TypeManager;
use encoding::thing::thing_encoding::ThingEncoder;
use encoding::type_::id_generator::TypeIIDGenerator;
use logger::result::ResultExt;
use storage::snapshot::Snapshot;
use storage::Storage;

use crate::transaction::TransactionRead;

struct Database {
    name: Rc<str>,
    path: PathBuf,
    storage: Storage,
    type_iid_generator: TypeIIDGenerator,
}

impl Database {
    fn new(path: &PathBuf, name: String) -> Database {
        let database_path = path.with_extension(&name);
        fs::create_dir(database_path.as_path());
        let database_name: Rc<str> = Rc::from(name);
        let mut storage = Storage::new(database_name.clone(), path)
            .unwrap_or_log(); // TODO we don't want to panic here

        let type_iid_generator = TypeIIDGenerator::new(&storage);

        let thing_encoder = ThingEncoder::new(&mut storage);
        // let thing_encoder = TypeEncoder::new(&mut storage);

        Database {
            name: database_name,
            path: database_path,
            storage: storage,
            type_iid_generator: type_iid_generator,
        }
    }

    fn transaction_read(&self) -> TransactionRead {
        let mut snapshot: Rc<Snapshot<'_>> = Rc::new(Snapshot::Read(self.storage.snapshot_read()));
        let type_manager = TypeManager::new(snapshot.clone(), &self.type_iid_generator);
        TransactionRead {
            snapshot: snapshot,
            type_manager: type_manager,
        }
    }
}


