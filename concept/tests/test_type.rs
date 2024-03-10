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
use std::sync::Arc;

use concept::type_::{EntityTypeAPI, TypeAPI};
use concept::type_::type_cache::TypeCache;
use concept::type_::type_manager::TypeManager;
use encoding::create_keyspaces;
use encoding::graph::type_::Root;
use encoding::graph::type_::vertex_generator::TypeVertexGenerator;
use encoding::primitive::label::Label;
use storage::MVCCStorage;
use storage::snapshot::snapshot::Snapshot;
use test_utils::{create_tmp_dir, delete_dir, init_logging};


#[test]
fn entity_creation() {
    init_logging();
    let storage_path = create_tmp_dir();
    let mut storage = MVCCStorage::new(Rc::from("storage"), &storage_path).unwrap();
    create_keyspaces(&mut storage);
    let type_vertex_generator = TypeVertexGenerator::new();
    TypeManager::initialise_types(&mut storage, &type_vertex_generator);

    let mut snapshot: Rc<Snapshot<'_>> = Rc::new(Snapshot::Write(storage.open_snapshot_write()));
    {
        // Without cache, uncommitted
        let type_manager = TypeManager::new(snapshot.clone(), &type_vertex_generator, None);

        let root_entity = type_manager.get_entity_type(&Root::Entity.label());
        assert_eq!(root_entity.get_label(&type_manager), &Root::Entity.label());
        assert!(root_entity.is_root(&type_manager));

        let person_label = Label::build("person");
        let person_type = type_manager.create_entity_type(&person_label);

        assert!(!person_type.is_root(&type_manager));
        assert_eq!(person_type.get_label(&type_manager), &person_label);

        let supertype = person_type.get_supertype(&type_manager);
        assert_eq!(supertype, Some(root_entity));
    }
    if let Snapshot::Write(write_snapshot) = Rc::try_unwrap(snapshot).ok().unwrap() {
        write_snapshot.commit().unwrap();
    } else {
        unreachable!();
    }

    {
        // With cache, committed
        let snapshot: Rc<Snapshot<'_>> = Rc::new(Snapshot::Read(storage.open_snapshot_read()));
        let type_cache = Arc::new(TypeCache::new(&storage, snapshot.open_sequence_number()));
        let type_manager = TypeManager::new(snapshot.clone(), &type_vertex_generator, Some(type_cache));

        let root_entity = type_manager.get_entity_type(&Root::Entity.label());
        assert_eq!(root_entity.get_label(&type_manager), &Root::Entity.label());
        assert!(root_entity.is_root(&type_manager));

        let person_label = Label::build("person");
        let person_type = type_manager.get_entity_type(&person_label);
        assert!(!person_type.is_root(&type_manager));
        assert_eq!(person_type.get_label(&type_manager), &person_label);

        let supertype = person_type.get_supertype(&type_manager);
        assert_eq!(supertype, Some(root_entity));
    }

    delete_dir(storage_path)
}
