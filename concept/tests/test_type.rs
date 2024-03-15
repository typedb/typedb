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


use std::ops::Deref;
use std::rc::Rc;
use std::sync::Arc;

use concept::type_::annotation::AnnotationAbstract;
use concept::type_::entity_type::EntityTypeAnnotation;
use concept::type_::EntityTypeAPI;
use concept::type_::type_cache::TypeCache;
use concept::type_::type_manager::TypeManager;
use encoding::create_keyspaces;
use encoding::graph::type_::Root;
use encoding::graph::type_::vertex_generator::TypeVertexGenerator;
use encoding::primitive::label::Label;
use storage::MVCCStorage;
use storage::snapshot::snapshot::Snapshot;
use test_utils::{create_tmp_dir, delete_dir, init_logging};

/*
This test is used to help develop the API of Types.
We don't aim for complete coverage of all APIs, and will rely on the BDD scenarios for coverage.
 */

#[test]
fn entity_creation() {
    init_logging();
    let storage_path = create_tmp_dir();
    let mut storage = MVCCStorage::new(Rc::from("storage"), &storage_path).unwrap();
    create_keyspaces(&mut storage).unwrap();
    let type_vertex_generator = TypeVertexGenerator::new();
    TypeManager::initialise_types(&mut storage, &type_vertex_generator);

    let mut snapshot: Rc<Snapshot<'_>> = Rc::new(Snapshot::Write(storage.open_snapshot_write()));
    {
        // Without cache, uncommitted
        let type_manager = TypeManager::new(snapshot.clone(), &type_vertex_generator, None);

        let root_entity = type_manager.get_entity_type(&Root::Entity.label()).unwrap();
        assert_eq!(root_entity.get_label(&type_manager).deref(), &Root::Entity.label());
        assert!(root_entity.is_root(&type_manager));

        // --- person sub entity @abstract ---
        let person_label = Label::build("person");
        let person_type = type_manager.create_entity_type(&person_label, false);
        person_type.set_annotation(&type_manager, EntityTypeAnnotation::from(AnnotationAbstract::new()));

        assert!(!person_type.is_root(&type_manager));
        assert!(person_type.get_annotations(&type_manager).contains(&EntityTypeAnnotation::from(AnnotationAbstract::new())));
        assert_eq!(person_type.get_label(&type_manager).deref(), &person_label);

        let supertype = person_type.get_supertype(&type_manager);
        assert_eq!(supertype.unwrap(), root_entity);

        // --- child sub person ---
        let child_label = Label::build("child");
        let child_type = type_manager.create_entity_type(&child_label, false);
        child_type.set_supertype(&type_manager, person_type.clone());

        assert!(!child_type.is_root(&type_manager));
        assert_eq!(child_type.get_label(&type_manager).deref(), &child_label);

        let supertype = child_type.get_supertype(&type_manager);
        assert_eq!(supertype.unwrap(), person_type);
        let supertypes = child_type.get_supertypes(&type_manager);
        assert_eq!(supertypes.len(), 2);
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

        let root_entity = type_manager.get_entity_type(&Root::Entity.label()).unwrap();
        assert_eq!(root_entity.get_label(&type_manager).deref(), &Root::Entity.label());
        assert!(root_entity.is_root(&type_manager));

        // --- person sub entity ---
        let person_label = Label::build("person");
        let person_type = type_manager.get_entity_type(&person_label).unwrap();
        assert!(!person_type.is_root(&type_manager));
        assert!(person_type.get_annotations(&type_manager).contains(&EntityTypeAnnotation::from(AnnotationAbstract::new())));
        assert_eq!(person_type.get_label(&type_manager).deref(), &person_label);

        let supertype = person_type.get_supertype(&type_manager);
        assert_eq!(supertype.unwrap(), root_entity);

        // --- child sub person ---
        let child_label = Label::build("child");
        let child_type = type_manager.get_entity_type(&child_label).unwrap();

        assert!(!child_type.is_root(&type_manager));
        assert_eq!(child_type.get_label(&type_manager).deref(), &child_label);

        let supertype = child_type.get_supertype(&type_manager);
        assert_eq!(supertype.unwrap(), person_type);
        let supertypes = child_type.get_supertypes(&type_manager);
        assert_eq!(supertypes.len(), 2);
    }

    delete_dir(storage_path)
}