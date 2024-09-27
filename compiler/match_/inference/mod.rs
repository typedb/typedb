/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use concept::error::ConceptReadError;
use error::typedb_error;

pub mod annotated_functions;
pub mod annotated_program;
pub mod pattern_type_inference;
pub mod type_annotations;
pub mod type_inference;
mod type_seeder;

typedb_error!(
    pub FunctionTypeInferenceError(component = "Function type inference", prefix = "FIN") {
        TypeInference(0, "Type inference error while type checking function '{name}'.", name: String, ( typedb_source : TypeInferenceError )),
    }
);

typedb_error!(
    pub TypeInferenceError(component = "Type inference", prefix = "INF") {
        ConceptRead(1, "Concept read error.", ( source: ConceptReadError )),
        LabelNotResolved(2, "Type label '{name}' not found.", name: String),
        RoleNameNotResolved(3, "Role label not found '{name}'.", name: String),
        IllegalInsertTypes(
            4,
            "Left type '{left_type}' across constraint '{constraint_name}' is not compatible with right type '{right_type}'.",
            constraint_name: String,
            left_type: String,
            right_type: String
        ),
        DetectedUnsatisfiablePattern(5, "Type-inference derived an empty-set for some variable"),
        AttemptedToResolveValueTypeOfNonAttributeType(
            6,
            "Attempted to resolve value type for a non-attribute type: {label}",
            label: String
        ),
        AttemptedToResolveValueTypeOfAttributeWithoutOne(
            7,
            "Attempted to resolve value type for an attribute-type without one: {label}",
            label: String
        ),
    }
);

// #[derive(Debug, Clone)]
// pub enum TypeInferenceError {
//     ConceptRead { source: ConceptReadError },
//     LabelNotResolved(String),
//     RoleNameNotResolved(String),
//     IllegalInsertTypes { constraint: Constraint<Variable>, left_type: String, right_type: String },
// }
//
// impl Display for TypeInferenceError {
//     fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
//         todo!()
//     }
// }
//
// impl Error for TypeInferenceError {
//     fn source(&self) -> Option<&(dyn Error + 'static)> {
//         match self {
//             TypeInferenceError::ConceptRead { source } => Some(source),
//             TypeInferenceError::LabelNotResolved(_) => None,
//             TypeInferenceError::RoleNameNotResolved(_) => None,
//             TypeInferenceError::IllegalInsertTypes { .. } => None,
//         }
//     }
// }

#[cfg(test)]
pub mod tests {
    use std::sync::Arc;

    use concept::{
        thing::{statistics::Statistics, thing_manager::ThingManager},
        type_::type_manager::TypeManager,
    };
    use durability::{wal::WAL, DurabilitySequenceNumber};
    use encoding::{
        graph::{
            definition::definition_key_generator::DefinitionKeyGenerator,
            thing::vertex_generator::ThingVertexGenerator, type_::vertex_generator::TypeVertexGenerator,
        },
        EncodingKeyspace,
    };
    use storage::{durability_client::WALClient, MVCCStorage};
    use test_utils::{create_tmp_dir, init_logging, TempDir};

    use crate::match_::inference::pattern_type_inference::{
        NestedTypeInferenceGraphDisjunction, TypeInferenceEdge, TypeInferenceGraph,
    };

    impl<'this> PartialEq<Self> for TypeInferenceEdge<'this> {
        fn eq(&self, other: &Self) -> bool {
            self.constraint == other.constraint
                && self.right == other.right
                && self.left == other.left
                && self.left_to_right == other.left_to_right
                && self.right_to_left == other.right_to_left
        }
    }

    impl<'this> Eq for TypeInferenceEdge<'this> {}

    impl<'this> PartialEq<Self> for TypeInferenceGraph<'this> {
        fn eq(&self, other: &Self) -> bool {
            self.vertices == other.vertices
                && self.edges == other.edges
                && self.nested_disjunctions == other.nested_disjunctions
        }
    }

    impl<'this> Eq for TypeInferenceGraph<'this> {}

    impl<'this> PartialEq<Self> for NestedTypeInferenceGraphDisjunction<'this> {
        fn eq(&self, other: &Self) -> bool {
            self.disjunction == other.disjunction
        }
    }

    impl<'this> Eq for NestedTypeInferenceGraphDisjunction<'this> {}

    pub(crate) fn setup_storage() -> (TempDir, Arc<MVCCStorage<WALClient>>) {
        init_logging();
        let storage_path = create_tmp_dir();
        let wal = WAL::create(&storage_path).unwrap();
        let storage = Arc::new(
            MVCCStorage::<WALClient>::create::<EncodingKeyspace>("storage", &storage_path, WALClient::new(wal))
                .unwrap(),
        );
        (storage_path, storage)
    }

    pub(crate) fn managers() -> (Arc<TypeManager>, ThingManager) {
        let definition_key_generator = Arc::new(DefinitionKeyGenerator::new());
        let type_vertex_generator = Arc::new(TypeVertexGenerator::new());
        let thing_vertex_generator = Arc::new(ThingVertexGenerator::new());
        let type_manager =
            Arc::new(TypeManager::new(definition_key_generator.clone(), type_vertex_generator.clone(), None));
        let thing_manager = ThingManager::new(
            thing_vertex_generator.clone(),
            type_manager.clone(),
            Arc::new(Statistics::new(DurabilitySequenceNumber::MIN)),
        );

        (type_manager, thing_manager)
    }

    pub(crate) mod schema_consts {
        use answer::Type as TypeAnnotation;
        use concept::{
            thing::thing_manager::ThingManager,
            type_::{
                annotation::AnnotationAbstract, attribute_type::AttributeTypeAnnotation,
                entity_type::EntityTypeAnnotation, type_manager::TypeManager, Ordering, OwnerAPI, PlayerAPI,
            },
        };
        use encoding::value::{label::Label, value_type::ValueType};
        use storage::{
            durability_client::WALClient,
            snapshot::{CommittableSnapshot, WritableSnapshot},
        };

        pub(crate) const LABEL_ANIMAL: Label<'_> = Label::new_static("animal");
        pub(crate) const LABEL_CAT: Label<'_> = Label::new_static("cat");
        pub(crate) const LABEL_DOG: Label<'_> = Label::new_static("dog");

        pub(crate) const LABEL_NAME: Label<'_> = Label::new_static("name");
        pub(crate) const LABEL_CATNAME: Label<'_> = Label::new_static("cat-name");
        pub(crate) const LABEL_DOGNAME: Label<'_> = Label::new_static("dog-name");

        pub(crate) const LABEL_FEARS: Label<'_> = Label::new_static("fears");
        pub(crate) const LABEL_HAS_FEAR: Label<'_> = Label::new_static("has-fear");
        pub(crate) const LABEL_IS_FEARED: Label<'_> = Label::new_static("is-feared");

        pub(crate) fn setup_types<Snapshot: WritableSnapshot + CommittableSnapshot<WALClient>>(
            snapshot_: Snapshot,
            type_manager: &TypeManager,
            thing_manager: &ThingManager,
        ) -> (
            (TypeAnnotation, TypeAnnotation, TypeAnnotation),
            (TypeAnnotation, TypeAnnotation, TypeAnnotation),
            (TypeAnnotation, TypeAnnotation, TypeAnnotation),
        ) {
            // dog sub animal, owns dog-name; cat sub animal owns cat-name;
            // cat-name sub animal-name; dog-name sub animal-name;
            let mut snapshot = snapshot_;

            // Attributes
            let name = type_manager.create_attribute_type(&mut snapshot, &LABEL_NAME).unwrap();
            let catname = type_manager.create_attribute_type(&mut snapshot, &LABEL_CATNAME).unwrap();
            let dogname = type_manager.create_attribute_type(&mut snapshot, &LABEL_DOGNAME).unwrap();
            name.set_annotation(
                &mut snapshot,
                type_manager,
                thing_manager,
                AttributeTypeAnnotation::Abstract(AnnotationAbstract),
            )
            .unwrap();
            catname.set_supertype(&mut snapshot, type_manager, thing_manager, name.clone()).unwrap();
            dogname.set_supertype(&mut snapshot, type_manager, thing_manager, name.clone()).unwrap();

            name.set_value_type(&mut snapshot, type_manager, thing_manager, ValueType::String).unwrap();
            catname.set_value_type(&mut snapshot, type_manager, thing_manager, ValueType::String).unwrap();
            dogname.set_value_type(&mut snapshot, type_manager, thing_manager, ValueType::String).unwrap();

            // Entities
            let animal = type_manager.create_entity_type(&mut snapshot, &LABEL_ANIMAL).unwrap();
            let cat = type_manager.create_entity_type(&mut snapshot, &LABEL_CAT).unwrap();
            let dog = type_manager.create_entity_type(&mut snapshot, &LABEL_DOG).unwrap();
            cat.set_supertype(&mut snapshot, type_manager, thing_manager, animal.clone()).unwrap();
            dog.set_supertype(&mut snapshot, type_manager, thing_manager, animal.clone()).unwrap();
            animal
                .set_annotation(
                    &mut snapshot,
                    type_manager,
                    thing_manager,
                    EntityTypeAnnotation::Abstract(AnnotationAbstract),
                )
                .unwrap();

            // Ownerships
            animal.set_owns(&mut snapshot, type_manager, thing_manager, name.clone(), Ordering::Unordered).unwrap();
            cat.set_owns(&mut snapshot, type_manager, thing_manager, catname.clone(), Ordering::Unordered).unwrap();
            dog.set_owns(&mut snapshot, type_manager, thing_manager, dogname.clone(), Ordering::Unordered).unwrap();

            // Relations
            let fears = type_manager.create_relation_type(&mut snapshot, &LABEL_FEARS).unwrap();
            let has_fear = fears
                .create_relates(
                    &mut snapshot,
                    type_manager,
                    thing_manager,
                    LABEL_HAS_FEAR.scoped_name().as_str(),
                    Ordering::Unordered,
                )
                .unwrap()
                .role();
            let is_feared = fears
                .create_relates(
                    &mut snapshot,
                    type_manager,
                    thing_manager,
                    LABEL_IS_FEARED.scoped_name().as_str(),
                    Ordering::Unordered,
                )
                .unwrap()
                .role();
            cat.set_plays(&mut snapshot, type_manager, thing_manager, has_fear.clone()).unwrap();
            dog.set_plays(&mut snapshot, type_manager, thing_manager, is_feared.clone()).unwrap();

            snapshot.commit().unwrap();

            (
                (TypeAnnotation::Entity(animal), TypeAnnotation::Entity(cat), TypeAnnotation::Entity(dog)),
                (
                    TypeAnnotation::Attribute(name),
                    TypeAnnotation::Attribute(catname),
                    TypeAnnotation::Attribute(dogname),
                ),
                (
                    TypeAnnotation::Relation(fears),
                    TypeAnnotation::RoleType(has_fear),
                    TypeAnnotation::RoleType(is_feared),
                ),
            )
        }
    }
}
