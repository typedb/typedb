/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use answer::variable::Variable;
use concept::error::ConceptReadError;
use encoding::value::value_type::ValueTypeCategory;
use error::typedb_error;
use expression::ExpressionCompileError;
use ir::pattern::constraint::Constraint;

use crate::annotation::expression::compiled_expression::ExpressionValueType;

pub mod expression;
pub mod fetch;
pub mod function;
pub mod match_inference;
pub mod pipeline;
pub mod type_annotations;
pub mod type_inference;
mod type_seeder;

typedb_error!(
    pub AnnotationError(component = "Query annotation", prefix = "QUA") {
        Unimplemented(0, "Unimplemented: {description}", description: String),
        TypeInference(1, "Type inference error while compiling query annotations.", typedb_source: TypeInferenceError),
        PreambleTypeInference(2, "Type inference error while compiling query preamble functions.", typedb_source: Box<FunctionAnnotationError>),
        ExpressionCompilation(3, "Error inferring correct expression types.", source: Box<ExpressionCompileError>),
        FetchEntry(4, "Error during type inference for fetch operation for key '{key}'.", key: String, typedb_source: Box<AnnotationError>),
        FetchBlockFunctionInferenceError(5, "Error during type inference for fetch sub-query.", typedb_source: Box<FunctionAnnotationError>),
        ConceptRead(6, "Error while retrieving concept.", source: Box<ConceptReadError>),
        FetchAttributeNotFound(7, "Fetching '${var}.{name}' failed since the attribute type is not defined.", var: String, name: String),
        FetchSingleAttributeNotOwned(
            8,
            "Type checking '${var}.{attribute}' failed, since attribute '{attribute}' cannot be when '${var}' has type '{owner}'.",
            var: String,
            owner: String,
            attribute: String,
        ),
        FetchAttributesNotOwned(
            9,
            "Type checking '[${var}.{attribute}]' failed, since attribute '{attribute}' cannot be when '${var}' has type '{owner}'.",
            var: String,
            owner: String,
            attribute: String,
        ),
        FetchSingleAttributeCannotBeOwnedByKind(
            10,
            "Type checking '${var}.{attribute}' failed, since attribute '{attribute}' cannot be when '${var}' has kind '{kind}'.",
            var: String,
            kind: String,
            attribute: String,
        ),
        FetchAttributesCannotBeOwnedByKind(
            11,
            "Type checking '[${var}.{attribute}]' failed, since attribute '{attribute}' cannot be when '${var}' has kind '{kind}'.",
            var: String,
            kind: String,
            attribute: String,
        ),
        AttributeFetchCardTooHigh(
            12,
            "Fetch attribute '${var}.{attribute}' must be wrapped in '[]', since this attribute can be owned more than 1 time when '${var}' has type '{owner}', according to the schema's cardinality constraints.",
            var: String,
            owner: String,
            attribute: String,
        ),
        CouldNotDetermineValueTypeForReducerInput(
            13, "The value-type for the reducer input variable '{variable}' could not be determined.", variable: String,
        ),
        ReducerInputVariableDidNotHaveSingleValueType(
            14, "The reducer input variable '{variable}' had multiple value-types.", variable: String,
        ),
        UnsupportedValueTypeForReducer(
            15,
            "The input variable to the reducer'{reducer}({variable})' reducer had an unsupported value-type: '{value_type}'",
            reducer: String,
            variable: String,
            value_type: ValueTypeCategory,
        ),
        UncomparableValueTypesForSortVariable(
            16,
            "The sort variable '{variable}' could return incomparable value-types '{category1}' & '{category2}'.",
            variable: String,
            category1: ValueTypeCategory,
            category2: ValueTypeCategory,
        ),
        ReducerInputVariableIsList(
            17,
            "The input variable '{variable}' to the reducer '{reducer}' was a list.",
            reducer: String,
            variable: String,
        ),
    }
);

typedb_error!(
    pub FunctionAnnotationError(component = "Function type inference", prefix = "FIN") {
        TypeInference(0, "Type inference error while type checking function '{name}'.", name: String, typedb_source: Box<AnnotationError>),
        CouldNotResolveArgumentType(
            1,
            "An error occurred when trying to resolve the type of the argument at index: {index}.",
            index: usize,
            typedb_source: TypeInferenceError
        ),
        CallerSignatureTypeMismatch(
            2,
            "Function '{name}' argument at position '{index}' is only invoked with types that not compatible with the argument type '{arg_type}'.",
            name: String,
            index: usize,
            arg_type: String
        ),
        CallerSignatureValueTypeMismatch(
            3,
            "Function '{name}' argument at position '{index}' expects value type '{expected}' but is only invoked with '{actual}'.",
            name: String,
            index: usize,
            expected: ExpressionValueType,
            actual: ExpressionValueType
        ),
        CouldNotDetermineArgumentType(
            4,
            "Failed to infer any argument type for function '{name}' argument at position '{index}'.",
            name: String,
            index: usize
        ),
        CouldNotResolveReturnType(
            5,
            "An error occurred when trying to resolve the type at return index: {index}.",
            index: usize,
            typedb_source: TypeInferenceError
        ),
        ReturnReduce(
            6,
            "Error analysing return reduction.",
            typedb_source: Box<AnnotationError>,
        ),
        SignatureReturnMismatch(
            7,
            "The types inferred for the return statement of function \"{function_name}\" did not match those declared in the signature. Mismatching index: {mismatching_index}",
            function_name: String,
            mismatching_index: usize,
        ),
    }
);

typedb_error!(
    pub TypeInferenceError(component = "Type inference", prefix = "INF") {
        ConceptRead(1, "Concept read error.", source: Box<ConceptReadError>),
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
        ValueTypeNotFound(8, "Value type '{name}' was not found.", name: String),
        AnnotationsUnavailableForVariableInInsert(9,
            "The annotations for the variable '{variable}' in the inserted constraint {constraint} were not available. Check if the variable is available from a previous stage or is inserted in this stage.",
            variable: Variable,
            constraint: Constraint<Variable>,
        ),
        OptionalTypesUnsupported(255, "Optional types are not yet supported."),
        ListTypesUnsupported(256, "List types are not yet supported."),
    }
);

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

    use crate::annotation::match_inference::{
        NestedTypeInferenceGraphDisjunction, TypeInferenceEdge, TypeInferenceGraph,
    };

    impl PartialEq<Self> for TypeInferenceEdge<'_> {
        fn eq(&self, other: &Self) -> bool {
            self.constraint == other.constraint
                && self.right == other.right
                && self.left == other.left
                && self.left_to_right == other.left_to_right
                && self.right_to_left == other.right_to_left
        }
    }

    impl Eq for TypeInferenceEdge<'_> {}

    impl PartialEq<Self> for TypeInferenceGraph<'_> {
        fn eq(&self, other: &Self) -> bool {
            self.vertices == other.vertices
                && self.edges == other.edges
                && self.nested_disjunctions == other.nested_disjunctions
        }
    }

    impl Eq for TypeInferenceGraph<'_> {}

    impl PartialEq<Self> for NestedTypeInferenceGraphDisjunction<'_> {
        fn eq(&self, other: &Self) -> bool {
            self.disjunction == other.disjunction
        }
    }

    impl Eq for NestedTypeInferenceGraphDisjunction<'_> {}

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

        pub(crate) const LABEL_ANIMAL: Label = Label::new_static("animal");
        pub(crate) const LABEL_CAT: Label = Label::new_static("cat");
        pub(crate) const LABEL_DOG: Label = Label::new_static("dog");

        pub(crate) const LABEL_NAME: Label = Label::new_static("name");
        pub(crate) const LABEL_CATNAME: Label = Label::new_static("cat-name");
        pub(crate) const LABEL_DOGNAME: Label = Label::new_static("dog-name");

        pub(crate) const LABEL_FEARS: Label = Label::new_static("fears");
        pub(crate) const LABEL_HAS_FEAR: Label = Label::new_static_scoped("has-fear", "fears", "fears:has-fear");
        pub(crate) const LABEL_IS_FEARED: Label = Label::new_static_scoped("is-feared", "fears", "fears:is-feared");

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
            catname.set_supertype(&mut snapshot, type_manager, thing_manager, name).unwrap();
            dogname.set_supertype(&mut snapshot, type_manager, thing_manager, name).unwrap();

            name.set_value_type(&mut snapshot, type_manager, thing_manager, ValueType::String).unwrap();
            catname.set_value_type(&mut snapshot, type_manager, thing_manager, ValueType::String).unwrap();
            dogname.set_value_type(&mut snapshot, type_manager, thing_manager, ValueType::String).unwrap();

            // Entities
            let animal = type_manager.create_entity_type(&mut snapshot, &LABEL_ANIMAL).unwrap();
            let cat = type_manager.create_entity_type(&mut snapshot, &LABEL_CAT).unwrap();
            let dog = type_manager.create_entity_type(&mut snapshot, &LABEL_DOG).unwrap();
            cat.set_supertype(&mut snapshot, type_manager, thing_manager, animal).unwrap();
            dog.set_supertype(&mut snapshot, type_manager, thing_manager, animal).unwrap();
            animal
                .set_annotation(
                    &mut snapshot,
                    type_manager,
                    thing_manager,
                    EntityTypeAnnotation::Abstract(AnnotationAbstract),
                )
                .unwrap();

            // Ownerships
            animal.set_owns(&mut snapshot, type_manager, thing_manager, name, Ordering::Unordered).unwrap();
            cat.set_owns(&mut snapshot, type_manager, thing_manager, catname, Ordering::Unordered).unwrap();
            dog.set_owns(&mut snapshot, type_manager, thing_manager, dogname, Ordering::Unordered).unwrap();

            // Relations
            let fears = type_manager.create_relation_type(&mut snapshot, &LABEL_FEARS).unwrap();
            let has_fear = fears
                .create_relates(
                    &mut snapshot,
                    type_manager,
                    thing_manager,
                    LABEL_HAS_FEAR.name().as_str(),
                    Ordering::Unordered,
                )
                .unwrap()
                .role();
            let is_feared = fears
                .create_relates(
                    &mut snapshot,
                    type_manager,
                    thing_manager,
                    LABEL_IS_FEARED.name().as_str(),
                    Ordering::Unordered,
                )
                .unwrap()
                .role();
            cat.set_plays(&mut snapshot, type_manager, thing_manager, has_fear).unwrap();
            dog.set_plays(&mut snapshot, type_manager, thing_manager, is_feared).unwrap();

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
