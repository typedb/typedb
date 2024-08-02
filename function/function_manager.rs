/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{iter::zip, sync::Arc};

use bytes::{byte_array::ByteArray, Bytes};
use compiler::inference::{
    annotated_functions::IndexedAnnotatedFunctions, type_inference::infer_types_for_functions,
};
use concept::type_::type_manager::TypeManager;
use encoding::{
    graph::{
        definition::{
            definition_key::DefinitionKey, definition_key_generator::DefinitionKeyGenerator,
            function::FunctionDefinition,
        },
        type_::index::{NameToFunctionDefinitionIndex, NameToStructDefinitionIndex},
    },
    value::string_bytes::StringBytes,
    AsBytes, Keyable,
};
use ir::{
    program::{
        function_signature::{FunctionID, FunctionSignature, FunctionSignatureIndex, HashMapFunctionSignatureIndex},
        program::Program,
        FunctionReadError,
    },
    translation::function::build_signature,
};
use ir::program::FunctionDefinitionError;
use ir::translation::function::translate_function;
use primitive::maybe_owns::MaybeOwns;
use resource::constants::snapshot::{BUFFER_KEY_INLINE, BUFFER_VALUE_INLINE};
use storage::{
    key_range::KeyRange,
    snapshot::{ReadableSnapshot, WritableSnapshot},
};

use crate::{
    function::{Function, SchemaFunction},
    function_cache::FunctionCache,
    FunctionManagerError,
};

/// Analogy to TypeManager, but specialised just for Functions
pub struct FunctionManager {
    definition_key_generator: Arc<DefinitionKeyGenerator>,
    function_cache: Option<Arc<FunctionCache>>,
}

impl FunctionManager {
    pub fn new(
        definition_key_generator: Arc<DefinitionKeyGenerator>,
        function_cache: Option<Arc<FunctionCache>>,
    ) -> Self {
        FunctionManager { definition_key_generator, function_cache }
    }

    pub fn get_annotated_functions(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager
    ) -> Arc<IndexedAnnotatedFunctions> {
        match self.function_cache.as_ref() {
            None => {
                let (_, _, index) = FunctionCache::build_indexed_annotated_schema_functions(snapshot, type_manager);
                Arc::new(index)
            }
            Some(cache) => cache.get_annotated_functions(),
        }
    }

    pub fn finalise(
        self,
        snapshot: &impl WritableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<(), FunctionManagerError> {
        let functions = FunctionReader::get_functions_all(snapshot)
            .map_err(|source| FunctionManagerError::FunctionRead { source })?;
        // TODO: Optimise: We recompile & redo type-inference on all functions here.
        // Prepare ir
        let function_index =
            HashMapFunctionSignatureIndex::build(functions.iter().map(|f| (f.function_id.clone().into(), &f.parsed)));
        let ir = Self::translate_functions(&function_index, &functions)?;
        // Run type-inference
        infer_types_for_functions(ir, snapshot, type_manager, &IndexedAnnotatedFunctions::empty())
            .map_err(|source| FunctionManagerError::TypeInference { source })?;
        Ok(())
    }

    pub fn define_functions(
        &self,
        snapshot: &mut impl WritableSnapshot,
        definitions: Vec<String>,
    ) -> Result<Vec<SchemaFunction>, FunctionManagerError> {
        let mut functions: Vec<SchemaFunction> = Vec::new();
        for definition in &definitions {
            let definition_key = self
                .definition_key_generator
                .create_function(snapshot)
                .map_err(|source| FunctionManagerError::Encoding { source })?;
            let function = Function::build(definition_key, FunctionDefinition::build_ref(definition))?;
            let index_key =
                NameToStructDefinitionIndex::build::<BUFFER_KEY_INLINE>(StringBytes::build_ref(&function.name()))
                    .into_storage_key();
            let existing = snapshot
                .get::<BUFFER_VALUE_INLINE>(index_key.as_reference())
                .map_err(|source| FunctionManagerError::SnapshotGet { source })?;
            if existing.is_some() {
                Err(FunctionManagerError::FunctionAlreadyExists { name: function.name() })?;
            } else {
                functions.push(function);
            }
        }

        let buffered = HashMapFunctionSignatureIndex::build(functions.iter().map(|f| (f.function_id.clone().into(), &f.parsed)));
        let function_index = ReadThroughFunctionSignatureIndex::new(snapshot, self, buffered);
        // Translate to ensure the function calls are valid references. Type-inference is done at commit-time.
        Self::translate_functions(&function_index, &functions)?;


        for (function, definition) in zip(functions.iter(), definitions.iter()) {
            let index_key =
                NameToFunctionDefinitionIndex::build::<BUFFER_KEY_INLINE>(StringBytes::build_ref(&function.name()))
                    .into_storage_key();
            let definition_key = &function.function_id;
            snapshot.put_val(index_key.into_owned_array(), ByteArray::copy(definition_key.bytes().bytes()));
            snapshot.put_val(
                definition_key.clone().into_storage_key().into_owned_array(),
                FunctionDefinition::build_ref(definition).into_bytes().into_array(),
            );
        }
        Ok(functions)
    }

    pub(crate) fn translate_functions(
        function_index: &impl FunctionSignatureIndex,
        functions: &Vec<SchemaFunction>
    ) -> Result<Vec<ir::program::function::Function>, FunctionManagerError> {
        functions
            .iter()
            .map(|f| &f.parsed)
            .map(|function| translate_function(function_index, &function))
            .collect::<Result<Vec<ir::program::function::Function>, FunctionDefinitionError>>()
            .map_err(|err| FunctionManagerError::FunctionDefinition { source: err })
    }

    pub fn get_function_key(
        &self,
        snapshot: &impl ReadableSnapshot,
        name: &str,
    ) -> Result<Option<DefinitionKey<'static>>, FunctionReadError> {
        if let Some(cache) = &self.function_cache {
            Ok(cache.get_function_key(name))
        } else {
            Ok(FunctionReader::get_function_key(snapshot, name)?)
        }
    }

    pub fn get_function(
        &self,
        snapshot: &impl ReadableSnapshot,
        definition_key: DefinitionKey<'static>,
    ) -> Result<MaybeOwns<SchemaFunction>, FunctionReadError> {
        if let Some(cache) = &self.function_cache {
            Ok(MaybeOwns::Borrowed(cache.get_function(definition_key).unwrap()))
        } else {
            Ok(MaybeOwns::Owned(FunctionReader::get_function(snapshot, definition_key)?))
        }
    }
}

pub struct FunctionReader {}

impl FunctionReader {
    pub(crate) fn get_functions_all(
        snapshot: &impl ReadableSnapshot,
    ) -> Result<Vec<SchemaFunction>, FunctionReadError> {
        snapshot
            .iterate_range(KeyRange::new_within(
                DefinitionKey::build_prefix(FunctionDefinition::PREFIX),
                DefinitionKey::FIXED_WIDTH_ENCODING,
            ))
            .collect_cloned_vec(|key, value| {
                SchemaFunction::build(
                    DefinitionKey::new(Bytes::Reference(key.byte_ref()).into_owned()),
                    FunctionDefinition::new(Bytes::Reference(value).into_owned()),
                )
                .unwrap()
            })
            .map_err(|source| FunctionReadError::SnapshotIterate { source })
    }

    pub(crate) fn get_function_key(
        snapshot: &impl ReadableSnapshot,
        name: &str,
    ) -> Result<Option<DefinitionKey<'static>>, FunctionReadError> {
        let index_key = NameToFunctionDefinitionIndex::build(StringBytes::<BUFFER_KEY_INLINE>::build_ref(name));
        let bytes_opt = snapshot
            .get(index_key.into_storage_key().as_reference())
            .map_err(|source| FunctionReadError::SnapshotGet { source })?;
        Ok(bytes_opt.map(|bytes| DefinitionKey::new(Bytes::Array(bytes))))
    }

    pub(crate) fn get_function(
        snapshot: &impl ReadableSnapshot,
        definition_key: DefinitionKey<'static>,
    ) -> Result<SchemaFunction, FunctionReadError> {
        snapshot
            .get::<BUFFER_VALUE_INLINE>(definition_key.clone().into_storage_key().as_reference())
            .map_err(|source| FunctionReadError::SnapshotGet { source })?
            .map_or(
                Err(FunctionReadError::FunctionNotFound { function_id: FunctionID::Schema(definition_key.clone()) }),
                |bytes| Ok(Function::build(definition_key, FunctionDefinition::new(Bytes::Array(bytes))).unwrap()),
            )
    }
}

pub struct ReadThroughFunctionSignatureIndex<'this, Snapshot: ReadableSnapshot> {
    snapshot: &'this Snapshot,
    function_manager: &'this FunctionManager,
    buffered: HashMapFunctionSignatureIndex,
}

impl<'this, Snapshot: ReadableSnapshot> ReadThroughFunctionSignatureIndex<'this, Snapshot> {
    pub fn new(
        snapshot: &'this Snapshot,
        function_manager: &'this FunctionManager,
        buffered: HashMapFunctionSignatureIndex,
    ) -> Self {
        Self { snapshot, function_manager, buffered }
    }
}

impl<'snapshot, Snapshot: ReadableSnapshot> FunctionSignatureIndex
    for ReadThroughFunctionSignatureIndex<'snapshot, Snapshot>
{
    fn get_function_signature(
        &self,
        name: &str,
    ) -> Result<Option<MaybeOwns<'_, FunctionSignature>>, FunctionReadError> {
        Ok(if let Some(signature) = self.buffered.get_function_signature(name)? {
            Some(signature)
        } else if let Some(key) = self.function_manager.get_function_key(self.snapshot, name)? {
            let function = self.function_manager.get_function(self.snapshot, key)?;
            let signature = build_signature(FunctionID::Schema(function.function_id.clone()), &function.parsed);
            Some(MaybeOwns::Owned(signature))
        } else {
            None
        })
    }
}

#[cfg(test)]
pub mod tests {
    use std::{collections::HashSet, sync::Arc};

    use concept::type_::type_manager::TypeManager;
    use durability::wal::WAL;
    use encoding::{
        graph::{
            definition::{
                definition_key::{DefinitionID, DefinitionKey},
                definition_key_generator::DefinitionKeyGenerator,
            },
            type_::vertex_generator::TypeVertexGenerator,
        },
        layout::prefix::Prefix,
        EncodingKeyspace,
    };
    use ir::{
        pattern::variable_category::{VariableCategory, VariableOptionality},
        program::function_signature::{FunctionID, FunctionSignature, FunctionSignatureIndex, HashMapFunctionSignatureIndex},
    };
    use storage::{durability_client::WALClient, snapshot::CommittableSnapshot, MVCCStorage};
    use test_utils::{create_tmp_dir, init_logging};

    use crate::{
        function_cache::FunctionCache,
        function_manager::{tests::test_schema::setup_types, FunctionManager, ReadThroughFunctionSignatureIndex},
    };

    fn setup_storage() -> Arc<MVCCStorage<WALClient>> {
        init_logging();
        let storage_path = create_tmp_dir();
        let wal = WAL::create(&storage_path).unwrap();
        Arc::new(
            MVCCStorage::<WALClient>::create::<EncodingKeyspace>("storage", &storage_path, WALClient::new(wal))
                .unwrap(),
        )
    }

    #[test]
    fn test_define_functions() {
        let storage = setup_storage();
        let type_manager =
            TypeManager::new(Arc::new(DefinitionKeyGenerator::new()), Arc::new(TypeVertexGenerator::new()), None);
        let ((_type_animal, type_cat, _type_dog), _) =
            setup_types(storage.clone().open_snapshot_write(), &type_manager);
        let functions_to_define = vec!["
        fun cat_names($c: animal) -> { name } :
            match
                $c has cat-name $n;
            return { $n };
        "
        .to_owned()];
        let expected_name = "cat_names";

        let expected_function_id = DefinitionKey::build(Prefix::DefinitionFunction, DefinitionID::build(0));
        let expected_signature = FunctionSignature::new(
            FunctionID::Schema(expected_function_id.clone()),
            vec![VariableCategory::Object],
            vec![(VariableCategory::Object, VariableOptionality::Required)],
            true,
        );
        let sequence_number = {
            let function_manager = FunctionManager::new(Arc::new(DefinitionKeyGenerator::new()), None);
            let mut snapshot = storage.clone().open_snapshot_write();
            let stored_functions = function_manager.define_functions(&mut snapshot, functions_to_define).unwrap();
            // Read buffered
            assert_eq!(expected_function_id, stored_functions[0].function_id());

            assert_eq!(
                expected_function_id,
                function_manager.get_function_key(&snapshot, expected_name).unwrap().unwrap()
            );
            assert_eq!(
                expected_name,
                function_manager.get_function(&snapshot, expected_function_id.clone()).unwrap().name().as_str()
            );
            function_manager.finalise(&snapshot, &type_manager).unwrap();
            snapshot.commit().unwrap().unwrap()
        };

        {
            // Read committed
            let function_manager = FunctionManager::new(Arc::new(DefinitionKeyGenerator::new()), None);
            let snapshot = storage.clone().open_snapshot_read();
            assert_eq!(
                expected_function_id,
                function_manager.get_function_key(&snapshot, expected_name).unwrap().unwrap()
            );
            assert_eq!(
                expected_name,
                function_manager.get_function(&snapshot, expected_function_id.clone()).unwrap().name().as_str()
            );
            let index =
                ReadThroughFunctionSignatureIndex::new(&snapshot, &function_manager, HashMapFunctionSignatureIndex::empty());
            assert!(matches!(index.get_function_signature("unresolved"), Ok(None)));
            let looked_up = index.get_function_signature("cat_names").unwrap().unwrap();
            assert_eq!(expected_signature.function_id(), looked_up.function_id());
        }

        {
            // Read cached
            let cache = Arc::new(FunctionCache::new(storage.clone(), &type_manager, sequence_number).unwrap());
            let snapshot = storage.clone().open_snapshot_read();
            let function_manager = FunctionManager::new(Arc::new(DefinitionKeyGenerator::new()), Some(cache.clone()));
            let index =
                ReadThroughFunctionSignatureIndex::new(&snapshot, &function_manager, HashMapFunctionSignatureIndex::empty());
            assert!(matches!(index.get_function_signature("unresolved"), Ok(None)));
            let looked_up = index.get_function_signature("cat_names").unwrap().unwrap();
            assert_eq!(expected_signature.function_id(), looked_up.function_id());

            let function_annotations = cache.get_function_annotations(expected_function_id.clone()).unwrap();
            let function_ir = cache.get_function_ir(expected_function_id.clone()).unwrap();
            let var_c = *function_ir.block().context().get_variable_named("c", function_ir.block().scope_id()).unwrap();
            let var_c_annotations = function_annotations.body_annotations().variable_annotations_of(var_c).unwrap();
            assert_eq!(&Arc::new(HashSet::from([type_cat.clone()])), var_c_annotations);
        }
    }

    pub(crate) mod test_schema {
        use answer::Type as TypeAnnotation;
        use concept::type_::{
            annotation::AnnotationAbstract, attribute_type::AttributeTypeAnnotation, entity_type::EntityTypeAnnotation,
            type_manager::TypeManager, Ordering, OwnerAPI,
        };
        use encoding::value::{label::Label, value_type::ValueType};
        use storage::{
            durability_client::WALClient,
            snapshot::{CommittableSnapshot, WritableSnapshot},
        };
        pub(crate) const LABEL_ANIMAL: &str = "animal";
        pub(crate) const LABEL_CAT: &str = "cat";
        pub(crate) const LABEL_DOG: &str = "dog";

        pub(crate) const LABEL_NAME: &str = "name";
        pub(crate) const LABEL_CATNAME: &str = "cat-name";
        pub(crate) const LABEL_DOGNAME: &str = "dog-name";

        pub(crate) fn setup_types<Snapshot: WritableSnapshot + CommittableSnapshot<WALClient>>(
            snapshot_: Snapshot,
            type_manager: &TypeManager,
        ) -> ((TypeAnnotation, TypeAnnotation, TypeAnnotation), (TypeAnnotation, TypeAnnotation, TypeAnnotation))
        {
            // dog sub animal, owns dog-name; cat sub animal owns cat-name;
            // cat-name sub animal-name; dog-name sub animal-name;
            let mut snapshot = snapshot_;

            // Attributes
            let name = type_manager.create_attribute_type(&mut snapshot, &Label::build(LABEL_NAME)).unwrap();
            let catname = type_manager.create_attribute_type(&mut snapshot, &Label::build(LABEL_CATNAME)).unwrap();
            let dogname = type_manager.create_attribute_type(&mut snapshot, &Label::build(LABEL_DOGNAME)).unwrap();
            name.set_annotation(&mut snapshot, type_manager, AttributeTypeAnnotation::Abstract(AnnotationAbstract))
                .unwrap();
            catname.set_supertype(&mut snapshot, type_manager, name.clone()).unwrap();
            dogname.set_supertype(&mut snapshot, type_manager, name.clone()).unwrap();

            name.set_value_type(&mut snapshot, type_manager, ValueType::String).unwrap();
            catname.set_value_type(&mut snapshot, type_manager, ValueType::String).unwrap();
            dogname.set_value_type(&mut snapshot, type_manager, ValueType::String).unwrap();

            // Entities
            let animal = type_manager.create_entity_type(&mut snapshot, &Label::build(LABEL_ANIMAL)).unwrap();
            let cat = type_manager.create_entity_type(&mut snapshot, &Label::build(LABEL_CAT)).unwrap();
            let dog = type_manager.create_entity_type(&mut snapshot, &Label::build(LABEL_DOG)).unwrap();
            cat.set_supertype(&mut snapshot, type_manager, animal.clone()).unwrap();
            dog.set_supertype(&mut snapshot, type_manager, animal.clone()).unwrap();
            animal
                .set_annotation(&mut snapshot, type_manager, EntityTypeAnnotation::Abstract(AnnotationAbstract))
                .unwrap();

            // Ownerships
            let animal_owns = animal.set_owns(&mut snapshot, type_manager, name.clone(), Ordering::Unordered).unwrap();
            let cat_owns = cat.set_owns(&mut snapshot, type_manager, catname.clone(), Ordering::Unordered).unwrap();
            let dog_owns = dog.set_owns(&mut snapshot, type_manager, dogname.clone(), Ordering::Unordered).unwrap();
            cat_owns.set_override(&mut snapshot, type_manager, animal_owns.clone()).unwrap();
            dog_owns.set_override(&mut snapshot, type_manager, animal_owns.clone()).unwrap();

            snapshot.commit().unwrap();

            (
                (TypeAnnotation::Entity(animal), TypeAnnotation::Entity(cat), TypeAnnotation::Entity(dog)),
                (
                    TypeAnnotation::Attribute(name),
                    TypeAnnotation::Attribute(catname),
                    TypeAnnotation::Attribute(dogname),
                ),
            )
        }
    }
}
