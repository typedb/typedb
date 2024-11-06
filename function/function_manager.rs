/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{iter::zip, sync::Arc};

use bytes::{byte_array::ByteArray, Bytes};
use compiler::annotation::function::{annotate_functions, IndexedAnnotatedFunctions};
use concept::type_::type_manager::TypeManager;
use encoding::{
    graph::{
        definition::{
            definition_key::DefinitionKey, definition_key_generator::DefinitionKeyGenerator,
            function::FunctionDefinition,
        },
        type_::index::{NameToFunctionDefinitionIndex, NameToStructDefinitionIndex},
    },
    AsBytes, Keyable,
};
use ir::{
    pipeline::{
        function_signature::{FunctionID, FunctionSignature, FunctionSignatureIndex, HashMapFunctionSignatureIndex},
        FunctionReadError,
    },
    translation::function::{build_signature, translate_typeql_function},
};
use itertools::Itertools;
use primitive::maybe_owns::MaybeOwns;
use resource::constants::snapshot::BUFFER_VALUE_INLINE;
use storage::{
    key_range::{KeyRange, RangeStart},
    snapshot::{ReadableSnapshot, WritableSnapshot},
};

use crate::{function::SchemaFunction, function_cache::FunctionCache, FunctionError};

/// Analogy to TypeManager, but specialised just for Functions
#[derive(Debug)]
pub struct FunctionManager {
    definition_key_generator: Arc<DefinitionKeyGenerator>,
    function_cache: Option<Arc<FunctionCache>>,
}

impl Default for FunctionManager {
    fn default() -> Self {
        Self::new(Arc::new(DefinitionKeyGenerator::new()), None)
    }
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
        type_manager: &TypeManager,
    ) -> Result<Arc<IndexedAnnotatedFunctions>, FunctionError> {
        match self.function_cache.as_ref() {
            None => FunctionCache::build_cache(snapshot, type_manager).map(|cache| cache.get_annotated_functions()),
            Some(cache) => Ok(cache.get_annotated_functions()),
        }
    }

    pub fn finalise(self, snapshot: &impl WritableSnapshot, type_manager: &TypeManager) -> Result<(), FunctionError> {
        let functions = FunctionReader::get_functions_all(snapshot)
            .map_err(|source| FunctionError::FunctionRetrieval { source })?;
        // TODO: Optimise: We recompile & redo type-inference on all functions here.
        // Prepare ir
        let function_index =
            HashMapFunctionSignatureIndex::build(functions.iter().map(|f| (f.function_id.clone().into(), &f.parsed)));
        let translated = Self::translate_functions(snapshot, &functions, &function_index)?;
        // Run type-inference
        annotate_functions(translated, snapshot, type_manager, &IndexedAnnotatedFunctions::empty())
            .map_err(|source| FunctionError::AllFunctionsTypeCheckFailure { typedb_source: source })?;
        Ok(())
    }

    pub fn define_functions(
        &self,
        snapshot: &mut impl WritableSnapshot,
        definitions: Vec<String>,
    ) -> Result<Vec<SchemaFunction>, FunctionError> {
        let mut functions: Vec<SchemaFunction> = Vec::new();
        for definition in &definitions {
            let definition_key = self
                .definition_key_generator
                .create_function(snapshot)
                .map_err(|source| FunctionError::CreateFunctionEncoding { source })?;
            let function = SchemaFunction::build(definition_key, FunctionDefinition::build_ref(definition))?;
            let index_key = NameToStructDefinitionIndex::build(function.name().as_str()).into_storage_key();
            let existing = snapshot.get::<BUFFER_VALUE_INLINE>(index_key.as_reference()).map_err(|source| {
                FunctionError::FunctionRetrieval { source: FunctionReadError::FunctionRetrieval { source } }
            })?;
            if existing.is_some() {
                Err(FunctionError::FunctionAlreadyExists { name: function.name() })?;
            } else {
                functions.push(function);
            }
        }

        let buffered =
            HashMapFunctionSignatureIndex::build(functions.iter().map(|f| (f.function_id.clone().into(), &f.parsed)));
        let function_index = ReadThroughFunctionSignatureIndex::new(snapshot, self, buffered);
        // Translate to ensure the function calls are valid references. Type-inference is done at commit-time.
        Self::translate_functions(snapshot, &functions, &function_index)?;

        for (function, definition) in zip(functions.iter(), definitions.iter()) {
            let index_key = NameToFunctionDefinitionIndex::build(function.name().as_str()).into_storage_key();
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
        snapshot: &impl ReadableSnapshot,
        functions: &[SchemaFunction],
        function_index: &impl FunctionSignatureIndex,
    ) -> Result<Vec<ir::pipeline::function::Function>, FunctionError> {
        functions
            .iter()
            .map(|function| translate_typeql_function(snapshot, function_index, &function.parsed))
            .try_collect()
            .map_err(|err| FunctionError::FunctionTranslation { typedb_source: err })
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
                RangeStart::Inclusive(DefinitionKey::build_prefix(FunctionDefinition::PREFIX)),
                DefinitionKey::FIXED_WIDTH_ENCODING,
            ))
            .collect_cloned_vec(|key, value| {
                SchemaFunction::build(
                    DefinitionKey::new(Bytes::Reference(key.byte_ref()).into_owned()),
                    FunctionDefinition::new(Bytes::Reference(value).into_owned()),
                )
                .unwrap()
            })
            .map_err(|source| FunctionReadError::FunctionsScan { source })
    }

    pub(crate) fn get_function_key(
        snapshot: &impl ReadableSnapshot,
        name: &str,
    ) -> Result<Option<DefinitionKey<'static>>, FunctionReadError> {
        let index_key = NameToFunctionDefinitionIndex::build(name);
        let bytes_opt = snapshot
            .get(index_key.into_storage_key().as_reference())
            .map_err(|source| FunctionReadError::FunctionRetrieval { source })?;
        Ok(bytes_opt.map(|bytes| DefinitionKey::new(Bytes::Array(bytes))))
    }

    pub(crate) fn get_function(
        snapshot: &impl ReadableSnapshot,
        definition_key: DefinitionKey<'static>,
    ) -> Result<SchemaFunction, FunctionReadError> {
        snapshot
            .get::<BUFFER_VALUE_INLINE>(definition_key.clone().into_storage_key().as_reference())
            .map_err(|source| FunctionReadError::FunctionRetrieval { source })?
            .map_or(
                Err(FunctionReadError::FunctionNotFound { function_id: FunctionID::Schema(definition_key.clone()) }),
                |bytes| Ok(SchemaFunction::build(definition_key, FunctionDefinition::new(Bytes::Array(bytes))).unwrap()),
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
    use std::{collections::BTreeSet, sync::Arc};

    use compiler::annotation::pipeline::AnnotatedStage;
    use concept::{
        thing::{statistics::Statistics, thing_manager::ThingManager},
        type_::type_manager::TypeManager,
    };
    use durability::{wal::WAL, DurabilitySequenceNumber};
    use encoding::{
        graph::{
            definition::{
                definition_key::{DefinitionID, DefinitionKey},
                definition_key_generator::DefinitionKeyGenerator,
            },
            thing::vertex_generator::ThingVertexGenerator,
            type_::vertex_generator::TypeVertexGenerator,
        },
        layout::prefix::Prefix,
        EncodingKeyspace,
    };
    use ir::{
        pattern::{
            variable_category::{VariableCategory, VariableOptionality},
            Vertex,
        },
        pipeline::function_signature::{
            FunctionID, FunctionSignature, FunctionSignatureIndex, HashMapFunctionSignatureIndex,
        },
    };
    use storage::{durability_client::WALClient, snapshot::CommittableSnapshot, MVCCStorage};
    use test_utils::{create_tmp_dir, init_logging, TempDir};

    use crate::{
        function_cache::FunctionCache,
        function_manager::{tests::test_schema::setup_types, FunctionManager, ReadThroughFunctionSignatureIndex},
    };

    fn setup_storage() -> (TempDir, Arc<MVCCStorage<WALClient>>) {
        init_logging();
        let storage_path = create_tmp_dir();
        let wal = WAL::create(&storage_path).unwrap();
        let storage = Arc::new(
            MVCCStorage::<WALClient>::create::<EncodingKeyspace>("storage", &storage_path, WALClient::new(wal))
                .unwrap(),
        );
        (storage_path, storage)
    }

    #[test]
    fn test_define_functions() {
        let (_tmp_dir, storage) = setup_storage();
        let type_manager = Arc::new(TypeManager::new(
            Arc::new(DefinitionKeyGenerator::new()),
            Arc::new(TypeVertexGenerator::new()),
            None,
        ));
        let thing_manager = ThingManager::new(
            Arc::new(ThingVertexGenerator::new()),
            type_manager.clone(),
            Arc::new(Statistics::new(DurabilitySequenceNumber::MIN)),
        );

        let ((_type_animal, type_cat, _type_dog), _) =
            setup_types(storage.clone().open_snapshot_write(), &type_manager, &thing_manager);
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
            let index = ReadThroughFunctionSignatureIndex::new(
                &snapshot,
                &function_manager,
                HashMapFunctionSignatureIndex::empty(),
            );
            assert!(matches!(index.get_function_signature("unresolved"), Ok(None)));
            let looked_up = index.get_function_signature("cat_names").unwrap().unwrap();
            assert_eq!(expected_signature.function_id(), looked_up.function_id());
        }

        {
            // Read cached
            let cache = Arc::new(FunctionCache::new(storage.clone(), &type_manager, sequence_number).unwrap());
            let snapshot = storage.clone().open_snapshot_read();
            let function_manager = FunctionManager::new(Arc::new(DefinitionKeyGenerator::new()), Some(cache.clone()));
            let index = ReadThroughFunctionSignatureIndex::new(
                &snapshot,
                &function_manager,
                HashMapFunctionSignatureIndex::empty(),
            );
            assert!(matches!(index.get_function_signature("unresolved"), Ok(None)));
            let looked_up = index.get_function_signature("cat_names").unwrap().unwrap();
            assert_eq!(expected_signature.function_id(), looked_up.function_id());

            let function_annotations = cache.get_annotated_function(expected_function_id.clone()).unwrap();
            let AnnotatedStage::Match { block_annotations: body_annotations, .. } =
                function_annotations.stages.first().as_ref().unwrap()
            else {
                unreachable!()
            };
            let var_c = function_annotations.arguments[0];
            let var_c_annotations = body_annotations.vertex_annotations_of(&Vertex::Variable(var_c)).unwrap();
            assert_eq!(&Arc::new(BTreeSet::from([type_cat.clone()])), var_c_annotations);
        }
    }

    pub(crate) mod test_schema {
        use answer::Type as TypeAnnotation;
        use concept::{
            thing::thing_manager::ThingManager,
            type_::{
                annotation::AnnotationAbstract, attribute_type::AttributeTypeAnnotation,
                entity_type::EntityTypeAnnotation, type_manager::TypeManager, Ordering, OwnerAPI,
            },
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
            thing_manager: &ThingManager,
        ) -> ((TypeAnnotation, TypeAnnotation, TypeAnnotation), (TypeAnnotation, TypeAnnotation, TypeAnnotation))
        {
            // dog sub animal, owns dog-name; cat sub animal owns cat-name;
            // cat-name sub animal-name; dog-name sub animal-name;
            let mut snapshot = snapshot_;

            // Attributes
            let name = type_manager.create_attribute_type(&mut snapshot, &Label::build(LABEL_NAME)).unwrap();
            let catname = type_manager.create_attribute_type(&mut snapshot, &Label::build(LABEL_CATNAME)).unwrap();
            let dogname = type_manager.create_attribute_type(&mut snapshot, &Label::build(LABEL_DOGNAME)).unwrap();
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
            let animal = type_manager.create_entity_type(&mut snapshot, &Label::build(LABEL_ANIMAL)).unwrap();
            let cat = type_manager.create_entity_type(&mut snapshot, &Label::build(LABEL_CAT)).unwrap();
            let dog = type_manager.create_entity_type(&mut snapshot, &Label::build(LABEL_DOG)).unwrap();
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
