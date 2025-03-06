/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{HashMap, HashSet},
    iter::zip,
    sync::Arc,
};

use bytes::{byte_array::ByteArray, Bytes};
use compiler::annotation::function::{annotate_stored_functions, AnnotatedSchemaFunctions};
use concept::type_::type_manager::TypeManager;
use encoding::{
    graph::{
        definition::{
            definition_key::DefinitionKey, definition_key_generator::DefinitionKeyGenerator,
            function::FunctionDefinition,
        },
        type_::index::NameToFunctionDefinitionIndex,
    },
    Keyable,
};
use ir::{
    pattern::{conjunction::Conjunction, constraint::Constraint, nested_pattern::NestedPattern},
    pipeline::{
        function::ReturnOperation,
        function_signature::{
            FunctionID, FunctionIDAPI, FunctionSignature, FunctionSignatureIndex, HashMapFunctionSignatureIndex,
        },
        FunctionReadError,
    },
    translation::{
        function::{build_signature, translate_typeql_function},
        pipeline::TranslatedStage,
    },
};
use itertools::Itertools;
use primitive::maybe_owns::MaybeOwns;
use resource::constants::snapshot::BUFFER_VALUE_INLINE;
use storage::{
    key_range::KeyRange,
    snapshot::{ReadableSnapshot, WritableSnapshot},
};
use typeql::common::Spanned;

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
    ) -> Result<Arc<AnnotatedSchemaFunctions>, FunctionError> {
        match self.function_cache.as_ref() {
            None => FunctionCache::build_cache(snapshot, type_manager).map(|cache| cache.get_annotated_functions()),
            Some(cache) => Ok(cache.get_annotated_functions()),
        }
    }

    pub fn finalise(self, snapshot: &impl WritableSnapshot, type_manager: &TypeManager) -> Result<(), FunctionError> {
        let functions = FunctionReader::get_functions_all(snapshot)
            .map_err(|typedb_source| FunctionError::FunctionRetrieval { typedb_source })?;
        // TODO: Optimise: We recompile & redo type-inference on all functions here.
        // Prepare ir
        let function_index =
            HashMapFunctionSignatureIndex::build(functions.iter().map(|f| (f.function_id.clone().into(), &f.parsed)));
        let mut translated = Self::translate_functions(snapshot, &functions, &function_index)?;

        // Run type-inference
        let translated_refs = translated.iter().map(|(id, f)| (id.clone(), f)).collect();
        validate_no_cycles(&translated_refs)?;
        annotate_stored_functions(&mut translated, snapshot, type_manager)
            .map_err(|source| FunctionError::AllFunctionsTypeCheckFailure { typedb_source: source })?;
        Ok(())
    }

    pub fn define_functions<'a>(
        &self,
        snapshot: &mut impl WritableSnapshot,
        definitions: impl Iterator<Item = &'a typeql::Function> + Clone,
    ) -> Result<Vec<SchemaFunction>, FunctionError> {
        let mut functions: Vec<SchemaFunction> = Vec::new();
        for definition in definitions.clone() {
            let definition_key = self
                .definition_key_generator
                .create_function(snapshot)
                .map_err(|source| FunctionError::CreateFunctionEncoding { source })?;
            let function =
                SchemaFunction::build(definition_key, FunctionDefinition::build_ref(definition.unparsed.as_str()))?;
            let index_key = NameToFunctionDefinitionIndex::build(function.name().as_str()).into_storage_key();
            let existing = snapshot.get::<BUFFER_VALUE_INLINE>(index_key.as_reference()).map_err(|source| {
                FunctionError::FunctionRetrieval { typedb_source: FunctionReadError::FunctionRetrieval { source } }
            })?;
            if existing.is_some() {
                Err(FunctionError::FunctionAlreadyExists { name: function.name(), source_span: definition.span() })?;
            } else {
                functions.push(function);
            }
        }

        let buffered =
            HashMapFunctionSignatureIndex::build(functions.iter().map(|f| (f.function_id.clone().into(), &f.parsed)));
        let function_index = ReadThroughFunctionSignatureIndex::new(snapshot, self, buffered);
        // Translate to ensure the function calls are valid references. Type-inference is done at commit-time.
        Self::translate_functions(snapshot, &functions, &function_index)?;
        for (function, definition) in zip(functions.iter(), definitions.clone()) {
            let index_key = NameToFunctionDefinitionIndex::build(function.name().as_str()).into_storage_key();
            let definition_key = &function.function_id;
            snapshot.put_val(index_key.into_owned_array(), ByteArray::copy(definition_key.bytes()));
            snapshot.put_val(
                definition_key.clone().into_storage_key().into_owned_array(),
                FunctionDefinition::build_ref(definition.unparsed.as_str()).into_bytes().into_array(),
            );
        }
        Ok(functions)
    }

    pub fn undefine_function(&self, snapshot: &mut impl WritableSnapshot, name: &str) -> Result<(), FunctionError> {
        let definition_key = match self.get_function_key(snapshot, name) {
            Err(typedb_source) => Err(FunctionError::FunctionRetrieval { typedb_source }),
            Ok(None) => Err(FunctionError::FunctionNotFound {}),
            Ok(Some(key)) => Ok(key),
        }?;
        snapshot.delete(definition_key.into_storage_key().into_owned_array());
        let index_key = NameToFunctionDefinitionIndex::build(name);
        snapshot.delete(index_key.into_storage_key().into_owned_array());

        Ok(())
    }

    pub fn redefine_function(
        &self,
        snapshot: &mut impl WritableSnapshot,
        definition: &typeql::Function,
    ) -> Result<SchemaFunction, FunctionError> {
        // TODO: Better query time checking. Maybe redefine all functions at once.
        let definition_key = match self.get_function_key(snapshot, definition.signature.ident.as_str_unchecked()) {
            Err(typedb_source) => Err(FunctionError::FunctionRetrieval { typedb_source }),
            Ok(None) => Err(FunctionError::FunctionNotFound {}),
            Ok(Some(key)) => Ok(key),
        }?;
        let functions =
            [SchemaFunction::build(definition_key, FunctionDefinition::build_ref(definition.unparsed.as_str()))?];
        let buffered =
            HashMapFunctionSignatureIndex::build(functions.iter().map(|f| (f.function_id.clone().into(), &f.parsed)));
        let function_index = ReadThroughFunctionSignatureIndex::new(snapshot, self, buffered);
        // Translate to ensure the function calls are valid references. Type-inference is done at commit-time.
        Self::translate_functions(snapshot, &functions, &function_index)?;
        for (function, definition) in zip(functions.iter(), [definition].iter()) {
            let index_key = NameToFunctionDefinitionIndex::build(function.name().as_str()).into_storage_key();
            let definition_key = &function.function_id;
            snapshot.put_val(index_key.into_owned_array(), ByteArray::copy(definition_key.bytes()));
            snapshot.put_val(
                definition_key.clone().into_storage_key().into_owned_array(),
                FunctionDefinition::build_ref(definition.unparsed.as_str()).into_bytes().into_array(),
            );
        }
        let [function] = functions;
        Ok(function)
    }

    pub(crate) fn translate_functions(
        snapshot: &impl ReadableSnapshot,
        functions: &[SchemaFunction],
        function_index: &impl FunctionSignatureIndex,
    ) -> Result<HashMap<DefinitionKey, ir::pipeline::function::Function>, FunctionError> {
        functions
            .iter()
            .map(|function| {
                translate_typeql_function(snapshot, function_index, &function.parsed)
                    .map(|translated| (function.function_id.clone(), translated))
            })
            .try_collect()
            .map_err(|err| FunctionError::FunctionTranslation { typedb_source: *err })
    }

    pub fn get_function_key(
        &self,
        snapshot: &impl ReadableSnapshot,
        name: &str,
    ) -> Result<Option<DefinitionKey>, FunctionReadError> {
        if let Some(cache) = &self.function_cache {
            Ok(cache.get_function_key(name))
        } else {
            Ok(FunctionReader::get_function_key(snapshot, name)?)
        }
    }

    pub fn get_function(
        &self,
        snapshot: &impl ReadableSnapshot,
        definition_key: DefinitionKey,
        name: &str,
    ) -> Result<MaybeOwns<SchemaFunction>, FunctionReadError> {
        if let Some(cache) = &self.function_cache {
            Ok(MaybeOwns::Borrowed(cache.get_function(definition_key).unwrap()))
        } else {
            Ok(MaybeOwns::Owned(FunctionReader::get_function(snapshot, definition_key, name)?))
        }
    }
}

pub struct FunctionReader {}

impl FunctionReader {
    pub(crate) fn get_functions_all(
        snapshot: &impl ReadableSnapshot,
    ) -> Result<Vec<SchemaFunction>, FunctionReadError> {
        snapshot
            .iterate_range(&KeyRange::new_within(
                DefinitionKey::build_prefix(FunctionDefinition::PREFIX),
                DefinitionKey::FIXED_WIDTH_ENCODING,
            ))
            .collect_cloned_vec(|key, value| {
                SchemaFunction::build(
                    DefinitionKey::new(Bytes::Reference(key.bytes()).into_owned()),
                    FunctionDefinition::new(Bytes::Reference(value).into_owned()),
                )
                .unwrap()
            })
            .map_err(|source| FunctionReadError::FunctionsScan { source })
    }

    pub(crate) fn get_function_key(
        snapshot: &impl ReadableSnapshot,
        name: &str,
    ) -> Result<Option<DefinitionKey>, FunctionReadError> {
        let index_key = NameToFunctionDefinitionIndex::build(name);
        let bytes_opt = snapshot
            .get(index_key.into_storage_key().as_reference())
            .map_err(|source| FunctionReadError::FunctionRetrieval { source })?;
        Ok(bytes_opt.map(|bytes| DefinitionKey::new(Bytes::Array(bytes))))
    }

    pub(crate) fn get_function(
        snapshot: &impl ReadableSnapshot,
        definition_key: DefinitionKey,
        name: &str,
    ) -> Result<SchemaFunction, FunctionReadError> {
        snapshot
            .get::<BUFFER_VALUE_INLINE>(definition_key.clone().into_storage_key().as_reference())
            .map_err(|source| FunctionReadError::FunctionRetrieval { source })?
            .map_or(
                Err(FunctionReadError::FunctionIDNotFound { name: name.to_owned(), id: FunctionID::Schema(definition_key.clone()) }),
                |bytes| Ok(SchemaFunction::build(definition_key, FunctionDefinition::new(Bytes::Array(bytes))).unwrap()),
            )
    }
}

pub fn validate_no_cycles<ID: FunctionIDAPI>(
    functions: &HashMap<ID, &ir::pipeline::function::Function>,
) -> Result<(), FunctionError> {
    let mut active = HashMap::new();
    let mut complete = HashSet::new();
    for id in functions.keys() {
        debug_assert!(active.is_empty());
        if !complete.contains(id) {
            validate_no_cycles_impl(
                id.clone(),
                functions,
                &mut active,
                &mut complete,
                StratumAndDepth { stratum: 0, depth: 0 },
            )?;
            debug_assert!(complete.contains(id));
        }
    }
    Ok(())
}

#[derive(Clone, Copy)]
struct StratumAndDepth {
    stratum: usize,
    depth: usize,
}
impl StratumAndDepth {
    fn add(&self, stratum_increment: usize, depth_increment: usize) -> Self {
        StratumAndDepth { stratum: self.stratum + stratum_increment, depth: self.depth + depth_increment }
    }
}
fn validate_no_cycles_impl<ID: FunctionIDAPI + Ord + Eq>(
    id: ID,
    functions: &HashMap<ID, &ir::pipeline::function::Function>,
    active: &mut HashMap<ID, StratumAndDepth>,
    complete: &mut HashSet<ID>,
    current_stratum: StratumAndDepth,
) -> Result<(), FunctionError> {
    if complete.contains(&id) {
        return Ok(());
    }
    if let Some(StratumAndDepth { stratum, depth }) = active.get(&id) {
        if *stratum < current_stratum.stratum {
            let ids_by_depth = active.iter().map(|(id, sd)| (sd.depth, id)).sorted().collect::<Vec<_>>();
            debug_assert!(ids_by_depth[*depth].1 == &id);
            let cycle_names =
                (*depth..ids_by_depth.len()).map(|i| functions.get(ids_by_depth[i].1).unwrap().name.clone()).join(", ");
            return Err(FunctionError::StratificationViolation { cycle_names });
        } else {
            return Ok(());
        }
    }

    active.insert(id.clone(), current_stratum);
    let function = functions.get(&id).unwrap();
    for called_id in negated_function_calls(function) {
        validate_no_cycles_impl(called_id, functions, active, complete, current_stratum.add(1, 1))?;
    }

    let has_aggregate_stage = function.function_body.stages.iter().any(|stage| {
        matches!(
            stage,
            TranslatedStage::Sort(_)
                | TranslatedStage::Distinct(_)
                | TranslatedStage::Offset(_)
                | TranslatedStage::Limit(_)
                | TranslatedStage::Reduce(_)
        )
    });
    let has_aggregate_return = match function.function_body.return_operation {
        ReturnOperation::Stream(_, _) | ReturnOperation::Single(_, _, _) => false,
        ReturnOperation::ReduceCheck(_) | ReturnOperation::ReduceReducer(_, _) => true,
    };
    let unnegated_stratum =
        if has_aggregate_stage || has_aggregate_return { current_stratum.add(1, 1) } else { current_stratum.add(0, 1) };
    for called_id in unnegated_function_calls(function) {
        validate_no_cycles_impl(called_id, functions, active, complete, unnegated_stratum)?;
    }
    active.remove(&id);

    complete.insert(id);
    Ok(())
}

fn negated_function_calls<ID: FunctionIDAPI>(function: &ir::pipeline::function::Function) -> impl Iterator<Item = ID> {
    let mut calls = Vec::new();
    for stage in &function.function_body.stages {
        if let TranslatedStage::Match { block, .. } = stage {
            collect_negated_function_calls(block.conjunction(), &mut calls, false)
        }
    }
    calls.into_iter()
}

fn collect_negated_function_calls<ID: FunctionIDAPI>(conjunction: &Conjunction, calls: &mut Vec<ID>, is_negated: bool) {
    if is_negated {
        conjunction.constraints().iter().for_each(|constraint| {
            if let Constraint::FunctionCallBinding(binding) = constraint {
                let id = binding.function_call().function_id();
                if let Ok(unwrapped_id) = id.try_into() {
                    calls.push(unwrapped_id)
                }
            }
        })
    }

    for pattern in conjunction.nested_patterns() {
        match pattern {
            NestedPattern::Negation(inner) => collect_negated_function_calls(inner.conjunction(), calls, true),
            NestedPattern::Disjunction(inner) => inner.conjunctions().iter().for_each(|branch| {
                collect_negated_function_calls(branch, calls, is_negated);
            }),
            NestedPattern::Optional(inner) => collect_negated_function_calls(inner.conjunction(), calls, is_negated),
        }
    }
}

fn unnegated_function_calls<ID: FunctionIDAPI>(
    function: &ir::pipeline::function::Function,
) -> impl Iterator<Item = ID> {
    let mut calls = Vec::new();
    for stage in &function.function_body.stages {
        if let TranslatedStage::Match { block, .. } = stage {
            collect_unnegated_function_calls(block.conjunction(), &mut calls)
        }
    }
    calls.into_iter()
}

fn collect_unnegated_function_calls<ID: FunctionIDAPI>(conjunction: &Conjunction, calls: &mut Vec<ID>) {
    conjunction.constraints().iter().for_each(|constraint| {
        if let Constraint::FunctionCallBinding(binding) = constraint {
            let id = binding.function_call().function_id();
            if let Ok(unwrapped_id) = id.try_into() {
                calls.push(unwrapped_id)
            }
        }
    });

    for pattern in conjunction.nested_patterns() {
        match pattern {
            NestedPattern::Negation(_) => {}
            NestedPattern::Disjunction(inner) => inner.conjunctions().iter().for_each(|branch| {
                collect_unnegated_function_calls(branch, calls);
            }),
            NestedPattern::Optional(inner) => collect_unnegated_function_calls(inner.conjunction(), calls),
        }
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

impl<Snapshot: ReadableSnapshot> FunctionSignatureIndex for ReadThroughFunctionSignatureIndex<'_, Snapshot> {
    fn get_function_signature(
        &self,
        name: &str,
    ) -> Result<Option<MaybeOwns<'_, FunctionSignature>>, FunctionReadError> {
        Ok(if let Some(signature) = self.buffered.get_function_signature(name)? {
            Some(signature)
        } else if let Some(key) = self.function_manager.get_function_key(self.snapshot, name)? {
            let function = self.function_manager.get_function(self.snapshot, key, name)?;
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
        let functions_to_define = ["
        fun cat_names($c: animal) -> { name } :
            match
                $c has cat-name $n;
            return { $n };
        "];
        let expected_name = "cat_names";

        let expected_function_id = DefinitionKey::build(Prefix::DefinitionFunction, DefinitionID::build(0));
        let expected_signature = FunctionSignature::new(
            FunctionID::Schema(expected_function_id.clone()),
            vec![VariableCategory::Object],
            vec![(VariableCategory::Object, VariableOptionality::Required)],
            true,
        );
        let parsed =
            functions_to_define.iter().map(|f| typeql::parse_definition_function(f).unwrap()).collect::<Vec<_>>();
        let sequence_number = {
            let function_manager = FunctionManager::new(Arc::new(DefinitionKeyGenerator::new()), None);
            let mut snapshot = storage.clone().open_snapshot_write();
            let stored_functions = function_manager.define_functions(&mut snapshot, parsed.iter()).unwrap();
            // Read buffered
            assert_eq!(expected_function_id, stored_functions[0].function_id());

            assert_eq!(
                expected_function_id,
                function_manager.get_function_key(&snapshot, expected_name).unwrap().unwrap()
            );
            assert_eq!(
                expected_name,
                function_manager
                    .get_function(&snapshot, expected_function_id.clone(), expected_name)
                    .unwrap()
                    .name()
                    .as_str()
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
                function_manager
                    .get_function(&snapshot, expected_function_id.clone(), expected_name)
                    .unwrap()
                    .name()
                    .as_str()
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
            assert_eq!(&Arc::new(BTreeSet::from([type_cat])), var_c_annotations);
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
            let name = type_manager.create_attribute_type(&mut snapshot, &Label::build(LABEL_NAME, None)).unwrap();
            let catname =
                type_manager.create_attribute_type(&mut snapshot, &Label::build(LABEL_CATNAME, None)).unwrap();
            let dogname =
                type_manager.create_attribute_type(&mut snapshot, &Label::build(LABEL_DOGNAME, None)).unwrap();
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
            let animal = type_manager.create_entity_type(&mut snapshot, &Label::build(LABEL_ANIMAL, None)).unwrap();
            let cat = type_manager.create_entity_type(&mut snapshot, &Label::build(LABEL_CAT, None)).unwrap();
            let dog = type_manager.create_entity_type(&mut snapshot, &Label::build(LABEL_DOG, None)).unwrap();
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
