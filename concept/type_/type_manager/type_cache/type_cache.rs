/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{HashMap, HashSet},
    error::Error,
    fmt,
    sync::Arc,
};

use encoding::{
    graph::definition::{definition_key::DefinitionKey, r#struct::StructDefinition},
    value::{label::Label, value_type::ValueType},
};
use storage::{sequence_number::SequenceNumber, MVCCStorage};

use crate::type_::{
    annotation::AnnotationIndependent,
    attribute_type::{AttributeType, AttributeTypeAnnotation},
    entity_type::EntityType,
    object_type::ObjectType,
    owns::{Owns, OwnsAnnotation},
    plays::{Plays, PlaysAnnotation},
    relates::{Relates, RelatesAnnotation},
    relation_type::RelationType,
    role_type::RoleType,
    type_manager::type_cache::{
        kind_cache::{
            AttributeTypeCache, CommonTypeCache, EntityTypeCache, ObjectCache, OwnsCache, PlaysCache, RelatesCache,
            RelationTypeCache, RoleTypeCache,
        },
        selection,
        selection::{CacheGetter, HasCommonTypeCache, HasObjectCache},
        struct_definition_cache::StructDefinitionCache,
    },
    KindAPI, Ordering, OwnerAPI, PlayerAPI, TypeAPI,
};

// TODO: could/should we slab allocate the schema cache?
#[derive(Debug)]
pub struct TypeCache {
    open_sequence_number: SequenceNumber,

    // Types that are borrowable and returned from the cache
    entity_types: Box<[Option<EntityTypeCache>]>,
    relation_types: Box<[Option<RelationTypeCache>]>,
    role_types: Box<[Option<RoleTypeCache>]>,
    attribute_types: Box<[Option<AttributeTypeCache>]>,

    owns: HashMap<Owns<'static>, OwnsCache>,
    plays: HashMap<Plays<'static>, PlaysCache>,
    relates: HashMap<Relates<'static>, RelatesCache>,

    struct_definitions: Box<[Option<StructDefinitionCache>]>,

    entity_types_index_label: HashMap<Label<'static>, EntityType<'static>>,
    relation_types_index_label: HashMap<Label<'static>, RelationType<'static>>,
    role_types_index_label: HashMap<Label<'static>, RoleType<'static>>,
    attribute_types_index_label: HashMap<Label<'static>, AttributeType<'static>>,
    struct_definition_index_by_name: HashMap<String, DefinitionKey<'static>>,

    role_types_by_name: HashMap<String, Vec<RoleType<'static>>>,
    // specific caches to simplify architectures
    independent_attribute_types: Arc<HashSet<AttributeType<'static>>>,
}

selection::impl_cache_getter!(EntityTypeCache, EntityType, entity_types);
selection::impl_cache_getter!(AttributeTypeCache, AttributeType, attribute_types);
selection::impl_cache_getter!(RelationTypeCache, RelationType, relation_types);
selection::impl_cache_getter!(RoleTypeCache, RoleType, role_types);

selection::impl_has_common_type_cache!(EntityTypeCache, EntityType<'static>);
selection::impl_has_common_type_cache!(AttributeTypeCache, AttributeType<'static>);
selection::impl_has_common_type_cache!(RelationTypeCache, RelationType<'static>);
selection::impl_has_common_type_cache!(RoleTypeCache, RoleType<'static>);

selection::impl_has_object_cache!(EntityTypeCache, EntityType<'static>);
selection::impl_has_object_cache!(RelationTypeCache, RelationType<'static>);

impl TypeCache {
    // If creation becomes slow, We should restore pre-fetching of the schema
    //  with a single pass on disk (as it was in 1f339733feaf4542e47ff604462f107d2ade1f1a)
    pub fn new<D>(
        storage: Arc<MVCCStorage<D>>,
        open_sequence_number: SequenceNumber,
    ) -> Result<Self, TypeCacheCreateError> {
        // note: since we will parse out many heterogenous properties/edges from the schema, we will scan once into a vector,
        //       then go through it again to pull out the type information.

        let snapshot = storage.open_snapshot_read_at(open_sequence_number);

        let entity_type_caches = EntityTypeCache::create(&snapshot);
        let relation_type_caches = RelationTypeCache::create(&snapshot);
        let role_type_caches = RoleTypeCache::create(&snapshot);
        let attribute_type_caches = AttributeTypeCache::create(&snapshot);
        let struct_definition_caches = StructDefinitionCache::create(&snapshot);

        let entity_types_index_label = Self::build_label_to_type_index(&entity_type_caches);
        let relation_types_index_label = Self::build_label_to_type_index(&relation_type_caches);
        let role_types_index_label = Self::build_label_to_type_index(&role_type_caches);
        let attribute_types_index_label = Self::build_label_to_type_index(&attribute_type_caches);
        let struct_definition_index_by_name = Self::build_name_to_struct_definition_index(&struct_definition_caches);

        let independent_attribute_types = attribute_type_caches
            .iter()
            .filter_map(|cache| cache.as_ref())
            .filter_map(|cache| {
                if cache
                    .common_type_cache()
                    .annotations
                    .contains_key(&AttributeTypeAnnotation::Independent(AnnotationIndependent {}))
                {
                    Some(cache.common_type_cache.type_.clone())
                } else {
                    None
                }
            })
            .collect();

        let mut role_types_by_name = HashMap::new();
        for (label, role_type) in &role_types_index_label {
            if !role_types_by_name.contains_key(label.name.as_str()) {
                role_types_by_name.insert(label.name.as_str().to_owned(), Vec::new());
            }
            role_types_by_name.get_mut(label.name.as_str()).unwrap().push(role_type.clone());
        }

        Ok(TypeCache {
            open_sequence_number,
            entity_types: entity_type_caches,
            relation_types: relation_type_caches,
            role_types: role_type_caches,
            attribute_types: attribute_type_caches,
            owns: OwnsCache::create(&snapshot),
            plays: PlaysCache::create(&snapshot),
            relates: RelatesCache::create(&snapshot),
            struct_definitions: struct_definition_caches,

            entity_types_index_label,
            relation_types_index_label,
            role_types_index_label,
            attribute_types_index_label,
            struct_definition_index_by_name,
            role_types_by_name,

            independent_attribute_types: Arc::new(independent_attribute_types),
        })
    }

    fn build_label_to_type_index<T: KindAPI<'static>, Cache: HasCommonTypeCache<T>>(
        type_cache_array: &[Option<Cache>],
    ) -> HashMap<Label<'static>, T> {
        type_cache_array
            .iter()
            .flatten()
            .map(|cache| (cache.common_type_cache().label.clone(), cache.common_type_cache().type_.clone()))
            .collect()
    }

    fn build_name_to_struct_definition_index(
        struct_cache_array: &[Option<StructDefinitionCache>],
    ) -> HashMap<String, DefinitionKey<'static>> {
        struct_cache_array
            .iter()
            .flatten()
            .map(|cache| (cache.definition.name.clone(), cache.definition_key.clone()))
            .collect()
    }

    pub(crate) fn get_object_type(&self, label: &Label<'_>) -> Option<ObjectType<'static>> {
        (self.get_entity_type(label).map(ObjectType::Entity))
            .or_else(|| self.get_relation_type(label).map(ObjectType::Relation))
    }

    pub(crate) fn get_entity_type(&self, label: &Label<'_>) -> Option<EntityType<'static>> {
        self.entity_types_index_label.get(label).cloned()
    }

    pub(crate) fn get_relation_type(&self, label: &Label<'_>) -> Option<RelationType<'static>> {
        self.relation_types_index_label.get(label).cloned()
    }

    pub(crate) fn get_role_type(&self, label: &Label<'_>) -> Option<RoleType<'static>> {
        self.role_types_index_label.get(label).cloned()
    }

    pub(crate) fn get_roles_by_name(&self, name: &str) -> Option<&Vec<RoleType<'static>>> {
        self.role_types_by_name.get(name)
    }

    pub(crate) fn get_attribute_type(&self, label: &Label<'_>) -> Option<AttributeType<'static>> {
        self.attribute_types_index_label.get(label).cloned()
    }

    pub(crate) fn get_object_types(&self) -> Vec<ObjectType<'static>> {
        let entities = self.get_entity_types().into_iter().map(ObjectType::Entity);
        let relatiions = self.get_relation_types().into_iter().map(ObjectType::Relation);
        entities.chain(relatiions).collect()
    }

    pub(crate) fn get_entity_types(&self) -> Vec<EntityType<'static>> {
        self.entity_types.iter().flatten().map(|cache| cache.common_type_cache().type_.clone()).collect()
    }

    pub(crate) fn get_relation_types(&self) -> Vec<RelationType<'static>> {
        self.relation_types.iter().flatten().map(|cache| cache.common_type_cache().type_.clone()).collect()
    }

    pub(crate) fn get_attribute_types(&self) -> Vec<AttributeType<'static>> {
        self.attribute_types.iter().flatten().map(|cache| cache.common_type_cache().type_.clone()).collect()
    }

    pub(crate) fn get_role_types(&self) -> Vec<RoleType<'static>> {
        self.role_types.iter().filter_map(Option::as_ref).map(|cache| cache.common_type_cache().type_.clone()).collect()
    }

    pub(crate) fn get_supertype<'a, 'this, T, CACHE>(&'this self, type_: T) -> Option<T::SelfStatic>
    where
        T: KindAPI<'a> + CacheGetter<CacheType = CACHE>,
        CACHE: HasCommonTypeCache<T::SelfStatic> + 'this,
    {
        Some(T::get_cache(self, type_).common_type_cache().supertype.as_ref()?.clone())
    }

    pub(crate) fn get_supertypes_transitive<'a, 'this, T, CACHE>(&'this self, type_: T) -> &'this Vec<T::SelfStatic>
    where
        T: KindAPI<'a> + CacheGetter<CacheType = CACHE>,
        CACHE: HasCommonTypeCache<T::SelfStatic> + 'this,
    {
        &T::get_cache(self, type_).common_type_cache().supertypes
    }

    pub(crate) fn get_subtypes<'a, 'this, T, CACHE>(&'this self, type_: T) -> &'this Vec<T::SelfStatic>
    where
        T: KindAPI<'a> + CacheGetter<CacheType = CACHE>,
        CACHE: HasCommonTypeCache<T::SelfStatic> + 'this,
    {
        &T::get_cache(self, type_).common_type_cache().subtypes
    }

    pub(crate) fn get_subtypes_transitive<'a, 'this, T, CACHE>(&'this self, type_: T) -> &'this Vec<T::SelfStatic>
    where
        T: KindAPI<'a> + CacheGetter<CacheType = CACHE>,
        CACHE: HasCommonTypeCache<T::SelfStatic> + 'this,
    {
        &T::get_cache(self, type_).common_type_cache().subtypes_transitive
    }

    pub(crate) fn get_label<'a, 'this, T, CACHE>(&'this self, type_: T) -> &'this Label<'static>
    where
        T: KindAPI<'a> + CacheGetter<CacheType = CACHE>,
        CACHE: HasCommonTypeCache<T::SelfStatic> + 'this,
    {
        &T::get_cache(self, type_).common_type_cache().label
    }

    pub(crate) fn get_annotations_declared<'a, 'this, T, CACHE>(
        &'this self,
        type_: T,
    ) -> &HashSet<<<T as TypeAPI<'a>>::SelfStatic as KindAPI<'static>>::AnnotationType>
    where
        T: KindAPI<'a> + CacheGetter<CacheType = CACHE>,
        CACHE: HasCommonTypeCache<T::SelfStatic> + 'this,
    {
        &T::get_cache(self, type_).common_type_cache().annotations_declared
    }

    pub(crate) fn get_annotations<'a, 'this, T, CACHE>(
        &'this self,
        type_: T,
    ) -> &HashMap<<<T as TypeAPI<'a>>::SelfStatic as KindAPI<'static>>::AnnotationType, <T as TypeAPI<'a>>::SelfStatic>
    where
        T: KindAPI<'a> + CacheGetter<CacheType = CACHE>,
        CACHE: HasCommonTypeCache<T::SelfStatic> + 'this,
    {
        &T::get_cache(self, type_).common_type_cache().annotations
    }

    pub(crate) fn get_owns_for_attribute_type_declared(
        &self,
        attribute_type: AttributeType<'_>,
    ) -> &HashSet<Owns<'static>> {
        &AttributeType::get_cache(self, attribute_type).owns_declared
    }

    pub(crate) fn get_owns_for_attribute_type(
        &self,
        attribute_type: AttributeType<'_>,
    ) -> &HashMap<ObjectType<'static>, Owns<'static>> {
        &AttributeType::get_cache(self, attribute_type).owns
    }

    pub(crate) fn get_owns_declared<'a, 'this, T, CACHE>(&'this self, type_: T) -> &HashSet<Owns<'static>>
    where
        T: OwnerAPI<'a> + PlayerAPI<'a> + CacheGetter<CacheType = CACHE>,
        CACHE: HasObjectCache + 'this,
    {
        &T::get_cache(self, type_).object_cache().owns_declared
    }

    pub(crate) fn get_owns<'a, 'this, T, CACHE>(&'this self, type_: T) -> &HashSet<Owns<'static>>
    where
        T: OwnerAPI<'a> + PlayerAPI<'a> + CacheGetter<CacheType = CACHE>,
        CACHE: HasObjectCache + 'this,
    {
        &T::get_cache(self, type_).object_cache().owns
    }

    pub(crate) fn get_object_owns_overrides<'a, 'this, T, CACHE>(
        &'this self,
        type_: T,
    ) -> &HashMap<Owns<'static>, Owns<'static>>
    where
        T: OwnerAPI<'a> + PlayerAPI<'a> + CacheGetter<CacheType = CACHE>,
        CACHE: HasObjectCache + 'this,
    {
        &T::get_cache(self, type_).object_cache().owns_overrides
    }

    pub(crate) fn get_role_type_ordering(&self, role_type: RoleType<'_>) -> Ordering {
        RoleType::get_cache(self, role_type).ordering
    }

    pub(crate) fn get_role_type_relates_declared(&self, role_type: RoleType<'_>) -> &Relates<'static> {
        &RoleType::get_cache(self, role_type).relates_declared
    }

    pub(crate) fn get_role_type_relates(&self, role_type: RoleType<'_>) -> &HashSet<Relates<'static>> {
        &RoleType::get_cache(self, role_type).relates
    }

    pub(crate) fn get_relation_type_relates_declared(
        &self,
        relation_type: RelationType<'_>,
    ) -> &HashSet<Relates<'static>> {
        &RelationType::get_cache(self, relation_type).relates_declared
    }

    pub(crate) fn get_relation_type_relates(&self, relation_type: RelationType<'_>) -> &HashSet<Relates<'static>> {
        &RelationType::get_cache(self, relation_type).relates
    }

    pub(crate) fn get_relates_annotations_declared<'c>(
        &'c self,
        relates: Relates<'c>,
    ) -> &'c HashSet<RelatesAnnotation> {
        &self.relates.get(&relates).unwrap().annotations_declared
    }

    pub(crate) fn get_relates_annotations<'c>(
        &'c self,
        relates: Relates<'c>,
    ) -> &'c HashMap<RelatesAnnotation, Relates<'static>> {
        &self.relates.get(&relates).unwrap().annotations
    }

    pub(crate) fn get_relates_override<'c>(&'c self, relates: Relates<'c>) -> &'c Option<Relates<'static>> {
        &self.relates.get(&relates).unwrap().overrides
    }

    pub(crate) fn get_relates_overriding<'c>(&'c self, relates: Relates<'c>) -> &'c HashSet<Relates<'static>> {
        &self.relates.get(&relates).unwrap().overriding
    }

    pub(crate) fn get_relates_overriding_transitive<'c>(
        &'c self,
        relates: Relates<'c>,
    ) -> &'c HashSet<Relates<'static>> {
        &self.relates.get(&relates).unwrap().overriding_transitive
    }

    pub(crate) fn get_plays_for_role_type_declared(&self, role_type: RoleType<'_>) -> &HashSet<Plays<'static>> {
        &RoleType::get_cache(self, role_type).plays_declared
    }

    pub(crate) fn get_plays_for_role_type(
        &self,
        role_type: RoleType<'_>,
    ) -> &HashMap<ObjectType<'static>, Plays<'static>> {
        &RoleType::get_cache(self, role_type).plays
    }

    pub(crate) fn get_plays_declared<'a, 'this, T, CACHE>(&'this self, type_: T) -> &HashSet<Plays<'static>>
    where
        T: OwnerAPI<'a> + PlayerAPI<'a> + CacheGetter<CacheType = CACHE>,
        CACHE: HasObjectCache + 'this,
    {
        &T::get_cache(self, type_).object_cache().plays_declared
    }

    pub(crate) fn get_plays<'a, 'this, T, CACHE>(&'this self, type_: T) -> &'this HashSet<Plays<'static>>
    where
        T: OwnerAPI<'a> + PlayerAPI<'a> + CacheGetter<CacheType = CACHE>,
        CACHE: HasObjectCache + 'this,
    {
        &T::get_cache(self, type_).object_cache().plays
    }

    pub(crate) fn get_object_plays_overrides<'a, 'this, T, CACHE>(
        &'this self,
        type_: T,
    ) -> &'this HashMap<Plays<'static>, Plays<'static>>
    where
        T: OwnerAPI<'a> + PlayerAPI<'a> + CacheGetter<CacheType = CACHE>,
        CACHE: HasObjectCache + 'this,
    {
        &T::get_cache(self, type_).object_cache().plays_overrides
    }

    pub(crate) fn get_plays_override<'c>(&'c self, plays: Plays<'c>) -> &'c Option<Plays<'static>> {
        &self.plays.get(&plays).unwrap().overrides
    }

    pub(crate) fn get_plays_overriding<'c>(&'c self, plays: Plays<'c>) -> &'c HashSet<Plays<'static>> {
        &self.plays.get(&plays).unwrap().overriding
    }

    pub(crate) fn get_plays_overriding_transitive<'c>(&'c self, plays: Plays<'c>) -> &'c HashSet<Plays<'static>> {
        &self.plays.get(&plays).unwrap().overriding_transitive
    }

    pub(crate) fn get_plays_annotations_declared<'c>(&'c self, plays: Plays<'c>) -> &'c HashSet<PlaysAnnotation> {
        &self.plays.get(&plays).unwrap().annotations_declared
    }

    pub(crate) fn get_plays_annotations<'c>(
        &'c self,
        plays: Plays<'c>,
    ) -> &'c HashMap<PlaysAnnotation, Plays<'static>> {
        &self.plays.get(&plays).unwrap().annotations
    }

    pub(crate) fn get_attribute_type_value_type_declared(
        &self,
        attribute_type: AttributeType<'_>,
    ) -> &Option<ValueType> {
        &AttributeType::get_cache(self, attribute_type).value_type_declared
    }

    pub(crate) fn get_attribute_type_value_type(
        &self,
        attribute_type: AttributeType<'_>,
    ) -> &Option<(ValueType, AttributeType<'static>)> {
        &AttributeType::get_cache(self, attribute_type).value_type
    }

    pub(crate) fn get_owns_annotations_declared<'c>(&'c self, owns: Owns<'c>) -> &'c HashSet<OwnsAnnotation> {
        &self.owns.get(&owns).unwrap().annotations_declared
    }

    pub(crate) fn get_owns_annotations<'c>(&'c self, owns: Owns<'c>) -> &'c HashMap<OwnsAnnotation, Owns<'static>> {
        &self.owns.get(&owns).unwrap().annotations
    }

    pub(crate) fn get_owns_ordering<'c>(&'c self, owns: Owns<'c>) -> Ordering {
        self.owns.get(&owns).unwrap().ordering
    }

    pub(crate) fn get_owns_override<'c>(&'c self, owns: Owns<'c>) -> &'c Option<Owns<'static>> {
        &self.owns.get(&owns).unwrap().overrides
    }

    pub(crate) fn get_owns_overriding<'c>(&'c self, owns: Owns<'c>) -> &'c HashSet<Owns<'static>> {
        &self.owns.get(&owns).unwrap().overriding
    }

    pub(crate) fn get_owns_overriding_transitive<'c>(&'c self, owns: Owns<'c>) -> &'c HashSet<Owns<'static>> {
        &self.owns.get(&owns).unwrap().overriding_transitive
    }

    pub(crate) fn get_struct_definition_key(&self, label: &str) -> Option<DefinitionKey<'static>> {
        self.struct_definition_index_by_name.get(label).cloned()
    }

    pub(crate) fn get_struct_definition(&self, definition_key: DefinitionKey<'static>) -> &StructDefinition {
        &self.struct_definitions[definition_key.definition_id().as_uint() as usize].as_ref().unwrap().definition
    }

    pub(crate) fn get_independent_attribute_types(&self) -> Arc<HashSet<AttributeType<'static>>> {
        self.independent_attribute_types.clone()
    }
}

#[derive(Debug, Clone)]
pub enum TypeCacheCreateError {}

impl fmt::Display for TypeCacheCreateError {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl Error for TypeCacheCreateError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        todo!()
    }
}
