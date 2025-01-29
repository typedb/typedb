/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use encoding::{
    graph::definition::{definition_key::DefinitionKey, r#struct::StructDefinition},
    value::{label::Label, value_type::ValueType},
};
use error::typedb_error;
use storage::{sequence_number::SequenceNumber, MVCCStorage};

use crate::type_::{
    attribute_type::AttributeType,
    constraint::{CapabilityConstraint, Constraint, ConstraintCategory, TypeConstraint},
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
    KindAPI, Ordering, OwnerAPI, PlayerAPI,
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

    owns: HashMap<Owns, OwnsCache>,
    plays: HashMap<Plays, PlaysCache>,
    relates: HashMap<Relates, RelatesCache>,

    struct_definitions: Box<[Option<StructDefinitionCache>]>,

    entity_types_index_label: HashMap<Arc<Label>, EntityType>,
    relation_types_index_label: HashMap<Arc<Label>, RelationType>,
    role_types_index_label: HashMap<Arc<Label>, RoleType>,
    attribute_types_index_label: HashMap<Arc<Label>, AttributeType>,
    struct_definition_index_by_name: HashMap<String, DefinitionKey>,

    role_types_by_name: HashMap<String, Vec<RoleType>>,
    // specific caches to simplify architectures
    independent_attribute_types: Arc<HashSet<AttributeType>>,
}

selection::impl_cache_getter!(EntityTypeCache, EntityType, entity_types);
selection::impl_cache_getter!(AttributeTypeCache, AttributeType, attribute_types);
selection::impl_cache_getter!(RelationTypeCache, RelationType, relation_types);
selection::impl_cache_getter!(RoleTypeCache, RoleType, role_types);

selection::impl_has_common_type_cache!(EntityTypeCache, EntityType);
selection::impl_has_common_type_cache!(AttributeTypeCache, AttributeType);
selection::impl_has_common_type_cache!(RelationTypeCache, RelationType);
selection::impl_has_common_type_cache!(RoleTypeCache, RoleType);

selection::impl_has_object_cache!(EntityTypeCache, EntityType);
selection::impl_has_object_cache!(RelationTypeCache, RelationType);

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
                    .constraints
                    .iter()
                    .map(|constraint| constraint.category())
                    .any(|category| category == ConstraintCategory::Independent)
                {
                    Some(cache.common_type_cache.type_)
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
            role_types_by_name.get_mut(label.name.as_str()).unwrap().push(*role_type);
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

    fn build_label_to_type_index<T: KindAPI, Cache: HasCommonTypeCache<T>>(
        type_cache_array: &[Option<Cache>],
    ) -> HashMap<Arc<Label>, T> {
        type_cache_array
            .iter()
            .flatten()
            .map(|cache| (cache.common_type_cache().label.clone(), cache.common_type_cache().type_))
            .collect()
    }

    fn build_name_to_struct_definition_index(
        struct_cache_array: &[Option<StructDefinitionCache>],
    ) -> HashMap<String, DefinitionKey> {
        struct_cache_array
            .iter()
            .flatten()
            .map(|cache| (cache.definition.name.clone(), cache.definition_key.clone()))
            .collect()
    }

    pub(crate) fn get_object_type(&self, label: &Label) -> Option<ObjectType> {
        (self.get_entity_type(label).map(ObjectType::Entity))
            .or_else(|| self.get_relation_type(label).map(ObjectType::Relation))
    }

    pub(crate) fn get_entity_type(&self, label: &Label) -> Option<EntityType> {
        self.entity_types_index_label.get(label).cloned()
    }

    pub(crate) fn get_relation_type(&self, label: &Label) -> Option<RelationType> {
        self.relation_types_index_label.get(label).cloned()
    }

    pub(crate) fn get_role_type(&self, label: &Label) -> Option<RoleType> {
        self.role_types_index_label.get(label).cloned()
    }

    pub(crate) fn get_roles_by_name(&self, name: &str) -> Option<&[RoleType]> {
        self.role_types_by_name.get(name).map(|v| &**v)
    }

    pub(crate) fn get_attribute_type(&self, label: &Label) -> Option<AttributeType> {
        self.attribute_types_index_label.get(label).cloned()
    }

    pub(crate) fn get_object_types(&self) -> Vec<ObjectType> {
        let entities = self.get_entity_types().into_iter().map(ObjectType::Entity);
        let relatiions = self.get_relation_types().into_iter().map(ObjectType::Relation);
        entities.chain(relatiions).collect()
    }

    pub(crate) fn get_entity_types(&self) -> Vec<EntityType> {
        self.entity_types.iter().flatten().map(|cache| cache.common_type_cache().type_).collect()
    }

    pub(crate) fn get_relation_types(&self) -> Vec<RelationType> {
        self.relation_types.iter().flatten().map(|cache| cache.common_type_cache().type_).collect()
    }

    pub(crate) fn get_attribute_types(&self) -> Vec<AttributeType> {
        self.attribute_types.iter().flatten().map(|cache| cache.common_type_cache().type_).collect()
    }

    pub(crate) fn get_role_types(&self) -> Vec<RoleType> {
        self.role_types.iter().filter_map(Option::as_ref).map(|cache| cache.common_type_cache().type_).collect()
    }

    pub fn get_entity_types_count(&self) -> u64 {
        self.entity_types.len() as u64
    }

    pub fn get_relation_types_count(&self) -> u64 {
        self.relation_types.len() as u64
    }

    pub fn get_attribute_types_count(&self) -> u64 {
        self.attribute_types.len() as u64
    }

    pub fn get_role_types_count(&self) -> u64 {
        self.role_types.len() as u64
    }

    pub fn get_types_count(&self) -> u64 {
        self.get_entity_types_count()
            + self.get_relation_types_count()
            + self.get_attribute_types_count()
            + self.get_role_types_count()
    }

    pub(crate) fn get_supertype<'a, 'this, T, CACHE>(&'this self, type_: T) -> Option<T>
    where
        T: KindAPI + CacheGetter<CacheType = CACHE>,
        CACHE: HasCommonTypeCache<T> + 'this,
    {
        Some(*T::get_cache(self, type_).common_type_cache().supertype.as_ref()?)
    }

    pub(crate) fn get_supertypes_transitive<'a, 'this, T, CACHE>(&'this self, type_: T) -> &'this Vec<T>
    where
        T: KindAPI + CacheGetter<CacheType = CACHE>,
        CACHE: HasCommonTypeCache<T> + 'this,
    {
        &T::get_cache(self, type_).common_type_cache().supertypes_transitive
    }

    pub(crate) fn get_subtypes<'a, 'this, T, CACHE>(&'this self, type_: T) -> &'this HashSet<T>
    where
        T: KindAPI + CacheGetter<CacheType = CACHE>,
        CACHE: HasCommonTypeCache<T> + 'this,
    {
        &T::get_cache(self, type_).common_type_cache().subtypes
    }

    pub(crate) fn get_subtypes_transitive<'a, 'this, T, CACHE>(&'this self, type_: T) -> &'this Vec<T>
    where
        T: KindAPI + CacheGetter<CacheType = CACHE>,
        CACHE: HasCommonTypeCache<T> + 'this,
    {
        &T::get_cache(self, type_).common_type_cache().subtypes_transitive
    }

    pub(crate) fn get_label<'a, 'this, T, CACHE>(&'this self, type_: T) -> &'this Label
    where
        T: KindAPI + CacheGetter<CacheType = CACHE>,
        CACHE: HasCommonTypeCache<T> + 'this,
    {
        &T::get_cache(self, type_).common_type_cache().label
    }

    pub(crate) fn get_label_owned<'a, 'this, T, CACHE>(&'this self, type_: T) -> Arc<Label>
    where
        T: KindAPI + CacheGetter<CacheType = CACHE>,
        CACHE: HasCommonTypeCache<T> + 'this,
    {
        T::get_cache(self, type_).common_type_cache().label.clone()
    }

    pub(crate) fn get_annotations_declared<'a, T, CACHE>(
        &'a self,
        type_: T,
    ) -> &'a HashSet<<T as KindAPI>::AnnotationType>
    where
        T: KindAPI + CacheGetter<CacheType = CACHE>,
        CACHE: HasCommonTypeCache<T> + 'a,
    {
        &T::get_cache(self, type_).common_type_cache().annotations_declared
    }

    pub(crate) fn get_constraints<'a, T, CACHE>(&'a self, type_: T) -> &'a HashSet<TypeConstraint<T>>
    where
        T: KindAPI + CacheGetter<CacheType = CACHE>,
        CACHE: HasCommonTypeCache<T> + 'a,
    {
        &T::get_cache(self, type_).common_type_cache().constraints
    }

    pub(crate) fn get_attribute_type_owns(&self, attribute_type: AttributeType) -> &HashSet<Owns> {
        &AttributeType::get_cache(self, attribute_type).owns
    }

    pub(crate) fn get_attribute_type_owner_types(&self, attribute_type: AttributeType) -> &HashMap<ObjectType, Owns> {
        &AttributeType::get_cache(self, attribute_type).owner_types
    }

    pub(crate) fn get_owns_declared<'a, T, CACHE>(&'a self, type_: T) -> &'a HashSet<Owns>
    where
        T: OwnerAPI + PlayerAPI + CacheGetter<CacheType = CACHE>,
        CACHE: HasObjectCache + 'a,
    {
        &T::get_cache(self, type_).object_cache().owns_declared
    }

    pub(crate) fn get_owns<'a, T, CACHE>(&'a self, type_: T) -> &'a HashSet<Owns>
    where
        T: OwnerAPI + PlayerAPI + CacheGetter<CacheType = CACHE>,
        CACHE: HasObjectCache + 'a,
    {
        &T::get_cache(self, type_).object_cache().owns
    }

    pub(crate) fn get_owns_with_specialised<'a, T, CACHE>(&'a self, type_: T) -> &'a HashSet<Owns>
    where
        T: OwnerAPI + PlayerAPI + CacheGetter<CacheType = CACHE>,
        CACHE: HasObjectCache + 'a,
    {
        &T::get_cache(self, type_).object_cache().owns_with_specialised
    }

    pub(crate) fn get_owned_attribute_type_constraints<'a, 'this, T, CACHE>(
        &'this self,
        type_: T,
    ) -> &'this HashMap<AttributeType, HashSet<CapabilityConstraint<Owns>>>
    where
        T: OwnerAPI + PlayerAPI + CacheGetter<CacheType = CACHE>,
        CACHE: HasObjectCache + 'this,
    {
        &T::get_cache(self, type_).object_cache().owned_attribute_type_constraints
    }

    pub(crate) fn get_role_type_ordering(&self, role_type: RoleType) -> Ordering {
        RoleType::get_cache(self, role_type).ordering
    }

    pub(crate) fn get_role_type_relates_explicit(&self, role_type: RoleType) -> &Relates {
        &RoleType::get_cache(self, role_type).relates_explicit
    }

    pub(crate) fn get_role_type_relates(&self, role_type: RoleType) -> &HashSet<Relates> {
        &RoleType::get_cache(self, role_type).relates
    }

    pub(crate) fn get_role_type_relation_types(&self, role_type: RoleType) -> &HashMap<RelationType, Relates> {
        &RoleType::get_cache(self, role_type).relation_types
    }

    pub(crate) fn get_relation_type_relates_root(&self, relation_type: RelationType) -> &HashSet<Relates> {
        &RelationType::get_cache(self, relation_type).relates_root
    }

    pub(crate) fn get_relation_type_relates_declared(&self, relation_type: RelationType) -> &HashSet<Relates> {
        &RelationType::get_cache(self, relation_type).relates_declared
    }

    pub(crate) fn get_relation_type_relates(&self, relation_type: RelationType) -> &HashSet<Relates> {
        &RelationType::get_cache(self, relation_type).relates
    }

    pub(crate) fn get_relation_type_relates_with_specialised(&self, relation_type: RelationType) -> &HashSet<Relates> {
        &RelationType::get_cache(self, relation_type).relates_with_specialised
    }

    pub(crate) fn get_relation_type_related_role_type_constraints(
        &self,
        relation_type: RelationType,
    ) -> &HashMap<RoleType, HashSet<CapabilityConstraint<Relates>>> {
        &RelationType::get_cache(self, relation_type).related_role_type_constraints
    }

    pub(crate) fn get_relates_annotations_declared(&self, relates: Relates) -> &HashSet<RelatesAnnotation> {
        &self.relates.get(&relates).unwrap().common_capability_cache.annotations_declared
    }

    pub(crate) fn get_relates_constraints(&self, relates: Relates) -> &HashSet<CapabilityConstraint<Relates>> {
        &self.relates.get(&relates).unwrap().common_capability_cache.constraints
    }

    pub(crate) fn get_relates_is_implicit(&self, relates: Relates) -> bool {
        self.relates.get(&relates).unwrap().is_implicit
    }

    pub(crate) fn get_role_type_plays(&self, role_type: RoleType) -> &HashSet<Plays> {
        &RoleType::get_cache(self, role_type).plays
    }

    pub(crate) fn get_role_type_player_types(&self, role_type: RoleType) -> &HashMap<ObjectType, Plays> {
        &RoleType::get_cache(self, role_type).player_types
    }

    pub(crate) fn get_plays_declared<'a, T, CACHE>(&'a self, type_: T) -> &'a HashSet<Plays>
    where
        T: OwnerAPI + PlayerAPI + CacheGetter<CacheType = CACHE>,
        CACHE: HasObjectCache + 'a,
    {
        &T::get_cache(self, type_).object_cache().plays_declared
    }

    pub(crate) fn get_plays<'a, T, CACHE>(&'a self, type_: T) -> &'a HashSet<Plays>
    where
        T: OwnerAPI + PlayerAPI + CacheGetter<CacheType = CACHE>,
        CACHE: HasObjectCache + 'a,
    {
        &T::get_cache(self, type_).object_cache().plays
    }

    pub(crate) fn get_plays_with_specialised<'a, T, CACHE>(&'a self, type_: T) -> &'a HashSet<Plays>
    where
        T: OwnerAPI + PlayerAPI + CacheGetter<CacheType = CACHE>,
        CACHE: HasObjectCache + 'a,
    {
        &T::get_cache(self, type_).object_cache().plays_with_specialised
    }

    pub(crate) fn get_played_role_type_constraints<'a, 'this, T, CACHE>(
        &'this self,
        type_: T,
    ) -> &'this HashMap<RoleType, HashSet<CapabilityConstraint<Plays>>>
    where
        T: OwnerAPI + PlayerAPI + CacheGetter<CacheType = CACHE>,
        CACHE: HasObjectCache + 'this,
    {
        &T::get_cache(self, type_).object_cache().played_role_type_constraints
    }

    pub(crate) fn get_plays_annotations_declared(&self, plays: Plays) -> &HashSet<PlaysAnnotation> {
        &self.plays.get(&plays).unwrap().common_capability_cache.annotations_declared
    }

    pub(crate) fn get_plays_constraints(&self, plays: Plays) -> &HashSet<CapabilityConstraint<Plays>> {
        &self.plays.get(&plays).unwrap().common_capability_cache.constraints
    }

    pub(crate) fn get_attribute_type_value_type_declared(&self, attribute_type: AttributeType) -> &Option<ValueType> {
        &AttributeType::get_cache(self, attribute_type).value_type_declared
    }

    pub(crate) fn get_attribute_type_value_type(
        &self,
        attribute_type: AttributeType,
    ) -> &Option<(ValueType, AttributeType)> {
        &AttributeType::get_cache(self, attribute_type).value_type
    }

    pub(crate) fn get_owns_annotations_declared(&self, owns: Owns) -> &HashSet<OwnsAnnotation> {
        &self.owns.get(&owns).unwrap().common_capability_cache.annotations_declared
    }

    pub(crate) fn get_owns_constraints(&self, owns: Owns) -> &HashSet<CapabilityConstraint<Owns>> {
        &self.owns.get(&owns).unwrap().common_capability_cache.constraints
    }

    pub(crate) fn get_owns_ordering(&self, owns: Owns) -> Ordering {
        self.owns.get(&owns).unwrap().ordering
    }

    pub(crate) fn get_struct_definition_key(&self, label: &str) -> Option<DefinitionKey> {
        self.struct_definition_index_by_name.get(label).cloned()
    }

    pub(crate) fn get_struct_definition(&self, definition_key: DefinitionKey) -> &StructDefinition {
        &self.struct_definitions[definition_key.definition_id().as_uint() as usize].as_ref().unwrap().definition
    }

    pub(crate) fn get_independent_attribute_types(&self) -> Arc<HashSet<AttributeType>> {
        self.independent_attribute_types.clone()
    }
}

typedb_error! {
    pub TypeCacheCreateError(component = "TypeCache create", prefix = "TCC") {
        Empty(1, ""),
    }
}
