/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::{
    borrow::Cow,
    collections::{BTreeMap, BTreeSet},
    ops::{Deref, DerefMut},
};

use answer::variable::Variable;
use encoding::value::value_type::ValueType;
// use encoding::value::value_type::ValueType;
use ir::pattern::Vertex;

use crate::annotation::TypeInferenceError;

pub mod match_inference;
pub mod type_seeder;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum VertexTypeAnnotations {
    Concept(ConceptVertexTypes),
    Value(ValueVertexTypes),
}

impl VertexTypeAnnotations {
    fn concept_from(iter: impl IntoIterator<Item = answer::Type>) -> Self {
        Self::Concept(ConceptVertexTypes(BTreeSet::from_iter(iter)))
    }

    fn value_from(iter: impl IntoIterator<Item = ValueType>) -> Self {
        Self::Value(ValueVertexTypes(BTreeSet::from_iter(iter)))
    }

    fn len(&self) -> usize {
        match self {
            VertexTypeAnnotations::Concept(type_set) => type_set.len(),
            VertexTypeAnnotations::Value(type_set) => type_set.len(),
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            VertexTypeAnnotations::Concept(type_set) => type_set.is_empty(),
            VertexTypeAnnotations::Value(type_set) => type_set.is_empty(),
        }
    }

    pub(crate) fn retain_types(&mut self, f: impl Fn(&TypeAnnotationSetEntry) -> bool) {
        match self {
            VertexTypeAnnotations::Concept(type_set) => type_set.retain(|t| f(&((*t).into()))),
            VertexTypeAnnotations::Value(type_set) => type_set.retain(|t| f(&((*t).into()))),
        }
    }

    pub(crate) fn extend_to_union(&mut self, other: &Self) -> Result<(), TypeInferenceError> {
        match (self, other) {
            (Self::Concept(inner), Self::Concept(other)) => Ok(inner.extend(other)),
            (Self::Value(inner), Self::Value(other)) => Ok(inner.extend(other)),
            (Self::Concept(_), Self::Value(_)) => {
                debug_assert!(false);
                Err(TypeInferenceError::InternalVertexTypesMismatch { expected: "concept".to_owned() })
            }
            (Self::Value(_), Self::Concept(_)) => {
                debug_assert!(false);
                Err(TypeInferenceError::InternalVertexTypesMismatch { expected: "value".to_owned() })
            }
        }
    }
}

impl From<ConceptVertexTypes> for VertexTypeAnnotations {
    fn from(value: ConceptVertexTypes) -> Self {
        Self::Concept(value)
    }
}

impl From<ValueVertexTypes> for VertexTypeAnnotations {
    fn from(value: ValueVertexTypes) -> Self {
        Self::Value(value)
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(super) struct ConceptVertexTypes(BTreeSet<answer::Type>);

impl Deref for ConceptVertexTypes {
    type Target = BTreeSet<answer::Type>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for ConceptVertexTypes {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<BTreeSet<answer::Type>> for ConceptVertexTypes {
    fn from(value: BTreeSet<answer::Type>) -> Self {
        Self(value)
    }
}

impl<'a> IntoIterator for &'a ConceptVertexTypes {
    type Item = <&'a BTreeSet<answer::Type> as IntoIterator>::Item;
    type IntoIter = <&'a BTreeSet<answer::Type> as IntoIterator>::IntoIter;
    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(super) struct ValueVertexTypes(BTreeSet<ValueType>);

impl Deref for ValueVertexTypes {
    type Target = BTreeSet<ValueType>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for ValueVertexTypes {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<BTreeSet<ValueType>> for ValueVertexTypes {
    fn from(value: BTreeSet<ValueType>) -> Self {
        Self(value)
    }
}

impl<'a> IntoIterator for &'a ValueVertexTypes {
    type Item = <&'a BTreeSet<ValueType> as IntoIterator>::Item;
    type IntoIter = <&'a BTreeSet<ValueType> as IntoIterator>::IntoIter;
    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct VertexAnnotations {
    annotations: BTreeMap<Vertex<Variable>, VertexTypeAnnotations>,
}

impl VertexAnnotations {
    pub(crate) fn new() -> VertexAnnotations {
        Self { annotations: BTreeMap::new() }
    }

    pub(crate) fn add_or_intersect<OTHER: TypeAnnotationSetTrait + Into<VertexTypeAnnotations> + Clone>(
        &mut self,
        vertex: &Vertex<Variable>,
        new_annotations: Cow<'_, OTHER>,
    ) -> bool {
        if let Some(existing_annotations) = self.get_mut(vertex) {
            existing_annotations.retain_intersection(&*new_annotations)
        } else {
            self.insert(vertex.clone(), new_annotations.into_owned().into());
            true
        }
    }
}

impl Deref for VertexAnnotations {
    type Target = BTreeMap<Vertex<Variable>, VertexTypeAnnotations>;

    fn deref(&self) -> &Self::Target {
        &self.annotations
    }
}

impl DerefMut for VertexAnnotations {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.annotations
    }
}

impl IntoIterator for VertexAnnotations {
    type Item = <BTreeMap<Vertex<Variable>, VertexTypeAnnotations> as IntoIterator>::Item;
    type IntoIter = <BTreeMap<Vertex<Variable>, VertexTypeAnnotations> as IntoIterator>::IntoIter;
    fn into_iter(self) -> Self::IntoIter {
        self.annotations.into_iter()
    }
}

impl<'a> IntoIterator for &'a VertexAnnotations {
    type Item = <&'a BTreeMap<Vertex<Variable>, VertexTypeAnnotations> as IntoIterator>::Item;
    type IntoIter = <&'a BTreeMap<Vertex<Variable>, VertexTypeAnnotations> as IntoIterator>::IntoIter;
    fn into_iter(self) -> Self::IntoIter {
        self.annotations.iter()
    }
}

impl<'a> IntoIterator for &'a mut VertexAnnotations {
    type Item = <&'a mut BTreeMap<Vertex<Variable>, VertexTypeAnnotations> as IntoIterator>::Item;
    type IntoIter = <&'a mut BTreeMap<Vertex<Variable>, VertexTypeAnnotations> as IntoIterator>::IntoIter;
    fn into_iter(self) -> Self::IntoIter {
        self.annotations.iter_mut()
    }
}

impl<T> From<T> for VertexAnnotations
where
    BTreeMap<Vertex<Variable>, VertexTypeAnnotations>: From<T>,
{
    fn from(t: T) -> Self {
        Self { annotations: t.into() }
    }
}

pub(crate) trait FromIteratorMappedOperations<T>: Sized + FromIterator<T> {
    fn from_into<FROM>(iter: impl IntoIterator<Item = FROM>) -> Self
    where
        T: From<FROM>,
    {
        Self::from_iter(iter.into_iter().map(From::from))
    }

    fn from_into_ref<'a, FROM>(iter: impl IntoIterator<Item = &'a FROM>) -> Self
    where
        FROM: Copy + 'a,
        T: From<FROM>,
    {
        Self::from_into(iter.into_iter().copied())
    }

    fn from_mapped<FROM>(iter: impl IntoIterator<Item = FROM>, f: impl Fn(FROM) -> T) -> Self {
        Self::from_iter(iter.into_iter().map(f))
    }

    fn from_mapped_ref<'a, FROM>(iter: impl IntoIterator<Item = &'a FROM>, f: impl Fn(FROM) -> T) -> Self
    where
        FROM: Copy + 'a,
    {
        Self::from_mapped(iter.into_iter().copied(), f)
    }
}

pub(crate) trait ExtendMappedOperations<T>: Extend<T> {
    fn extend_into<FROM>(&mut self, iter: impl IntoIterator<Item = FROM>)
    where
        T: From<FROM>,
    {
        self.extend(iter.into_iter().map(From::from))
    }

    fn extend_into_ref<'a, FROM>(&mut self, iter: impl IntoIterator<Item = &'a FROM>)
    where
        FROM: Copy + 'a,
        T: From<FROM>,
    {
        self.extend_into(iter.into_iter().copied())
    }

    fn extend_mapped<FROM>(&mut self, iter: impl IntoIterator<Item = FROM>, f: impl Fn(FROM) -> T) {
        self.extend(iter.into_iter().map(f))
    }

    fn extend_mapped_ref<'a, FROM>(&mut self, iter: impl IntoIterator<Item = &'a FROM>, f: impl Fn(FROM) -> T)
    where
        FROM: Copy + 'a,
    {
        self.extend_mapped(iter.into_iter().copied(), f)
    }
}

impl<T: Ord> FromIteratorMappedOperations<T> for BTreeSet<T> {}
impl<T: Ord> ExtendMappedOperations<T> for BTreeSet<T> {}
impl<T> FromIteratorMappedOperations<T> for Vec<T> {}
impl<T> ExtendMappedOperations<T> for Vec<T> {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TypeAnnotationSetEntry {
    Concept(answer::Type),
    Value(ValueType),
}

impl From<answer::Type> for TypeAnnotationSetEntry {
    fn from(value: answer::Type) -> Self {
        Self::Concept(value)
    }
}

impl From<ValueType> for TypeAnnotationSetEntry {
    fn from(value: ValueType) -> Self {
        Self::Value(value)
    }
}

pub(crate) trait TypeAnnotationSetTrait {
    type NativeItem;

    fn contains_type<T: Into<TypeAnnotationSetEntry> + Copy>(&self, type_: &T) -> bool;
    fn iter_types(&self) -> impl Iterator<Item = TypeAnnotationSetEntry>;
    fn retain_intersection<OTHER: TypeAnnotationSetTrait>(&mut self, other: &OTHER) -> bool;
}

impl TypeAnnotationSetTrait for VertexTypeAnnotations {
    type NativeItem = TypeAnnotationSetEntry;

    fn contains_type<T: Into<TypeAnnotationSetEntry> + Copy>(&self, type_: &T) -> bool {
        match self {
            VertexTypeAnnotations::Concept(type_set) => type_set.contains_type(type_),
            VertexTypeAnnotations::Value(type_set) => type_set.contains_type(type_),
        }
    }

    fn iter_types(&self) -> Box<dyn Iterator<Item = TypeAnnotationSetEntry> + '_> {
        match self {
            VertexTypeAnnotations::Concept(type_set) => Box::new(type_set.iter_types()),
            VertexTypeAnnotations::Value(type_set) => Box::new(type_set.iter_types()),
        }
    }

    fn retain_intersection<OTHER: TypeAnnotationSetTrait>(&mut self, other: &OTHER) -> bool {
        match self {
            VertexTypeAnnotations::Concept(type_set) => type_set.retain_intersection(other),
            VertexTypeAnnotations::Value(type_set) => type_set.retain_intersection(other),
        }
    }
}

impl TypeAnnotationSetTrait for ConceptVertexTypes {
    type NativeItem = answer::Type;

    fn contains_type<T: Into<TypeAnnotationSetEntry> + Copy>(&self, type_: &T) -> bool {
        match (*type_).into() {
            TypeAnnotationSetEntry::Concept(type_) => self.contains(&type_),
            TypeAnnotationSetEntry::Value(_) => false,
        }
    }

    fn iter_types(&self) -> impl Iterator<Item = TypeAnnotationSetEntry> {
        self.iter().map(|t| TypeAnnotationSetEntry::Concept(*t))
    }

    fn retain_intersection<OTHER: TypeAnnotationSetTrait>(&mut self, other: &OTHER) -> bool {
        let size_before = self.len();
        self.retain(|type_| other.contains_type(type_));
        self.len() != size_before
    }
}

impl TypeAnnotationSetTrait for ValueVertexTypes {
    type NativeItem = ValueType;

    fn contains_type<T: Into<TypeAnnotationSetEntry> + Copy>(&self, type_: &T) -> bool {
        match (*type_).into() {
            TypeAnnotationSetEntry::Concept(_) => false,
            TypeAnnotationSetEntry::Value(type_) => self.contains(&type_),
        }
    }

    fn iter_types(&self) -> impl Iterator<Item = TypeAnnotationSetEntry> {
        self.iter().map(|t| TypeAnnotationSetEntry::Value(*t))
    }

    fn retain_intersection<OTHER: TypeAnnotationSetTrait>(&mut self, other: &OTHER) -> bool {
        let size_before = self.len();
        self.retain(|type_| other.contains_type(type_));
        self.len() != size_before
    }
}

impl TypeAnnotationSetTrait for BTreeMap<answer::Type, BTreeSet<answer::Type>> {
    type NativeItem = answer::Type;

    fn contains_type<T: Into<TypeAnnotationSetEntry> + Copy>(&self, type_: &T) -> bool {
        match (*type_).into() {
            TypeAnnotationSetEntry::Concept(type_) => self.contains_key(&type_),
            TypeAnnotationSetEntry::Value(_) => false,
        }
    }

    fn iter_types(&self) -> impl Iterator<Item = TypeAnnotationSetEntry> {
        self.keys().map(|type_| TypeAnnotationSetEntry::Concept(*type_))
    }

    fn retain_intersection<OTHER: TypeAnnotationSetTrait>(&mut self, other: &OTHER) -> bool {
        let size_before = self.len();
        self.retain(|type_, _| other.contains_type(type_));
        self.len() != size_before
    }
}

#[cfg(debug_assertions)]
pub mod tests {
    use std::collections::BTreeSet;

    use answer::{Type, variable::Variable};
    use ir::pattern::Vertex;

    use crate::annotation::inference::{ConceptVertexTypes, VertexAnnotations, VertexTypeAnnotations};

    impl FromIterator<(Vertex<Variable>, BTreeSet<answer::Type>)> for VertexAnnotations {
        fn from_iter<T: IntoIterator<Item = (Vertex<Variable>, BTreeSet<Type>)>>(iter: T) -> Self {
            let annotations = iter
                .into_iter()
                .map(|(v, types)| (v, VertexTypeAnnotations::Concept(ConceptVertexTypes(types))))
                .collect();
            Self { annotations }
        }
    }
}
