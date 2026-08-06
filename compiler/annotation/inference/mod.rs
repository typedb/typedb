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

use answer::{Type as TypeAnnotation, variable::Variable};
use ir::pattern::Vertex;

pub mod match_inference;
pub mod type_seeder;

#[derive(Clone, Debug, Eq, PartialEq)]
enum VertexTypeAnnotations {
    Concept(ConceptVertexTypes),
}

impl From<ConceptVertexTypes> for VertexTypeAnnotations {
    fn from(value: ConceptVertexTypes) -> Self {
        Self::Concept(value)
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct ConceptVertexTypes(BTreeSet<answer::Type>);

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

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct VertexAnnotations {
    annotations: BTreeMap<Vertex<Variable>, VertexTypeAnnotations>,
}

impl VertexAnnotations {
    pub(crate) fn new() -> VertexAnnotations {
        Self { annotations: BTreeMap::new() }
    }

    pub(crate) fn add_or_intersect(
        &mut self,
        vertex: &Vertex<Variable>,
        new_annotations: Cow<'_, VertexTypeAnnotations>,
    ) -> bool {
        if let Some(existing_annotations) = self.get_mut(vertex) {
            existing_annotations.retain_intersection(&*new_annotations)
        } else {
            self.insert(vertex.clone(), new_annotations.into_owned());
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
    type Item = <BTreeMap<Vertex<Variable>, BTreeSet<TypeAnnotation>> as IntoIterator>::Item;
    type IntoIter = <BTreeMap<Vertex<Variable>, BTreeSet<TypeAnnotation>> as IntoIterator>::IntoIter;
    fn into_iter(self) -> Self::IntoIter {
        self.annotations.into_iter()
    }
}

impl<'a> IntoIterator for &'a VertexAnnotations {
    type Item = <&'a BTreeMap<Vertex<Variable>, BTreeSet<TypeAnnotation>> as IntoIterator>::Item;
    type IntoIter = <&'a BTreeMap<Vertex<Variable>, BTreeSet<TypeAnnotation>> as IntoIterator>::IntoIter;
    fn into_iter(self) -> Self::IntoIter {
        self.annotations.iter()
    }
}

impl<'a> IntoIterator for &'a mut VertexAnnotations {
    type Item = <&'a mut BTreeMap<Vertex<Variable>, BTreeSet<TypeAnnotation>> as IntoIterator>::Item;
    type IntoIter = <&'a mut BTreeMap<Vertex<Variable>, BTreeSet<TypeAnnotation>> as IntoIterator>::IntoIter;
    fn into_iter(self) -> Self::IntoIter {
        self.annotations.iter_mut()
    }
}

impl<T> From<T> for VertexAnnotations
where
    BTreeMap<Vertex<Variable>, BTreeSet<TypeAnnotation>>: From<T>,
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

pub(crate) trait RetainAndContainExt<T> {
    fn contains_ext(&self, item: &T) -> bool;
    fn retain_intersection<S: RetainAndContainExt<T>>(&mut self, other: &S) -> bool;
}

impl<K: Ord, V> RetainAndContainExt<K> for BTreeMap<K, V> {
    fn contains_ext(&self, item: &K) -> bool {
        self.contains_key(item)
    }

    fn retain_intersection<S: RetainAndContainExt<K>>(&mut self, other: &S) -> bool {
        let size_before = self.len();
        self.retain(|x, _| other.contains_ext(x));
        self.len() != size_before
    }
}

impl<K: Ord> RetainAndContainExt<K> for BTreeSet<K> {
    fn contains_ext(&self, item: &K) -> bool {
        self.contains(item)
    }

    fn retain_intersection<S: RetainAndContainExt<K>>(&mut self, other: &S) -> bool {
        let size_before = self.len();
        self.retain(|x| other.contains_ext(x));
        self.len() != size_before
    }
}
