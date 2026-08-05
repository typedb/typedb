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

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct VertexAnnotations {
    annotations: BTreeMap<Vertex<Variable>, BTreeSet<TypeAnnotation>>,
}

impl VertexAnnotations {
    pub(crate) fn add_or_intersect(
        &mut self,
        vertex: &Vertex<Variable>,
        new_annotations: Cow<'_, BTreeSet<TypeAnnotation>>,
    ) -> bool {
        if let Some(existing_annotations) = self.get_mut(vertex) {
            let size_before = existing_annotations.len();
            existing_annotations.retain(|x| new_annotations.contains(x));
            existing_annotations.len() == size_before
        } else {
            self.insert(vertex.clone(), new_annotations.into_owned());
            true
        }
    }
}

impl Deref for VertexAnnotations {
    type Target = BTreeMap<Vertex<Variable>, BTreeSet<TypeAnnotation>>;

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
}

impl<T: Ord> FromIteratorMappedOperations<T> for BTreeSet<T> {}
impl<T: Ord> ExtendMappedOperations<T> for BTreeSet<T> {}
impl<T> FromIteratorMappedOperations<T> for Vec<T> {}
impl<T> ExtendMappedOperations<T> for Vec<T> {}
