/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{HashMap, HashSet},
    fmt::{Display, Formatter},
};

use encoding::{
    error::{EncodingError, EncodingError::UnexpectedPrefix},
    graph::{
        type_::{
            Kind,
            vertex::{PrefixedTypeVertexEncoding, TypeVertex, TypeVertexEncoding},
        },
        Typed,
    },
    layout::prefix::{Prefix, Prefix::VertexAttributeType},
    Prefixed,
    value::{label::Label, value_type::ValueType},
};
use primitive::maybe_owns::MaybeOwns;
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};

use crate::{
    ConceptAPI,
    error::{ConceptReadError, ConceptWriteError},
    thing::attribute::Attribute,
    type_::{
        annotation::{Annotation, AnnotationAbstract, AnnotationError, AnnotationIndependent, DefaultFrom},
        KindAPI,
        object_type::ObjectType,
        owns::Owns,
        ThingTypeAPI, type_manager::TypeManager, TypeAPI,
    },
};

use super::annotation::{AnnotationCategory, AnnotationRange, AnnotationRegex, AnnotationValues};

#[derive(Debug, Clone, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct AttributeType<'a> {
    vertex: TypeVertex<'a>,
}

impl<'a> AttributeType<'a> {}

impl<'a> ConceptAPI<'a> for AttributeType<'a> {}

impl<'a> PrefixedTypeVertexEncoding<'a> for AttributeType<'a> {
    const PREFIX: Prefix = VertexAttributeType;
}

impl<'a> TypeVertexEncoding<'a> for AttributeType<'a> {
    fn from_vertex(vertex: TypeVertex<'a>) -> Result<Self, EncodingError> {
        debug_assert!(Self::PREFIX == VertexAttributeType);
        if vertex.prefix() != Prefix::VertexAttributeType {
            Err(UnexpectedPrefix { expected_prefix: Prefix::VertexAttributeType, actual_prefix: vertex.prefix() })
        } else {
            Ok(AttributeType { vertex })
        }
    }

    fn into_vertex(self) -> TypeVertex<'a> {
        self.vertex
    }
}

impl<'a> primitive::prefix::Prefix for AttributeType<'a> {
    fn starts_with(&self, other: &Self) -> bool {
        self.vertex().starts_with(&other.vertex())
    }

    fn into_starts_with(self, other: Self) -> bool {
        self.vertex().as_reference().into_starts_with(other.vertex().as_reference())
    }
}

impl<'a> TypeAPI<'a> for AttributeType<'a> {
    type SelfStatic = AttributeType<'static>;

    fn new(vertex: TypeVertex<'a>) -> AttributeType<'a> {
        Self::from_vertex(vertex).unwrap()
    }

    fn vertex(&self) -> TypeVertex<'_> {
        self.vertex.as_reference()
    }

    fn is_abstract(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<bool, ConceptReadError> {
        let annotations = self.get_annotations(snapshot, type_manager)?;
        Ok(annotations.contains_key(&AttributeTypeAnnotation::Abstract(AnnotationAbstract)))
    }

    fn delete(self, snapshot: &mut impl WritableSnapshot, type_manager: &TypeManager) -> Result<(), ConceptWriteError> {
        type_manager.delete_attribute_type(snapshot, self.clone().into_owned())
    }

    fn get_label<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, Label<'static>>, ConceptReadError> {
        type_manager.get_attribute_type_label(snapshot, self.clone().into_owned())
    }
}

impl<'a> KindAPI<'a> for AttributeType<'a> {
    type AnnotationType = AttributeTypeAnnotation;
    const ROOT_KIND: Kind = Kind::Attribute;

    fn get_annotations_declared<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashSet<AttributeTypeAnnotation>>, ConceptReadError> {
        type_manager.get_attribute_type_annotations_declared(snapshot, self.clone().into_owned())
    }

    fn get_annotations<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashMap<AttributeTypeAnnotation, AttributeType<'static>>>, ConceptReadError> {
        type_manager.get_attribute_type_annotations(snapshot, self.clone().into_owned())
    }
}

impl<'a> ThingTypeAPI<'a> for AttributeType<'a> {
    type InstanceType<'b> = Attribute<'b>;
}

impl<'a> AttributeType<'a> {
    pub fn get_value_type(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<Option<ValueType>, ConceptReadError> {
        type_manager
            .get_attribute_type_value_type(snapshot, self.clone().into_owned())
            .map(|value_type_opt| value_type_opt.map(|(value_type, _)| value_type))
    }

    pub fn set_value_type(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        value_type: ValueType,
    ) -> Result<(), ConceptWriteError> {
        type_manager.set_value_type(snapshot, self.clone().into_owned(), value_type)
    }

    pub fn unset_value_type(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<(), ConceptWriteError> {
        type_manager.unset_value_type(snapshot, self.clone().into_owned())
    }

    pub fn set_label(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        label: &Label<'_>,
    ) -> Result<(), ConceptWriteError> {
        type_manager.set_label(snapshot, self.clone().into_owned(), label)
    }

    pub fn get_supertype(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<Option<AttributeType<'static>>, ConceptReadError> {
        type_manager.get_attribute_type_supertype(snapshot, self.clone().into_owned())
    }

    pub fn set_supertype(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        supertype: AttributeType<'static>,
    ) -> Result<(), ConceptWriteError> {
        type_manager.set_attribute_type_supertype(snapshot, self.clone().into_owned(), supertype)
    }

    pub fn get_supertypes<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, Vec<AttributeType<'static>>>, ConceptReadError> {
        type_manager.get_attribute_type_supertypes(snapshot, self.clone().into_owned())
    }

    pub fn get_subtypes<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, Vec<AttributeType<'static>>>, ConceptReadError> {
        type_manager.get_attribute_type_subtypes(snapshot, self.clone().into_owned())
    }

    pub fn get_subtypes_transitive<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, Vec<AttributeType<'static>>>, ConceptReadError> {
        type_manager.get_attribute_type_subtypes_transitive(snapshot, self.clone().into_owned())
    }

    pub(crate) fn is_independent(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<bool, ConceptReadError> {
        Ok(self
            .get_annotations(snapshot, type_manager)?
            .contains_key(&AttributeTypeAnnotation::Independent(AnnotationIndependent)))
    }

    pub fn set_annotation(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        annotation: AttributeTypeAnnotation,
    ) -> Result<(), ConceptWriteError> {
        match annotation {
            AttributeTypeAnnotation::Abstract(_) => {
                type_manager.set_annotation_abstract(snapshot, self.clone().into_owned())?
            }
            AttributeTypeAnnotation::Independent(_) => {
                type_manager.set_annotation_independent(snapshot, self.clone().into_owned())?
            }
            AttributeTypeAnnotation::Regex(regex) => {
                type_manager.set_annotation_regex(snapshot, self.clone().into_owned(), regex)?
            }
            AttributeTypeAnnotation::Range(range) => {
                type_manager.set_annotation_range(snapshot, self.clone().into_owned(), range)?
            }
            AttributeTypeAnnotation::Values(values) => {
                type_manager.set_annotation_values(snapshot, self.clone().into_owned(), values)?
            }
        };
        Ok(())
    }

    pub fn unset_annotation(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        annotation_category: AnnotationCategory,
    ) -> Result<(), ConceptWriteError> {
        let attribute_type_annotation = AttributeTypeAnnotation::try_getting_default(annotation_category)
            .map_err(|source| ConceptWriteError::Annotation { source })?;
        match attribute_type_annotation {
            AttributeTypeAnnotation::Abstract(_) => {
                type_manager.unset_attribute_type_annotation_abstract(snapshot, self.clone().into_owned())?
            }
            AttributeTypeAnnotation::Independent(_) => {
                type_manager.unset_annotation_independent(snapshot, self.clone().into_owned())?
            }
            AttributeTypeAnnotation::Regex(_) => {
                type_manager.unset_annotation_regex(snapshot, self.clone().into_owned())?
            }
            AttributeTypeAnnotation::Range(_) => {
                type_manager.unset_annotation_range(snapshot, self.clone().into_owned())?
            }
            AttributeTypeAnnotation::Values(_) => {
                type_manager.unset_annotation_values(snapshot, self.clone().into_owned())?
            }
        }
        Ok(())
    }

    pub fn into_owned(self) -> AttributeType<'static> {
        AttributeType { vertex: self.vertex.into_owned() }
    }
}

impl<'a> Display for AttributeType<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "[AttributeType:{}]", self.vertex.type_id_())
    }
}

// --- Owned API ---
impl<'a> AttributeType<'a> {
    pub fn get_owns_declared<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashSet<Owns<'static>>>, ConceptReadError> {
        type_manager.get_owns_for_attribute_declared(snapshot, self.clone().into_owned())
    }

    pub fn get_owns<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashMap<ObjectType<'static>, Owns<'static>>>, ConceptReadError> {
        type_manager.get_owns_for_attribute(snapshot, self.clone().into_owned())
    }

    fn get_owns_owners(&self) {
        // TODO: Why not just have owns?
        todo!()
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum AttributeTypeAnnotation {
    Abstract(AnnotationAbstract),
    Independent(AnnotationIndependent),
    Regex(AnnotationRegex),
    Range(AnnotationRange),
    Values(AnnotationValues),
}

impl From<Annotation> for Result<AttributeTypeAnnotation, AnnotationError> {
    fn from(annotation: Annotation) -> Result<AttributeTypeAnnotation, AnnotationError> {
        match annotation {
            Annotation::Abstract(annotation) => Ok(AttributeTypeAnnotation::Abstract(annotation)),
            Annotation::Independent(annotation) => Ok(AttributeTypeAnnotation::Independent(annotation)),
            Annotation::Regex(annotation) => Ok(AttributeTypeAnnotation::Regex(annotation)),
            Annotation::Range(annotation) => Ok(AttributeTypeAnnotation::Range(annotation)),
            Annotation::Values(annotation) => Ok(AttributeTypeAnnotation::Values(annotation)),

            | Annotation::Distinct(_)
            | Annotation::Unique(_)
            | Annotation::Key(_)
            | Annotation::Cardinality(_)
            | Annotation::Cascade(_) => {
                Err(AnnotationError::UnsupportedAnnotationForAttributeType(annotation.category()))
            }
        }
    }
}

impl From<Annotation> for AttributeTypeAnnotation {
    fn from(annotation: Annotation) -> Self {
        let into_annotation: Result<AttributeTypeAnnotation, AnnotationError> = annotation.into();
        match into_annotation {
            Ok(into_annotation) => into_annotation,
            Err(_) => unreachable!("Do not call this conversion from user-exposed code!"),
        }
    }
}

impl Into<Annotation> for AttributeTypeAnnotation {
    fn into(self) -> Annotation {
        match self {
            AttributeTypeAnnotation::Abstract(annotation) => Annotation::Abstract(annotation),
            AttributeTypeAnnotation::Independent(annotation) => Annotation::Independent(annotation),
            AttributeTypeAnnotation::Regex(annotation) => Annotation::Regex(annotation),
            AttributeTypeAnnotation::Range(annotation) => Annotation::Range(annotation),
            AttributeTypeAnnotation::Values(annotation) => Annotation::Values(annotation),
        }
    }
}
