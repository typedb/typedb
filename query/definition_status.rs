/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use concept::{
    error::ConceptReadError,
    type_::{
        annotation::{Annotation, AnnotationCategory},
        attribute_type::AttributeType,
        entity_type::EntityType,
        object_type::ObjectType,
        owns::Owns,
        plays::Plays,
        relates::Relates,
        relation_type::RelationType,
        role_type::RoleType,
        type_manager::TypeManager,
        Capability, KindAPI, Ordering, OwnerAPI, PlayerAPI, TypeAPI,
    },
};
use encoding::{
    graph::definition::{definition_key::DefinitionKey, r#struct::StructDefinitionField},
    value::{label::Label, value_type::ValueType},
};
use storage::snapshot::ReadableSnapshot;

use crate::definition_resolution::{
    try_resolve_owns_declared, try_resolve_plays_declared, try_resolve_relates_declared,
    try_resolve_struct_definition_key,
};

pub(crate) enum DefinitionStatus<T> {
    DoesNotExist,
    ExistsSame(Option<T>), // return Some(T) only when it's needed
    ExistsDifferent(T),
}

macro_rules! get_some_or_return_does_not_exist {
    ($res:pat = $opt:ident) => {
        let $res = if let Some(some) = $opt {
            some
        } else {
            return Ok(DefinitionStatus::DoesNotExist);
        };
    };
}

macro_rules! return_exists_same_none_if_some {
    ($opt:ident) => {
        if let Some(_) = $opt {
            return Ok(DefinitionStatus::ExistsSame(None));
        }
    };
}

macro_rules! return_exists_same_some_if_some {
    ($opt:ident) => {
        if let Some(some) = $opt {
            return Ok(DefinitionStatus::ExistsSame(Some(some)));
        }
    };
}

macro_rules! return_exists_different_if_some {
    ($opt:ident) => {
        if let Some(some) = $opt {
            return Ok(DefinitionStatus::ExistsDifferent(some));
        }
    };
}

macro_rules! get_type_status {
    ($(
        fn $method_name:ident() -> $type_:ident = $get_method:ident;
    )*) => {
        $(
            pub(crate) fn $method_name(
                snapshot: &impl ReadableSnapshot,
                type_manager: &TypeManager,
                label: &Label<'_>,
            ) -> Result<DefinitionStatus<$type_<'static>>, ConceptReadError> {
                let type_opt = type_manager.$get_method(snapshot, label)?;
                get_some_or_return_does_not_exist!(_ = type_opt);
                Ok(DefinitionStatus::ExistsSame(None))
            }
        )*
    }
}

get_type_status! {
    fn get_entity_type_status() -> EntityType = get_entity_type;
    fn get_relation_type_status() -> RelationType = get_relation_type;
    fn get_attribute_type_status() -> AttributeType = get_attribute_type;
}

pub(crate) fn get_struct_status(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    name: &str,
) -> Result<DefinitionStatus<DefinitionKey<'static>>, ConceptReadError> {
    let definition_key_opt = try_resolve_struct_definition_key(snapshot, type_manager, name)?;
    get_some_or_return_does_not_exist!(_ = definition_key_opt);
    Ok(DefinitionStatus::ExistsSame(None))
}

pub(crate) fn get_struct_field_status(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    definition_key: DefinitionKey<'static>,
    field_key: &str,
    value_type: ValueType,
    optional: bool,
) -> Result<DefinitionStatus<StructDefinitionField>, ConceptReadError> {
    let struct_definition = type_manager.get_struct_definition(snapshot, definition_key)?;
    let field_opt = struct_definition.get_field(field_key);
    get_some_or_return_does_not_exist!(field = field_opt);

    if field.same(optional, value_type) {
        Ok(DefinitionStatus::ExistsSame(None))
    } else {
        Ok(DefinitionStatus::ExistsDifferent(field.clone()))
    }
}

pub(crate) fn get_type_annotation_status<'a, T: KindAPI<'a>>(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    type_: T,
    annotation: &T::AnnotationType,
    annotation_category: AnnotationCategory,
) -> Result<DefinitionStatus<T::AnnotationType>, ConceptReadError> {
    let existing_annotations = type_.get_annotations_declared(snapshot, type_manager)?;

    let same_annotation_opt = existing_annotations.get(annotation);
    return_exists_same_none_if_some!(same_annotation_opt);

    let different_annotation_opt = existing_annotations
        .into_iter()
        .find(|existing_annotation| (*existing_annotation).clone().into().category() == annotation_category)
        .cloned();
    return_exists_different_if_some!(different_annotation_opt);

    Ok(DefinitionStatus::DoesNotExist)
}

pub(crate) fn get_capability_annotation_status<'a, CAP: Capability<'a>>(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    capability: CAP,
    annotation: &CAP::AnnotationType,
    annotation_category: AnnotationCategory,
) -> Result<DefinitionStatus<CAP::AnnotationType>, ConceptReadError> {
    let existing_annotations = capability.get_annotations_declared(snapshot, type_manager)?;

    let same_annotation_opt = existing_annotations.get(annotation);
    return_exists_same_none_if_some!(same_annotation_opt);

    let different_annotation_opt = existing_annotations
        .into_iter()
        .find(|existing_annotation| (*existing_annotation).clone().into().category() == annotation_category)
        .cloned();
    return_exists_different_if_some!(different_annotation_opt);

    Ok(DefinitionStatus::DoesNotExist)
}

pub(crate) fn get_sub_status<'a, T: TypeAPI<'a>>(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    type_: T,
    new_supertype: T,
) -> Result<DefinitionStatus<T>, ConceptReadError> {
    let existing_supertype_opt = type_.get_supertype(snapshot, type_manager)?;
    get_some_or_return_does_not_exist!(existing_supertype = existing_supertype_opt);

    Ok(if existing_supertype == new_supertype {
        DefinitionStatus::ExistsSame(None)
    } else {
        DefinitionStatus::ExistsDifferent(existing_supertype)
    })
}

pub(crate) fn get_override_status<'a, CAP: Capability<'a>>(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    capability: CAP,
    new_override: CAP,
) -> Result<DefinitionStatus<CAP>, ConceptReadError> {
    let existing_override_opt = capability.get_override(snapshot, type_manager)?.clone();
    get_some_or_return_does_not_exist!(existing_override = existing_override_opt);

    Ok(if existing_override == new_override {
        DefinitionStatus::ExistsSame(None)
    } else {
        DefinitionStatus::ExistsDifferent(existing_override)
    })
}

pub(crate) fn get_value_type_status<'a>(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    attribute_type: AttributeType<'a>,
    new_value_type: ValueType,
) -> Result<DefinitionStatus<ValueType>, ConceptReadError> {
    let existing_value_type_opt = attribute_type.get_value_type_declared(snapshot, type_manager)?;
    get_some_or_return_does_not_exist!(existing_value_type = existing_value_type_opt);

    Ok(if existing_value_type == new_value_type {
        DefinitionStatus::ExistsSame(None)
    } else {
        DefinitionStatus::ExistsDifferent(existing_value_type)
    })
}

pub(crate) fn get_relates_status<'a>(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    relation_type: RelationType<'static>,
    role_label: &Label<'a>,
    new_ordering: Ordering,
) -> Result<DefinitionStatus<(Relates<'static>, Ordering)>, ConceptReadError> {
    let existing_relates_opt =
        try_resolve_relates_declared(snapshot, type_manager, relation_type, role_label.name.as_str())?;
    get_some_or_return_does_not_exist!(existing_relates = existing_relates_opt);

    let existing_ordering = existing_relates.role().get_ordering(snapshot, type_manager)?;
    Ok(if existing_ordering == new_ordering {
        DefinitionStatus::ExistsSame(Some((existing_relates, existing_ordering)))
    } else {
        DefinitionStatus::ExistsDifferent((existing_relates, existing_ordering))
    })
}

pub(crate) fn get_owns_status(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    object_type: ObjectType<'static>,
    attribute_type: AttributeType<'static>,
    new_ordering: Ordering,
) -> Result<DefinitionStatus<(Owns<'static>, Ordering)>, ConceptReadError> {
    let existing_owns_opt = try_resolve_owns_declared(snapshot, type_manager, object_type, attribute_type)?;
    get_some_or_return_does_not_exist!(existing_owns = existing_owns_opt);

    let existing_ordering = existing_owns.get_ordering(snapshot, type_manager)?;
    Ok(if existing_ordering == new_ordering {
        DefinitionStatus::ExistsSame(Some((existing_owns, existing_ordering)))
    } else {
        DefinitionStatus::ExistsDifferent((existing_owns, existing_ordering))
    })
}

pub(crate) fn get_plays_status(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    object_type: ObjectType<'static>,
    role_type: RoleType<'static>,
) -> Result<DefinitionStatus<Plays<'static>>, ConceptReadError> {
    let existing_plays_opt = try_resolve_plays_declared(snapshot, type_manager, object_type, role_type)?;
    get_some_or_return_does_not_exist!(existing_plays = existing_plays_opt);

    Ok(DefinitionStatus::ExistsSame(Some(existing_plays)))
}
