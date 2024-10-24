/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use concept::{
    error::ConceptReadError,
    type_::{
        annotation::AnnotationCategory, attribute_type::AttributeType, entity_type::EntityType,
        object_type::ObjectType, owns::Owns, plays::Plays, relates::Relates, relation_type::RelationType,
        role_type::RoleType, type_manager::TypeManager, Capability, KindAPI, Ordering, TypeAPI,
    },
};
use encoding::{
    graph::definition::{definition_key::DefinitionKey, r#struct::StructDefinitionField},
    value::{label::Label, value_type::ValueType},
};
use storage::snapshot::ReadableSnapshot;

use crate::definable_resolution::{
    try_resolve_owns_declared, try_resolve_plays_declared, try_resolve_relates_declared,
    try_resolve_struct_definition_key,
};

pub(crate) enum DefinableStatus<T> {
    DoesNotExist,
    ExistsSame(Option<T>), // return Some(T) only when it's needed
    ExistsDifferent(T),
}

macro_rules! get_some_or_return_does_not_exist {
    ($res:pat = $opt:ident) => {
        let $res = if let Some(some) = $opt {
            some
        } else {
            return Ok(DefinableStatus::DoesNotExist);
        };
    };
}

macro_rules! return_exists_same_none_if_some {
    ($opt:ident) => {
        if let Some(_) = $opt {
            return Ok(DefinableStatus::ExistsSame(None));
        }
    };
}

macro_rules! return_exists_same_some_if_some {
    ($opt:ident) => {
        if let Some(some) = $opt {
            return Ok(DefinableStatus::ExistsSame(Some(some)));
        }
    };
}

macro_rules! return_exists_different_if_some {
    ($opt:ident) => {
        if let Some(some) = $opt {
            return Ok(DefinableStatus::ExistsDifferent(some));
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
            ) -> Result<DefinableStatus<$type_<'static>>, ConceptReadError> {
                let type_opt = type_manager.$get_method(snapshot, label)?;
                get_some_or_return_does_not_exist!(_ = type_opt);
                Ok(DefinableStatus::ExistsSame(None))
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
) -> Result<DefinableStatus<DefinitionKey<'static>>, ConceptReadError> {
    let definition_key_opt = try_resolve_struct_definition_key(snapshot, type_manager, name)?;
    get_some_or_return_does_not_exist!(_ = definition_key_opt);
    Ok(DefinableStatus::ExistsSame(None))
}

pub(crate) fn get_struct_field_status(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    definition_key: DefinitionKey<'static>,
    field_key: &str,
    value_type: ValueType,
    optional: bool,
) -> Result<DefinableStatus<StructDefinitionField>, ConceptReadError> {
    let struct_definition = type_manager.get_struct_definition(snapshot, definition_key)?;
    let field_opt = struct_definition.get_field(field_key);
    get_some_or_return_does_not_exist!(field = field_opt);

    if field.has_optionality_and_value_type(optional, value_type) {
        Ok(DefinableStatus::ExistsSame(None))
    } else {
        Ok(DefinableStatus::ExistsDifferent(field.clone()))
    }
}

pub(crate) fn get_type_annotation_status<'a, T: KindAPI<'a>>(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    type_: T,
    annotation: &T::AnnotationType,
    annotation_category: AnnotationCategory,
) -> Result<DefinableStatus<T::AnnotationType>, ConceptReadError> {
    let existing_annotations = type_.get_annotations_declared(snapshot, type_manager)?;

    let same_annotation_opt = existing_annotations.get(annotation);
    return_exists_same_none_if_some!(same_annotation_opt);

    let different_annotation_opt = existing_annotations
        .into_iter()
        .find(|existing_annotation| (*existing_annotation).clone().into().category() == annotation_category)
        .cloned();
    return_exists_different_if_some!(different_annotation_opt);

    Ok(DefinableStatus::DoesNotExist)
}

pub(crate) fn get_capability_annotation_status<'a, CAP: Capability<'a>>(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    capability: &CAP,
    annotation: &CAP::AnnotationType,
    annotation_category: AnnotationCategory,
) -> Result<DefinableStatus<CAP::AnnotationType>, ConceptReadError> {
    let existing_annotations = capability.get_annotations_declared(snapshot, type_manager)?;

    let same_annotation_opt = existing_annotations.get(annotation);
    return_exists_same_none_if_some!(same_annotation_opt);

    let different_annotation_opt = existing_annotations
        .into_iter()
        .find(|existing_annotation| (*existing_annotation).clone().into().category() == annotation_category)
        .cloned();
    return_exists_different_if_some!(different_annotation_opt);

    Ok(DefinableStatus::DoesNotExist)
}

pub(crate) fn get_sub_status<'a, T: TypeAPI<'a>>(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    type_: T,
    supertype: T,
) -> Result<DefinableStatus<T>, ConceptReadError> {
    let existing_supertype_opt = type_.get_supertype(snapshot, type_manager)?;
    get_some_or_return_does_not_exist!(existing_supertype = existing_supertype_opt);

    Ok(if existing_supertype == supertype {
        DefinableStatus::ExistsSame(None)
    } else {
        DefinableStatus::ExistsDifferent(existing_supertype)
    })
}

pub(crate) fn get_value_type_status(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    attribute_type: AttributeType<'_>,
    value_type: ValueType,
) -> Result<DefinableStatus<ValueType>, ConceptReadError> {
    let existing_value_type_opt = attribute_type.get_value_type_declared(snapshot, type_manager)?;
    get_some_or_return_does_not_exist!(existing_value_type = existing_value_type_opt);

    Ok(if existing_value_type == value_type {
        DefinableStatus::ExistsSame(None)
    } else {
        DefinableStatus::ExistsDifferent(existing_value_type)
    })
}

pub(crate) fn get_relates_status(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    relation_type: RelationType<'static>,
    role_label: &Label<'_>,
    ordering: Ordering,
) -> Result<DefinableStatus<(Relates<'static>, Ordering)>, ConceptReadError> {
    let existing_relates_opt =
        try_resolve_relates_declared(snapshot, type_manager, relation_type, role_label.name.as_str())?;
    get_some_or_return_does_not_exist!(existing_relates = existing_relates_opt);

    let existing_ordering = existing_relates.role().get_ordering(snapshot, type_manager)?;
    Ok(if existing_ordering == ordering {
        DefinableStatus::ExistsSame(Some((existing_relates, existing_ordering)))
    } else {
        DefinableStatus::ExistsDifferent((existing_relates, existing_ordering))
    })
}

pub(crate) fn get_owns_status(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    object_type: ObjectType<'static>,
    attribute_type: AttributeType<'static>,
    ordering: Ordering,
) -> Result<DefinableStatus<(Owns<'static>, Ordering)>, ConceptReadError> {
    let existing_owns_opt = try_resolve_owns_declared(snapshot, type_manager, object_type, attribute_type)?;
    get_some_or_return_does_not_exist!(existing_owns = existing_owns_opt);

    let existing_ordering = existing_owns.get_ordering(snapshot, type_manager)?;
    Ok(if existing_ordering == ordering {
        DefinableStatus::ExistsSame(Some((existing_owns, existing_ordering)))
    } else {
        DefinableStatus::ExistsDifferent((existing_owns, existing_ordering))
    })
}

pub(crate) fn get_plays_status(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    object_type: ObjectType<'static>,
    role_type: RoleType<'static>,
) -> Result<DefinableStatus<Plays<'static>>, ConceptReadError> {
    let existing_plays_opt = try_resolve_plays_declared(snapshot, type_manager, object_type, role_type)?;
    get_some_or_return_does_not_exist!(existing_plays = existing_plays_opt);

    Ok(DefinableStatus::ExistsSame(Some(existing_plays)))
}
