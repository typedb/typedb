/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{error::Error, fmt};

use concept::{
    error::{ConceptReadError, ConceptWriteError},
    thing::thing_manager::ThingManager,
    type_::{
        annotation::{Annotation, AnnotationError},
        attribute_type::AttributeType,
        object_type::ObjectType,
        owns::Owns,
        plays::Plays,
        relates::{Relates, RelatesAnnotation},
        type_manager::TypeManager,
        Ordering, OwnerAPI, PlayerAPI,
    },
};
use encoding::{
    graph::type_::Kind,
    value::{label::Label, value_type::ValueType},
};
use ir::{translation::tokens::translate_annotation, LiteralParseError};
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};
use typeql::{
    schema::definable::{
        struct_::Field,
        type_::{
            capability::{Owns as TypeQLOwns, Plays as TypeQLPlays, Relates as TypeQLRelates}}},
    common::token,
    query::schema::Undefine,
    type_::Optional,
    ScopedLabel, TypeRef, TypeRefAny,
};
use typeql::schema::definable::Type;
use typeql::schema::definable::type_::{Capability, CapabilityBase};
use typeql::schema::undefinable::{Struct, Undefinable};

use crate::{
    util::{
        capability_convert_and_validate_annotation_definition_need, resolve_type, resolve_value_type,
        type_convert_and_validate_annotation_definition_need, type_ref_to_label_and_ordering,
    },
    SymbolResolutionError,
};
use crate::definition_status::{DefinitionStatus, get_struct_field_status};

use crate::util::{check_can_and_need_define_override, check_can_and_need_define_supertype, filter_variants, try_unwrap};

pub(crate) fn execute(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    undefine: Undefine,
) -> Result<(), UndefineError> {
    process_struct_redefinitions(snapshot, type_manager, thing_manager, &undefine.undefinables)?;
    process_type_redefinitions(snapshot, type_manager, thing_manager, &undefine.undefinables)?;
    process_function_redefinitions(snapshot, type_manager, &undefine.undefinables)?;
    Ok(())
}

fn process_struct_redefinitions(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    undefinables: &[Undefinable],
) -> Result<(), UndefineError> {
    filter_variants!(Undefinable::Struct : undefinables).try_for_each(|struct_| undefine_struct(&struct_))?;
    filter_variants!(Undefinable::Struct : undefinables)
        .try_for_each(|struct_| undefine_struct_fields(snapshot, type_manager, thing_manager, &struct_))?;
    Ok(())
}

fn process_type_redefinitions(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    undefinables: &[Undefinable],
) -> Result<(), UndefineError> {
    let declarations = filter_variants!(Undefinable::Type : undefinables);
    // declarations.clone().try_for_each(|declaration| undefine_types(snapshot, type_manager, declaration))?;
    // declarations
    //     .clone()
    //     .try_for_each(|declaration| undefine_type_annotations(snapshot, type_manager, thing_manager, declaration))?;
    // declarations.clone().try_for_each(|declaration| undefine_sub(snapshot, type_manager, thing_manager, declaration))?;
    // declarations
    //     .clone()
    //     .try_for_each(|declaration| undefine_value_type(snapshot, type_manager, thing_manager, declaration))?;
    // declarations.clone().try_for_each(|declaration| undefine_alias(snapshot, type_manager, declaration))?;
    // declarations
    //     .clone()
    //     .try_for_each(|declaration| undefine_relates(snapshot, type_manager, thing_manager, declaration))?;
    // declarations.clone().try_for_each(|declaration| undefine_owns(snapshot, type_manager, thing_manager, declaration))?;
    // declarations
    //     .clone()
    //     .try_for_each(|declaration| undefine_plays(snapshot, type_manager, thing_manager, declaration))?;
    Ok(())
}

fn process_function_redefinitions(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    undefinables: &[Undefinable],
) -> Result<(), UndefineError> {
    filter_variants!(Undefinable::Function : undefinables)
        .try_for_each(|declaration| undefine_functions(snapshot, type_manager, undefinables))?;
    Ok(())
}

fn undefine_struct(
    struct_definable: &Struct,
) -> Result<(), UndefineError> {
    Ok(())
}

fn undefine_struct_fields(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    struct_definable: &Struct,
) -> Result<(), UndefineError> {
    Ok(())
}

// TODO: Make a method with one source of these errors for Define, Undefine
fn get_struct_field_value_type_optionality(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    field: &Field,
) -> Result<(ValueType, bool), UndefineError> {
    let optional = matches!(&field.type_, TypeRefAny::Optional(_));
    match &field.type_ {
        TypeRefAny::Type(TypeRef::Named(named))
        | TypeRefAny::Optional(Optional { inner: TypeRef::Named(named), .. }) => {
            let value_type = resolve_value_type(snapshot, type_manager, named)
                .map_err(|source| UndefineError::StructFieldCouldNotResolveValueType { source })?;
            Ok((value_type, optional))
        }
        TypeRefAny::Type(TypeRef::Variable(_)) | TypeRefAny::Optional(Optional { inner: TypeRef::Variable(_), .. }) => {
            return Err(UndefineError::StructFieldIllegalVariable { field_declaration: field.clone() });
        }
        TypeRefAny::List(_) => {
            return Err(UndefineError::StructFieldIllegalList { field_declaration: field.clone() });
        }
    }
}

fn undefine_types(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    type_declaration: &Type,
) -> Result<(), UndefineError> {
    Err(UndefineError::TypeRedefinitionIsNotSupported { type_declaration: type_declaration.clone() })
}

fn undefine_type_annotations(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    type_declaration: &Type,
) -> Result<(), UndefineError> {
    Ok(())
}

fn undefine_alias(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    type_declaration: &Type,
) -> Result<(), UndefineError> {
    &type_declaration
        .capabilities
        .iter()
        .filter_map(|capability| try_unwrap!(CapabilityBase::Alias = &capability.base))
        .try_for_each(|_| Err(UndefineError::Unimplemented))?;

    // TODO: Uncomment when alias is implemented
    // undefine_alias_annotations(capability)?;

    Ok(())
}

fn undefine_alias_annotations(typeql_capability: &Capability) -> Result<(), UndefineError> {
    // verify_empty_annotations_for_capability!(typeql_capability, AnnotationError::UnsupportedAnnotationForAlias)
    Ok(())
}

fn undefine_sub(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    type_declaration: &Type,
) -> Result<(), UndefineError> {
    Ok(())
}

fn undefine_sub_annotations(typeql_capability: &Capability) -> Result<(), UndefineError> {
    // verify_empty_annotations_for_capability!(typeql_capability, AnnotationError::UnsupportedAnnotationForSub)
    Ok(())
}

fn undefine_value_type(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    type_declaration: &Type,
) -> Result<(), UndefineError> {
    Ok(())
}

fn undefine_value_type_annotations<'a>(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    attribute_type: AttributeType<'a>,
    attribute_type_label: &Label<'a>,
    typeql_capability: &Capability,
) -> Result<(), UndefineError> {
    Ok(())
}

fn undefine_relates(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    type_declaration: &Type,
) -> Result<(), UndefineError> {
    Ok(())
}

fn undefine_relates_overridden<'a>(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    relation_label: &Label<'a>,
    relates: Relates<'a>,
    typeql_relates: &TypeQLRelates,
) -> Result<(), UndefineError> {
    Ok(())
}

fn undefine_relates_annotations<'a>(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    relation_label: &Label<'a>,
    relates: Relates<'a>,
    typeql_capability: &Capability,
    is_new: bool,
) -> Result<(), UndefineError> {
    Ok(())
}

fn undefine_owns(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    type_declaration: &Type,
) -> Result<(), UndefineError> {
    Ok(())
}

fn undefine_owns_overridden<'a>(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    owner_label: &Label<'a>,
    owns: Owns<'a>,
    typeql_owns: &TypeQLOwns,
) -> Result<(), UndefineError> {
    Ok(())
}

fn undefine_owns_annotations<'a>(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    owner_label: &Label<'a>,
    owns: Owns<'a>,
    typeql_capability: &Capability,
) -> Result<(), UndefineError> {
    Ok(())
}

fn undefine_plays(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    type_declaration: &Type,
) -> Result<(), UndefineError> {
    Ok(())
}

fn undefine_plays_overridden<'a>(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    player_label: &Label<'a>,
    plays: Plays<'a>,
    typeql_plays: &TypeQLPlays,
) -> Result<(), UndefineError> {
    Ok(())
}

fn undefine_plays_annotations<'a>(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    player_label: &Label<'a>,
    plays: Plays<'a>,
    typeql_capability: &Capability,
) -> Result<(), UndefineError> {
    Ok(())
}

fn undefine_functions(
    snapshot: &impl WritableSnapshot,
    type_manager: &TypeManager,
    undefinables: &[Undefinable],
) -> Result<(), UndefineError> {
    Ok(())
}

fn err_capability_kind_mismatch(
    capability_receiver: &Label<'_>,
    capability_provider: &Label<'_>,
    capability: &Capability,
    expected_kind: Kind,
    actual_kind: Kind,
) -> UndefineError {
    UndefineError::CapabilityKindMismatch {
        capability_receiver: capability_receiver.clone().into_owned(),
        capability_provider: capability_provider.clone().into_owned(),
        capability: capability.clone(),
        expected_kind,
        actual_kind,
    }
}

#[derive(Debug)]
pub enum UndefineError {
    Unimplemented,
    UnexpectedConceptRead {
        source: ConceptReadError,
    },
    TypeLookup {
        source: SymbolResolutionError,
    },
    TypeCreateRequiresKind {
        type_declaration: Type,
    },
    TypeCreateError {
        source: ConceptWriteError,
        type_declaration: Type,
    },
    RoleTypeDirectCreate {
        type_declaration: Type,
    },
    StructRedefinitionIsNotSupported {
        struct_declaration: Struct,
    },
    StructFieldDoesNotExist {
        field: Field,
    },
    StructFieldRedefinitionDoesNotChangeAnything {
        field: Field,
    },
    StructFieldDeleteError {
        source: ConceptWriteError,
        struct_name: String,
        struct_field: Field,
    },
    StructFieldCreateError {
        source: ConceptWriteError,
        struct_name: String,
        struct_field: Field,
    },
    StructFieldIllegalList {
        field_declaration: Field,
    },
    StructFieldIllegalVariable {
        field_declaration: Field,
    },
    StructFieldIllegalNotValueType {
        scoped_label: ScopedLabel,
    },
    StructFieldCouldNotResolveValueType {
        source: SymbolResolutionError,
    },
    TypeRedefinitionIsNotSupported {
        type_declaration: Type,
    },
    SetSupertype {
        sub: typeql::schema::definable::type_::capability::Sub,
        source: ConceptWriteError,
    },
    TypeCannotHaveCapability {
        label: Label<'static>,
        kind: Kind,
        capability: Capability,
    },
    AttributeTypeBadValueType {
        source: SymbolResolutionError,
    },
    TypeAlreadyHasDifferentUndefinedSub {
        label: Label<'static>,
        supertype: Label<'static>,
        existing_supertype: Label<'static>,
    },
    CapabilityAlreadyHasDifferentUndefinedOverride {
        label: Label<'static>,
        overridden_interface: Label<'static>,
        existing_overridden_interface: Label<'static>,
    },
    AttributeTypeAlreadyHasDifferentUndefinedValueType {
        label: Label<'static>,
        value_type: ValueType,
        existing_value_type: ValueType,
    },
    // Careful with the error message as it is also used for value types (stored on their attribute type)!
    TypeAnnotationIsAlreadyUndefinedWithDifferentArguments {
        label: Label<'static>,
        annotation: Annotation,
        existing_annotation: Annotation,
    },
    CapabilityAnnotationIsAlreadyUndefinedWithDifferentArguments {
        label: Label<'static>,
        annotation: Annotation,
        existing_annotation: Annotation,
    },
    SetValueType {
        label: Label<'static>,
        value_type: ValueType,
        source: ConceptWriteError,
    },
    RelatesRoleMustBeLabelAndNotOptional {
        relation: Label<'static>,
        role_label: TypeRefAny,
    },
    CreateRelates {
        relates: TypeQLRelates,
        source: ConceptWriteError,
    },
    OverriddenRelatesNotFound {
        relates: TypeQLRelates,
    },
    CreatePlays {
        plays: TypeQLPlays,
        source: ConceptWriteError,
    },
    CreatePlaysRoleTypeNotFound {
        plays: TypeQLPlays,
    },
    OverriddenPlaysNotFound {
        plays: TypeQLPlays,
    },
    OverriddenPlaysRoleTypeNotFound {
        plays: TypeQLPlays,
    },
    OwnsAttributeMustBeLabelOrList {
        owns: TypeQLOwns,
    },
    CreateOwns {
        owns: TypeQLOwns,
        source: ConceptWriteError,
    },
    CreateOwnsAttributeTypeNotFound {
        owns: TypeQLOwns,
    },
    OverriddenOwnsNotFound {
        owns: TypeQLOwns,
    },
    OverriddenOwnsAttributeTypeNotFound {
        owns: TypeQLOwns,
    },
    CreateRelatesModifiesExistingOrdering {
        label: Label<'static>,
        existing_ordering: Ordering,
        new_ordering: Ordering,
    },
    IllegalAnnotation {
        source: AnnotationError,
    },
    SetAnnotation {
        source: ConceptWriteError,
        label: Label<'static>,
        annotation: Annotation,
    },
    SetOverride {
        source: ConceptWriteError,
        label: Label<'static>,
    },
    CapabilityKindMismatch {
        capability_receiver: Label<'static>,
        capability_provider: Label<'static>,
        capability: Capability,
        expected_kind: Kind,
        actual_kind: Kind,
    },
    LiteralParseError {
        source: LiteralParseError,
    },
}

fn err_unsupported_capability(label: &Label<'static>, kind: Kind, capability: &Capability) -> UndefineError {
    UndefineError::TypeCannotHaveCapability { label: label.to_owned(), kind, capability: capability.clone() }
}

impl fmt::Display for UndefineError {
    fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl Error for UndefineError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        todo!()
    }
}
