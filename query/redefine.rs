/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{error::Error, fmt};

use answer::Type as TypeEnum;
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
    annotation::Annotation as TypeQLAnnotation,
    common::token,
    query::schema::Redefine,
    schema::definable::{
        struct_::Field,
        type_::{
            capability::{Owns as TypeQLOwns, Plays as TypeQLPlays, Relates as TypeQLRelates},
            Capability, CapabilityBase,
        },
        Struct, Type,
    },
    type_::Optional,
    Definable, ScopedLabel, TypeRef, TypeRefAny,
};

use crate::{
    util::{
        capability_convert_and_validate_annotation_definition_need, resolve_type, resolve_value_type,
        type_convert_and_validate_annotation_definition_need, type_ref_to_label_and_ordering,
    },
    SymbolResolutionError,
};
use crate::definition_status::{DefinitionStatus, get_struct_field_status};

use crate::util::{check_can_and_need_define_override, check_can_and_need_define_supertype, filter_variants, try_unwrap};

// TODO: DefinableStatus::ExistsSame for types/structs (they are unchangeable) results in error. However, ExistsSame for AnnotationAbstract (and any property without values) is Ok for now (because it's easier for now)... Discuss.

pub(crate) fn execute(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    redefine: Redefine,
) -> Result<(), RedefineError> {
    process_struct_redefinitions(snapshot, type_manager, thing_manager, &redefine.definables)?;
    process_type_redefinitions(snapshot, type_manager, thing_manager, &redefine.definables)?;
    process_function_redefinitions(snapshot, type_manager, &redefine.definables)?;
    Ok(())
}

fn process_struct_redefinitions(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    definables: &[Definable],
) -> Result<(), RedefineError> {
    filter_variants!(Definable::Struct : definables).try_for_each(|struct_| redefine_struct(struct_))?;
    filter_variants!(Definable::Struct : definables)
        .try_for_each(|struct_| redefine_struct_fields(snapshot, type_manager, thing_manager, &struct_))?;
    Ok(())
}

fn process_type_redefinitions(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    definables: &[Definable],
) -> Result<(), RedefineError> {
    let declarations = filter_variants!(Definable::TypeDeclaration : definables);
    declarations.clone().try_for_each(|declaration| redefine_types(snapshot, type_manager, declaration))?;
    declarations
        .clone()
        .try_for_each(|declaration| redefine_type_annotations(snapshot, type_manager, thing_manager, declaration))?;
    declarations.clone().try_for_each(|declaration| redefine_sub(snapshot, type_manager, thing_manager, declaration))?;
    declarations
        .clone()
        .try_for_each(|declaration| redefine_value_type(snapshot, type_manager, thing_manager, declaration))?;
    declarations.clone().try_for_each(|declaration| redefine_alias(snapshot, type_manager, declaration))?;
    declarations
        .clone()
        .try_for_each(|declaration| redefine_relates(snapshot, type_manager, thing_manager, declaration))?;
    declarations.clone().try_for_each(|declaration| redefine_owns(snapshot, type_manager, thing_manager, declaration))?;
    declarations
        .clone()
        .try_for_each(|declaration| redefine_plays(snapshot, type_manager, thing_manager, declaration))?;
    Ok(())
}

fn process_function_redefinitions(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    definables: &[Definable],
) -> Result<(), RedefineError> {
    filter_variants!(Definable::Function : definables)
        .try_for_each(|declaration| redefine_functions(snapshot, type_manager, definables))?;
    Ok(())
}

fn redefine_struct(
    struct_definable: &Struct,
) -> Result<(), RedefineError> {
    Err(RedefineError::StructRedefinitionIsNotSupported { struct_declaration: struct_definable.clone() })
}

fn redefine_struct_fields(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    struct_definable: &Struct,
) -> Result<(), RedefineError> {
    let name = struct_definable.ident.as_str();
    let struct_key = type_manager
        .get_struct_definition_key(snapshot, name)
        .map_err(|err| RedefineError::UnexpectedConceptRead { source: err })?
        .unwrap();

    for field in &struct_definable.fields {
        let (value_type, optional) = get_struct_field_value_type_optionality(snapshot, type_manager, field)?;

        let definable_status = get_struct_field_status(snapshot, type_manager, struct_key.clone(), field.key.as_str(), value_type.clone(), optional).map_err(|source| RedefineError::UnexpectedConceptRead { source })?;
        match definable_status {
            DefinitionStatus::DoesNotExist => return Err(RedefineError::StructFieldDoesNotExist { field: field.to_owned() }),
            DefinitionStatus::ExistsSame => return Err(RedefineError::StructFieldRedefinitionDoesNotChangeAnything { field: field.to_owned() }), // TODO: Should error?
            DefinitionStatus::ExistsDifferent(_) => {}
        }

        type_manager.delete_struct_field(snapshot, thing_manager, struct_key.clone(), field.key.as_str())
            .map_err(|err| RedefineError::StructFieldDeleteError {
                source: err,
                struct_name: name.to_owned(),
                struct_field: field.clone(),
            })?;

        type_manager
            .create_struct_field(snapshot, struct_key.clone(), field.key.as_str(), value_type, optional)
            .map_err(|err| RedefineError::StructFieldCreateError {
                source: err,
                struct_name: name.to_owned(),
                struct_field: field.clone(),
            })?;
    }
    Ok(())
}

// TODO: Make a method with one source of these errors for Define, Redefine
fn get_struct_field_value_type_optionality(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    field: &Field,
) -> Result<(ValueType, bool), RedefineError> {
    let optional = matches!(&field.type_, TypeRefAny::Optional(_));
    match &field.type_ {
        TypeRefAny::Type(TypeRef::Named(named))
        | TypeRefAny::Optional(Optional { inner: TypeRef::Named(named), .. }) => {
            let value_type = resolve_value_type(snapshot, type_manager, named)
                .map_err(|source| RedefineError::StructFieldCouldNotResolveValueType { source })?;
            Ok((value_type, optional))
        }
        TypeRefAny::Type(TypeRef::Variable(_)) | TypeRefAny::Optional(Optional { inner: TypeRef::Variable(_), .. }) => {
            return Err(RedefineError::StructFieldIllegalVariable { field_declaration: field.clone() });
        }
        TypeRefAny::List(_) => {
            return Err(RedefineError::StructFieldIllegalList { field_declaration: field.clone() });
        }
    }
}

fn redefine_types(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    type_declaration: &Type,
) -> Result<(), RedefineError> {
    Err(RedefineError::TypeRedefinitionIsNotSupported { type_declaration: type_declaration.clone() })
}

fn redefine_type_annotations(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    type_declaration: &Type,
) -> Result<(), RedefineError> {
    Ok(())
}

fn redefine_alias(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    type_declaration: &Type,
) -> Result<(), RedefineError> {
    &type_declaration
        .capabilities
        .iter()
        .filter_map(|capability| try_unwrap!(CapabilityBase::Alias = &capability.base))
        .try_for_each(|_| Err(RedefineError::Unimplemented))?;

    // TODO: Uncomment when alias is implemented
    // redefine_alias_annotations(capability)?;

    Ok(())
}

fn redefine_alias_annotations(typeql_capability: &Capability) -> Result<(), RedefineError> {
    // verify_empty_annotations_for_capability!(typeql_capability, AnnotationError::UnsupportedAnnotationForAlias)
    Ok(())
}

fn redefine_sub(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    type_declaration: &Type,
) -> Result<(), RedefineError> {
    Ok(())
}

fn redefine_sub_annotations(typeql_capability: &Capability) -> Result<(), RedefineError> {
    // verify_empty_annotations_for_capability!(typeql_capability, AnnotationError::UnsupportedAnnotationForSub)
    Ok(())
}

fn redefine_value_type(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    type_declaration: &Type,
) -> Result<(), RedefineError> {
    Ok(())
}

fn redefine_value_type_annotations<'a>(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    attribute_type: AttributeType<'a>,
    attribute_type_label: &Label<'a>,
    typeql_capability: &Capability,
) -> Result<(), RedefineError> {
    Ok(())
}

fn redefine_relates(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    type_declaration: &Type,
) -> Result<(), RedefineError> {
    Ok(())
}

fn redefine_relates_overridden<'a>(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    relation_label: &Label<'a>,
    relates: Relates<'a>,
    typeql_relates: &TypeQLRelates,
) -> Result<(), RedefineError> {
    Ok(())
}

fn redefine_relates_annotations<'a>(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    relation_label: &Label<'a>,
    relates: Relates<'a>,
    typeql_capability: &Capability,
    is_new: bool,
) -> Result<(), RedefineError> {
    Ok(())
}

fn redefine_owns(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    type_declaration: &Type,
) -> Result<(), RedefineError> {
    Ok(())
}

fn redefine_owns_overridden<'a>(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    owner_label: &Label<'a>,
    owns: Owns<'a>,
    typeql_owns: &TypeQLOwns,
) -> Result<(), RedefineError> {
    Ok(())
}

fn redefine_owns_annotations<'a>(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    owner_label: &Label<'a>,
    owns: Owns<'a>,
    typeql_capability: &Capability,
) -> Result<(), RedefineError> {
    Ok(())
}

fn redefine_plays(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    type_declaration: &Type,
) -> Result<(), RedefineError> {
    Ok(())
}

fn redefine_plays_overridden<'a>(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    player_label: &Label<'a>,
    plays: Plays<'a>,
    typeql_plays: &TypeQLPlays,
) -> Result<(), RedefineError> {
    Ok(())
}

fn redefine_plays_annotations<'a>(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    player_label: &Label<'a>,
    plays: Plays<'a>,
    typeql_capability: &Capability,
) -> Result<(), RedefineError> {
    Ok(())
}

fn redefine_functions(
    snapshot: &impl WritableSnapshot,
    type_manager: &TypeManager,
    definables: &[Definable],
) -> Result<(), RedefineError> {
    Ok(())
}

fn err_capability_kind_mismatch(
    capability_receiver: &Label<'_>,
    capability_provider: &Label<'_>,
    capability: &Capability,
    expected_kind: Kind,
    actual_kind: Kind,
) -> RedefineError {
    RedefineError::CapabilityKindMismatch {
        capability_receiver: capability_receiver.clone().into_owned(),
        capability_provider: capability_provider.clone().into_owned(),
        capability: capability.clone(),
        expected_kind,
        actual_kind,
    }
}

#[derive(Debug)]
pub enum RedefineError {
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
    TypeAlreadyHasDifferentRedefinedSub {
        label: Label<'static>,
        supertype: Label<'static>,
        existing_supertype: Label<'static>,
    },
    CapabilityAlreadyHasDifferentRedefinedOverride {
        label: Label<'static>,
        overridden_interface: Label<'static>,
        existing_overridden_interface: Label<'static>,
    },
    AttributeTypeAlreadyHasDifferentRedefinedValueType {
        label: Label<'static>,
        value_type: ValueType,
        existing_value_type: ValueType,
    },
    // Careful with the error message as it is also used for value types (stored on their attribute type)!
    TypeAnnotationIsAlreadyRedefinedWithDifferentArguments {
        label: Label<'static>,
        annotation: Annotation,
        existing_annotation: Annotation,
    },
    CapabilityAnnotationIsAlreadyRedefinedWithDifferentArguments {
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

fn err_unsupported_capability(label: &Label<'static>, kind: Kind, capability: &Capability) -> RedefineError {
    RedefineError::TypeCannotHaveCapability { label: label.to_owned(), kind, capability: capability.clone() }
}

impl fmt::Display for RedefineError {
    fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl Error for RedefineError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        todo!()
    }
}
