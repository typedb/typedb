/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::error::Error;
use std::fmt;
use concept::{thing::thing_manager::ThingManager, type_::type_manager::TypeManager};
use error::typedb_error;
use storage::snapshot::WritableSnapshot;
use typeql::{schema::undefinable::{
    Function, Label as TypeQLLabel,
    Struct, CapabilityType, Specialise, AnnotationCapability, AnnotationType,
}, Definable, token};
use typeql::query::schema::Undefine;
use typeql::schema::undefinable::Undefinable;
use answer::Type;
use concept::error::{ConceptReadError, ConceptWriteError};
use concept::type_::annotation::Annotation;
use concept::type_::{Ordering, TypeAPI};
use encoding::value::label::Label;
use encoding::value::value_type::ValueType;
use ir::LiteralParseError;
use crate::definable_resolution::{filter_variants, resolve_struct_definition_key, resolve_typeql_type, SymbolResolutionError, try_resolve_typeql_type};
use crate::definable_status::{DefinableStatus, get_struct_status};
use crate::define::DefineError;
use crate::redefine::RedefineError;

pub(crate) fn execute(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    undefine: Undefine,
) -> Result<(), UndefineError> {
    process_function_undefinitions(snapshot, type_manager, thing_manager, &undefine.undefinables)?;
    process_specialise_undefinitions(snapshot, type_manager, thing_manager, &undefine.undefinables)?;
    process_capability_annotation_undefinitions(snapshot, type_manager, thing_manager, &undefine.undefinables)?;
    process_type_capability_undefinitions(snapshot, type_manager, thing_manager, &undefine.undefinables)?;
    process_type_annotation_undefinitions(snapshot, type_manager, thing_manager, &undefine.undefinables)?;
    process_type_undefinitions(snapshot, type_manager, thing_manager, &undefine.undefinables)?;
    process_struct_undefinitions(snapshot, type_manager, thing_manager, &undefine.undefinables)?;
    Ok(())
}

fn process_function_undefinitions(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    undefinables: &[Undefinable],
) -> Result<(), UndefineError> {
    filter_variants!(Undefinable::Function : undefinables)
        .try_for_each(|function| undefine_function(snapshot, type_manager, function))?;
    Ok(())
}

fn process_specialise_undefinitions(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    undefinables: &[Undefinable],
) -> Result<(), UndefineError> {
    filter_variants!(Undefinable::Specialise : undefinables)
        .try_for_each(|specialise| undefine_specialise(snapshot, type_manager, thing_manager, specialise))?;
    Ok(())
}

fn process_capability_annotation_undefinitions(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    undefinables: &[Undefinable],
) -> Result<(), UndefineError> {
    filter_variants!(Undefinable::AnnotationCapability : undefinables)
        .try_for_each(|annotation| undefine_capability_annotation(snapshot, type_manager, thing_manager, annotation))?;
    Ok(())
}

fn process_type_capability_undefinitions(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    undefinables: &[Undefinable],
) -> Result<(), UndefineError> {
    filter_variants!(Undefinable::CapabilityType : undefinables)
        .try_for_each(|capability| undefine_type_capability(snapshot, type_manager, thing_manager, capability))?;
    Ok(())
}

fn process_type_annotation_undefinitions(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    undefinables: &[Undefinable],
) -> Result<(), UndefineError> {
    filter_variants!(Undefinable::AnnotationType : undefinables)
        .try_for_each(|annotation| undefine_type_annotation(snapshot, type_manager, thing_manager, annotation))?;
    Ok(())
}

fn process_type_undefinitions(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    undefinables: &[Undefinable],
) -> Result<(), UndefineError> {
    filter_variants!(Undefinable::Type : undefinables)
        .try_for_each(|type_| undefine_type(snapshot, type_manager, thing_manager, type_))?;
    Ok(())
}

fn process_struct_undefinitions(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    undefinables: &[Undefinable],
) -> Result<(), UndefineError> {
    filter_variants!(Undefinable::Struct : undefinables)
        .try_for_each(|struct_| undefine_struct(snapshot, type_manager, thing_manager, struct_))?;
    Ok(())
}

fn undefine_function(
    _snapshot: &mut impl WritableSnapshot,
    _type_manager: &TypeManager,
    _function_undefinable: &Function,
) -> Result<(), UndefineError> {
    Err(UndefineError::Unimplemented { description: "Function undefinition.".to_string() })
}

fn undefine_specialise(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    specialise_undefinable: &Specialise,
) -> Result<(), UndefineError> {
    Err(UndefineError::Unimplemented { description: "Specialise undefinition.".to_string() })
}

fn undefine_capability_annotation(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    annotation_undefinable: &AnnotationCapability,
) -> Result<(), UndefineError> {
    Err(UndefineError::Unimplemented { description: "AnnotationCapability undefinition.".to_string() })
}

fn undefine_type_capability(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    capability_undefinable: &CapabilityType,
) -> Result<(), UndefineError> {
    Err(UndefineError::Unimplemented { description: "CapabilityType undefinition.".to_string() })
}

fn undefine_type_annotation(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    annotation_undefinable: &AnnotationType,
) -> Result<(), UndefineError> {
    Err(UndefineError::Unimplemented { description: "AnnotationType undefinition.".to_string() })
}

fn undefine_type(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    label_undefinable: &TypeQLLabel,
) -> Result<(), UndefineError> {
    let label = Label::parse_from(label_undefinable.ident.as_str());
    let existing = resolve_typeql_type(snapshot, type_manager, &label).map_err(|source| RedefineError::DefinitionResolution { typedb_source: source })?;
    match existing {
        Type::Entity(type_) => type_.delete(snapshot, type_manager, thing_manager),
        Type::Relation(type_) => type_.delete(snapshot, type_manager, thing_manager),
        Type::Attribute(type_) => type_.delete(snapshot, type_manager, thing_manager),
        Type::RoleType(type_) => type_.delete(snapshot, type_manager, thing_manager),
    }.map_err(|err| UndefineError::TypeDeleteError {
        typedb_source: err,
        label,
        undefinition: label_undefinable.clone(),
    })?;
    Ok(())
}

fn undefine_struct(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    struct_undefinable: &Struct,
) -> Result<(), UndefineError> {
    let name = struct_undefinable.ident.as_str();
    let struct_key = resolve_struct_definition_key(snapshot, type_manager, name)
        .map_err(|source| RedefineError::DefinitionResolution { typedb_source: source })?;

    type_manager.delete_struct(snapshot, thing_manager, &struct_key).map_err(|err| UndefineError::StructDeleteError {
        typedb_source: err,
        struct_name: name.to_owned(),
        undefinition: struct_undefinable.clone(),
    })?;
    Ok(())
}

typedb_error!(
    pub UndefineError(component = "Undefine execution", prefix = "UEX") {
        Unimplemented(1, "Unimplemented undefine functionality: {description}", description: String),
        UnexpectedConceptRead(2, "Concept read error during undefine query execution.", ( source: ConceptReadError )),
        DefinitionResolution(3, "Could not find symbol in undefine query.", ( typedb_source: SymbolResolutionError )),
        LiteralParseError(4, "Error parsing literal in undefine query.", ( source : LiteralParseError )),
        StructDoesNotExist(5, "Struct used in undefine query does not exist.\nSource:\n{declaration}", undefinition: Struct),
        StructDeleteError(
            6,
            "Error removing struct type '{struct_name}'.\nSource:\n{undefinition}",
            struct_name: String,
            undefinition: Struct,
            ( typedb_source: ConceptWriteError )
        ),
        TypeDeleteError(
            7,
            "Error removing type '{label}'.\nSource:\n{undefinition}",
            label: Label,
            undefinition: TypeQLLabel,
            ( typedb_source: ConceptWriteError )
        ),
        ValueTypeSymbolResolution(
            7,
            "Error resolving value type in undefine query.",
            ( typedb_source: SymbolResolutionError )
        ),
        TypeSubIsNotDefined(
            8,
            "Undefining the supertype of '{label}' to '{new_supertype_label}' failed, since there is no supertype to replace. Use define to set instead of replace the supertype.",
            label: Label<'static>,
            new_supertype_label: Label<'static>
        ),
        RelatesIsNotDefined(
            9,
            "On type '{label}', undefining 'relates {role}{ordering}' failed since it this 'relates' doesn't yet exist. Undefine can only replace existing schema elements.\nSource:\n{undefinition}",
            label: Label<'static>,
            role: String,
            ordering: Ordering,
            undefinition: Capability
        ),
        OwnsIsNotDefined(
            10,
            "On type '{label}', undefining 'owns {attribute}{ordering}' failed since this 'owns' doesn't exist yet. Undefine can only replace existing schema elements.\nSource:\n{undefinition}",
            label: Label<'static>,
            attribute: Label<'static>,
            undefinition: Capability,
            ordering: Ordering
        ),
        PlaysIsNotDefined(
            11,
            "On type '{label}', undefining 'plays {role_label}' failed since this 'plays' doesn't exist yet. Undefine can only replace existing schema elements.\nSource:\n{undefinition}",
            label: Label<'static>,
            role_label: Label<'static>,
            undefinition: Capability
        ),
        RelatesSpecialiseIsNotDefined(
            12,
            "For type '{label}', undefining specialise 'as {new_specialise_type}' failed as no specialise exists yet. Undefine can only replace existing schema elements.\nSource:\n{undefinition}",
            label: Label<'static>,
            new_specialise_type: Label<'static>,
            undefinition: TypeQLRelates
        ),
        AttributeTypeValueTypeIsNotDefined(
            13,
            "For attribute type '{label}', undefining value type '{value_type}' failed since no value type exists yet. Undefine can only replace existing schema elements.",
            label: Label<'static>,
            value_type: ValueType
        ),
        TypeAnnotationIsNotDefined(
            14,
            "For type '{label}', undefining annotation '{annotation}' failed since this annotation doesn't exist yet. Undefine can only replace existing schema elements.",
            label: Label<'static>,
            annotation: Annotation
        ),
        CapabilityAnnotationIsNotDefined(
            15,
            "Undefining annotation '{annotation}' failed since this annotation doesn't exist yet. Undefine can only replace existing schema elements.\nSource:\n{undefinition}",
            undefinition: Capability,
            annotation: Annotation
        ),
        UnsetValueType(
            16,
            "Undefining '{label}' to have value type '{value_type}' failed.",
            label: Label<'static>,
            value_type: ValueType,
            ( typedb_source: ConceptWriteError )
        ),
        UnsetTypeAnnotation(
            17,
            "Undefining '{label}' to have annotation '{annotation}' failed.\nSource:\n{undefinition}",
            label: Label<'static>,
            annotation: Annotation,
            undefinition: Type,
            ( typedb_source: ConceptWriteError )
        ),
        UnsetCapabilityAnnotation(
            18,
            "Undefining '{annotation}' failed.\nSource:\n{undefinition}",
            annotation: Annotation,
            undefinition: Capability,
            ( typedb_source: ConceptWriteError )
        ),
        UnsetRelatesSpecialise(
            19,
            "For relation type '{label}', undefining 'relates' failed.\nSource:\n{undefinition}",
            label: Label<'static>,
            undefinition: TypeQLRelates,
            ( typedb_source: ConceptWriteError )
        ),
    }
);

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