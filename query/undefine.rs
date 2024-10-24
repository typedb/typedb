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
        annotation::{Annotation, AnnotationCategory, AnnotationError},
        attribute_type::AttributeTypeAnnotation,
        type_manager::TypeManager,
        Ordering, OwnerAPI, PlayerAPI, TypeAPI,
    },
};
use encoding::{
    graph::type_::Kind,
    value::{label::Label, value_type::ValueType},
};
use error::typedb_error;
use ir::{translation::tokens::translate_annotation_category, LiteralParseError};
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};
use typeql::{
    query::schema::Undefine,
    schema::{
        definable::type_::{
            capability::{
                Alias as TypeQLAlias, Owns as TypeQLOwns, Plays as TypeQLPlays, Relates as TypeQLRelates,
                Sub as TypeQLSub, ValueType as TypeQLValueType,
            },
            CapabilityBase,
        },
        undefinable::{
            AnnotationCapability, AnnotationType, CapabilityType, Function, Specialise, Struct, Undefinable,
        },
    },
    type_::Label as TypeQLLabel,
};

use crate::{
    definable_resolution::{
        filter_variants, resolve_attribute_type, resolve_object_type, resolve_owns, resolve_owns_declared,
        resolve_plays_declared, resolve_relates, resolve_relates_declared, resolve_relation_type, resolve_role_type,
        resolve_struct_definition_key, resolve_typeql_type, resolve_value_type, try_resolve_typeql_type,
        type_ref_to_label_and_ordering, type_to_object_type, SymbolResolutionError,
    },
    definable_status::{
        get_owns_status, get_plays_status, get_relates_status, get_sub_status, get_value_type_status, DefinableStatus,
    },
    define::DefineError,
};

pub(crate) fn execute(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    undefine: Undefine,
) -> Result<(), UndefineError> {
    process_function_undefinitions(snapshot, type_manager, &undefine.undefinables)?;
    process_specialise_undefinitions(snapshot, type_manager, thing_manager, &undefine.undefinables)?;
    process_capability_annotation_undefinitions(snapshot, type_manager, thing_manager, &undefine.undefinables)?;
    process_type_capability_undefinitions(snapshot, type_manager, thing_manager, &undefine.undefinables)?;
    process_type_annotation_undefinitions(snapshot, type_manager, &undefine.undefinables)?;
    process_type_undefinitions(snapshot, type_manager, thing_manager, &undefine.undefinables)?;
    process_struct_undefinitions(snapshot, type_manager, thing_manager, &undefine.undefinables)?;
    Ok(())
}

fn process_function_undefinitions(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
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
    undefinables: &[Undefinable],
) -> Result<(), UndefineError> {
    filter_variants!(Undefinable::AnnotationType : undefinables)
        .try_for_each(|annotation| undefine_type_annotation(snapshot, type_manager, annotation))?;
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
    let label = Label::parse_from(specialise_undefinable.type_.ident.as_str());
    let relation_type = resolve_relation_type(snapshot, type_manager, &label)
        .map_err(|source| UndefineError::DefinitionResolution { typedb_source: source })?;

    let (role_label, _) = type_ref_to_label_and_ordering(&label, &specialise_undefinable.capability.related)
        .map_err(|typedb_source| UndefineError::DefinitionResolution { typedb_source })?;
    let relates = resolve_relates_declared(snapshot, type_manager, relation_type, role_label.name.as_str())
        .map_err(|source| UndefineError::DefinitionResolution { typedb_source: source })?;
    let specialised_relates =
        resolve_relates(snapshot, type_manager, relates.relation(), specialise_undefinable.specialised.ident.as_str())
            .map_err(|typedb_source| UndefineError::DefinitionResolution { typedb_source })?;

    let definition_status = get_sub_status(snapshot, type_manager, relates.role(), specialised_relates.role())
        .map_err(|source| UndefineError::UnexpectedConceptRead { source })?;
    match definition_status {
        DefinableStatus::DoesNotExist => Ok(()),
        DefinableStatus::ExistsSame(_) => {
            relates.unset_specialise(snapshot, type_manager, thing_manager).map_err(|source| {
                UndefineError::UnsetRelatesSpecialiseError {
                    label: label.clone(),
                    undefinition: specialise_undefinable.clone(),
                    typedb_source: source,
                }
            })
        }
        DefinableStatus::ExistsDifferent(existing_specialised_role) => {
            Err(UndefineError::RelatesSpecialiseDefinedButDifferent {
                specialise: format!(
                    "{} relates {} as {}",
                    relates.relation().get_label(snapshot, type_manager).unwrap().name.to_string(),
                    relates.role().get_label(snapshot, type_manager).unwrap().name.to_string(),
                    specialised_relates.role().get_label(snapshot, type_manager).unwrap().name.to_string(),
                ),
                existing_specialise: format!(
                    "{} relates {} as {}",
                    relates.relation().get_label(snapshot, type_manager).unwrap().name.to_string(),
                    relates.role().get_label(snapshot, type_manager).unwrap().name.to_string(),
                    existing_specialised_role.get_label(snapshot, type_manager).unwrap().name.to_string(),
                ),
                undefinition: specialise_undefinable.clone(),
            })
        }
    }
}

fn undefine_capability_annotation(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    annotation_undefinable: &AnnotationCapability,
) -> Result<(), UndefineError> {
    let label = Label::parse_from(annotation_undefinable.type_.ident.as_str());
    let annotation_category = translate_annotation_category(annotation_undefinable.annotation_category);

    match &annotation_undefinable.capability {
        CapabilityBase::Sub(_) => unreachable!("Sub cannot have annotations"),
        CapabilityBase::Alias(_) => unreachable!("Alias cannot have annotations"),
        CapabilityBase::Owns(typeql_owns) => {
            let object_type = resolve_object_type(snapshot, type_manager, &label)
                .map_err(|source| UndefineError::DefinitionResolution { typedb_source: source })?;
            let (attr_label, ordering) = type_ref_to_label_and_ordering(&label, &typeql_owns.owned)
                .map_err(|typedb_source| UndefineError::DefinitionResolution { typedb_source })?;
            let attribute_type = resolve_attribute_type(snapshot, type_manager, &attr_label)
                .map_err(|typedb_source| UndefineError::DefinitionResolution { typedb_source })?;
            let owns = resolve_owns_declared(snapshot, type_manager, object_type, attribute_type)
                .map_err(|source| UndefineError::DefinitionResolution { typedb_source: source })?;

            owns.unset_annotation(snapshot, type_manager, thing_manager, annotation_category)
        }
        CapabilityBase::Plays(typeql_plays) => {
            let object_type = resolve_object_type(snapshot, type_manager, &label)
                .map_err(|source| UndefineError::DefinitionResolution { typedb_source: source })?;
            let role_label =
                Label::build_scoped(typeql_plays.role.name.ident.as_str(), typeql_plays.role.scope.ident.as_str());
            let role_type = resolve_role_type(snapshot, type_manager, &role_label)
                .map_err(|typedb_source| UndefineError::DefinitionResolution { typedb_source })?;
            let plays = resolve_plays_declared(snapshot, type_manager, object_type, role_type)
                .map_err(|source| UndefineError::DefinitionResolution { typedb_source: source })?;

            plays.unset_annotation(snapshot, type_manager, thing_manager, annotation_category)
        }
        CapabilityBase::Relates(typeql_relates) => {
            let relation_type = resolve_relation_type(snapshot, type_manager, &label)
                .map_err(|source| UndefineError::DefinitionResolution { typedb_source: source })?;
            let (role_label, ordering) = type_ref_to_label_and_ordering(&label, &typeql_relates.related)
                .map_err(|typedb_source| UndefineError::DefinitionResolution { typedb_source })?;
            let relates = resolve_relates_declared(snapshot, type_manager, relation_type, role_label.name.as_str())
                .map_err(|source| UndefineError::DefinitionResolution { typedb_source: source })?;

            relates.unset_annotation(snapshot, type_manager, thing_manager, annotation_category)
        }
        CapabilityBase::ValueType(_) => {
            if !AttributeTypeAnnotation::is_value_type_annotation_category(annotation_category) {
                return Err(UndefineError::IllegalAnnotation {
                    source: AnnotationError::UnsupportedAnnotationForValueType(annotation_category),
                });
            }

            let attribute_type = resolve_attribute_type(snapshot, type_manager, &label)
                .map_err(|source| UndefineError::DefinitionResolution { typedb_source: source })?;

            attribute_type.unset_annotation(snapshot, type_manager, annotation_category)
        }
    }
    .map_err(|err| UndefineError::UnsetCapabilityAnnotationError {
        annotation_category,
        undefinition: annotation_undefinable.clone(),
        typedb_source: err,
    })
}

fn undefine_type_capability(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    capability_undefinable: &CapabilityType,
) -> Result<(), UndefineError> {
    let label = Label::parse_from(capability_undefinable.type_.ident.as_str());
    let type_ = resolve_typeql_type(snapshot, type_manager, &label)
        .map_err(|source| UndefineError::DefinitionResolution { typedb_source: source })?;

    match &capability_undefinable.capability {
        CapabilityBase::Sub(sub) => undefine_type_capability_sub(
            snapshot,
            type_manager,
            thing_manager,
            type_,
            &label,
            sub,
            capability_undefinable,
        ),
        CapabilityBase::Alias(alias) => undefine_type_capability_alias(
            snapshot,
            type_manager,
            thing_manager,
            type_,
            &label,
            alias,
            capability_undefinable,
        ),
        CapabilityBase::Owns(owns) => undefine_type_capability_owns(
            snapshot,
            type_manager,
            thing_manager,
            type_,
            &label,
            owns,
            capability_undefinable,
        ),
        CapabilityBase::Plays(plays) => undefine_type_capability_plays(
            snapshot,
            type_manager,
            thing_manager,
            type_,
            &label,
            plays,
            capability_undefinable,
        ),
        CapabilityBase::Relates(relates) => undefine_type_capability_relates(
            snapshot,
            type_manager,
            thing_manager,
            type_,
            &label,
            relates,
            capability_undefinable,
        ),
        CapabilityBase::ValueType(value_type) => undefine_type_capability_value_type(
            snapshot,
            type_manager,
            thing_manager,
            type_,
            &label,
            value_type,
            capability_undefinable,
        ),
    }
}

fn undefine_type_capability_sub(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    type_: TypeEnum,
    type_label: &Label<'static>,
    sub: &TypeQLSub,
    capability_undefinable: &CapabilityType,
) -> Result<(), UndefineError> {
    let supertype_label = Label::parse_from(sub.supertype_label.ident.as_str());
    let supertype = resolve_typeql_type(snapshot, type_manager, &supertype_label)
        .map_err(|typedb_source| UndefineError::DefinitionResolution { typedb_source })?;

    match (&type_, supertype) {
        (TypeEnum::Entity(type_), TypeEnum::Entity(supertype)) => {
            let need_undefine = check_can_and_need_undefine_sub(
                snapshot,
                type_manager,
                type_label,
                type_.clone(),
                supertype.clone(),
                capability_undefinable,
            )?;
            if need_undefine {
                type_
                    .unset_supertype(snapshot, type_manager, thing_manager)
                    .map_err(|source| UndefineError::UnsetSupertypeError { sub: sub.clone(), typedb_source: source })?;
            }
        }
        (TypeEnum::Relation(type_), TypeEnum::Relation(supertype)) => {
            let need_undefine = check_can_and_need_undefine_sub(
                snapshot,
                type_manager,
                type_label,
                type_.clone(),
                supertype.clone(),
                capability_undefinable,
            )?;
            if need_undefine {
                type_
                    .unset_supertype(snapshot, type_manager, thing_manager)
                    .map_err(|source| UndefineError::UnsetSupertypeError { sub: sub.clone(), typedb_source: source })?;
            }
        }
        (TypeEnum::Attribute(type_), TypeEnum::Attribute(supertype)) => {
            let need_undefine = check_can_and_need_undefine_sub(
                snapshot,
                type_manager,
                type_label,
                type_.clone(),
                supertype.clone(),
                capability_undefinable,
            )?;
            if need_undefine {
                type_
                    .unset_supertype(snapshot, type_manager, thing_manager)
                    .map_err(|source| UndefineError::UnsetSupertypeError { sub: sub.clone(), typedb_source: source })?;
            }
        }
        (TypeEnum::RoleType(_), TypeEnum::RoleType(_)) => unreachable!("RoleType's sub is controlled by specialise"), // Turn into an error
        (type_, supertype) => {
            return Err(err_capability_kind_mismatch(
                type_label,
                &supertype_label,
                capability_undefinable,
                type_.kind(),
                supertype.kind(),
            ))
        }
    }

    Ok(())
}

fn undefine_type_capability_alias(
    _snapshot: &mut impl WritableSnapshot,
    _type_manager: &TypeManager,
    _thing_manager: &ThingManager,
    _type_: TypeEnum,
    _type_label: &Label<'static>,
    _alias: &TypeQLAlias,
    _capability_undefinable: &CapabilityType,
) -> Result<(), UndefineError> {
    Err(UndefineError::Unimplemented { description: "Alias undefinition.".to_string() })
}

fn undefine_type_capability_owns(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    type_: TypeEnum,
    type_label: &Label<'static>,
    owns: &TypeQLOwns,
    capability_undefinable: &CapabilityType,
) -> Result<(), UndefineError> {
    let (attr_label, ordering) = type_ref_to_label_and_ordering(&type_label, &owns.owned)
        .map_err(|typedb_source| UndefineError::DefinitionResolution { typedb_source })?;
    let attribute_type = resolve_attribute_type(snapshot, type_manager, &attr_label)
        .map_err(|typedb_source| UndefineError::DefinitionResolution { typedb_source })?;

    let object_type = type_to_object_type(&type_)
        .map_err(|_| err_unsupported_capability(type_label, type_.kind(), capability_undefinable))?;

    let definition_status =
        get_owns_status(snapshot, type_manager, object_type.clone(), attribute_type.clone(), ordering)
            .map_err(|source| UndefineError::UnexpectedConceptRead { source })?;
    match definition_status {
        DefinableStatus::DoesNotExist => Ok(()),
        DefinableStatus::ExistsSame(_) => object_type
            .unset_owns(snapshot, type_manager, thing_manager, attribute_type)
            .map_err(|source| UndefineError::UnsetOwnsError { owns: owns.clone(), typedb_source: source }),
        DefinableStatus::ExistsDifferent((existing_owns, existing_ordering)) => {
            Err(UndefineError::OwnsDefinedButDifferent {
                existing_owns: format!(
                    "{} owns {}",
                    existing_owns.owner().get_label(snapshot, type_manager).unwrap().name.to_string(),
                    existing_owns.attribute().get_label(snapshot, type_manager).unwrap().name.to_string(),
                ),
                existing_ordering,
                ordering,
                undefinition: capability_undefinable.clone(),
            })
        }
    }
}

fn undefine_type_capability_plays(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    type_: TypeEnum,
    type_label: &Label<'static>,
    plays: &TypeQLPlays,
    capability_undefinable: &CapabilityType,
) -> Result<(), UndefineError> {
    let role_label = Label::build_scoped(plays.role.name.ident.as_str(), plays.role.scope.ident.as_str());
    let role_type = resolve_role_type(snapshot, type_manager, &role_label)
        .map_err(|typedb_source| UndefineError::DefinitionResolution { typedb_source })?;

    let object_type = type_to_object_type(&type_)
        .map_err(|_| err_unsupported_capability(&type_label, type_.kind(), capability_undefinable))?;

    let definition_status = get_plays_status(snapshot, type_manager, object_type.clone(), role_type.clone())
        .map_err(|source| UndefineError::UnexpectedConceptRead { source })?;
    match definition_status {
        DefinableStatus::DoesNotExist => Ok(()),
        DefinableStatus::ExistsSame(_) => object_type
            .unset_plays(snapshot, type_manager, thing_manager, role_type)
            .map_err(|source| UndefineError::UnsetPlaysError { plays: plays.clone(), typedb_source: source }),
        DefinableStatus::ExistsDifferent(_) => unreachable!("Plays cannot differ"),
    }
}

fn undefine_type_capability_relates(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    type_: TypeEnum,
    type_label: &Label<'static>,
    relates: &TypeQLRelates,
    capability_undefinable: &CapabilityType,
) -> Result<(), UndefineError> {
    let (role_label, ordering) = type_ref_to_label_and_ordering(&type_label, &relates.related)
        .map_err(|typedb_source| UndefineError::DefinitionResolution { typedb_source })?;

    let TypeEnum::Relation(relation_type) = &type_ else {
        return Err(err_unsupported_capability(&type_label, type_.kind(), capability_undefinable));
    };

    let definition_status = get_relates_status(snapshot, type_manager, relation_type.clone(), &role_label, ordering)
        .map_err(|source| UndefineError::UnexpectedConceptRead { source })?;
    match definition_status {
        DefinableStatus::DoesNotExist => Ok(()),
        DefinableStatus::ExistsSame(None) => unreachable!("Expected existing relates definition"),
        DefinableStatus::ExistsSame(Some((existing_relates, _))) => existing_relates
            .role()
            .delete(snapshot, type_manager, thing_manager)
            .map_err(|source| UndefineError::DeleteRoleTypeError { relates: relates.clone(), typedb_source: source }),
        DefinableStatus::ExistsDifferent((existing_relates, existing_ordering)) => {
            Err(UndefineError::RelatesDefinedButDifferent {
                existing_relates: format!(
                    "{} relates {}",
                    existing_relates.relation().get_label(snapshot, type_manager).unwrap().name.to_string(),
                    existing_relates.role().get_label(snapshot, type_manager).unwrap().scoped_name.to_string(),
                ),
                existing_ordering,
                ordering,
                undefinition: capability_undefinable.clone(),
            })
        }
    }
}

fn undefine_type_capability_value_type(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    type_: TypeEnum,
    type_label: &Label<'static>,
    value_type: &TypeQLValueType,
    capability_undefinable: &CapabilityType,
) -> Result<(), UndefineError> {
    let TypeEnum::Attribute(attribute_type) = &type_ else {
        return Err(err_unsupported_capability(&type_label, type_.kind(), capability_undefinable));
    };
    let value_type = resolve_value_type(snapshot, type_manager, &value_type.value_type)
        .map_err(|typedb_source| UndefineError::ValueTypeSymbolResolution { typedb_source })?;

    let definition_status = get_value_type_status(snapshot, type_manager, attribute_type.clone(), value_type.clone())
        .map_err(|source| UndefineError::UnexpectedConceptRead { source })?;
    match definition_status {
        DefinableStatus::DoesNotExist => Ok(()),
        DefinableStatus::ExistsSame(_) => {
            attribute_type.unset_value_type(snapshot, type_manager, thing_manager).map_err(|source| {
                UndefineError::UnsetValueTypeError { label: type_label.to_owned(), value_type, typedb_source: source }
            })
        }
        DefinableStatus::ExistsDifferent(existing_value_type) => {
            Err(UndefineError::AttributeTypeValueTypeDefinedButDifferent {
                type_: type_label.clone().into_owned(),
                value_type,
                existing_value_type,
                undefinition: capability_undefinable.clone(),
            })
        }
    }
}

fn undefine_type_annotation(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    annotation_undefinable: &AnnotationType,
) -> Result<(), UndefineError> {
    let label = Label::parse_from(annotation_undefinable.type_.ident.as_str());
    let type_ = resolve_typeql_type(snapshot, type_manager, &label)
        .map_err(|source| UndefineError::DefinitionResolution { typedb_source: source })?;
    let annotation_category = translate_annotation_category(annotation_undefinable.annotation_category);

    match type_ {
        TypeEnum::Entity(entity_type) => entity_type.unset_annotation(snapshot, type_manager, annotation_category),
        TypeEnum::Relation(relation_type) => {
            relation_type.unset_annotation(snapshot, type_manager, annotation_category)
        }
        TypeEnum::Attribute(attribute_type) => {
            if AttributeTypeAnnotation::is_value_type_annotation_category(annotation_category) {
                return Err(UndefineError::IllegalAnnotation {
                    source: AnnotationError::UnsupportedAnnotationForAttributeType(annotation_category),
                });
            }
            attribute_type.unset_annotation(snapshot, type_manager, annotation_category)
        }
        TypeEnum::RoleType(_) => unreachable!("Role annotations are syntactically on relates"),
    }
    .map_err(|err| UndefineError::UnsetTypeAnnotationError {
        typedb_source: err,
        label,
        annotation_category,
        undefinition: annotation_undefinable.clone(),
    })
}

fn undefine_type(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    label_undefinable: &TypeQLLabel,
) -> Result<(), UndefineError> {
    let label = Label::parse_from(label_undefinable.ident.as_str());
    let type_ = resolve_typeql_type(snapshot, type_manager, &label)
        .map_err(|source| UndefineError::DefinitionResolution { typedb_source: source })?; // TODO: Ignore and return OK?

    match type_ {
        TypeEnum::Entity(entity_type) => entity_type.delete(snapshot, type_manager, thing_manager),
        TypeEnum::Relation(relation_type) => relation_type.delete(snapshot, type_manager, thing_manager),
        TypeEnum::Attribute(attribute_type) => attribute_type.delete(snapshot, type_manager, thing_manager),
        TypeEnum::RoleType(_) => unreachable!("Role undefinition is processed through relates undefinition"),
    }
    .map_err(|err| UndefineError::TypeDeleteError {
        typedb_source: err,
        label,
        undefinition: label_undefinable.clone(),
    })
}

fn undefine_struct(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    struct_undefinable: &Struct,
) -> Result<(), UndefineError> {
    let name = struct_undefinable.ident.as_str();
    let struct_key = resolve_struct_definition_key(snapshot, type_manager, name)
        .map_err(|source| UndefineError::DefinitionResolution { typedb_source: source })?; // TODO: Ignore and return OK?

    type_manager.delete_struct(snapshot, thing_manager, &struct_key).map_err(|err| UndefineError::StructDeleteError {
        typedb_source: err,
        struct_name: name.to_owned(),
        undefinition: struct_undefinable.clone(),
    })
}

fn check_can_and_need_undefine_sub<'a, T: TypeAPI<'a>>(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    label: &Label<'a>,
    type_: T,
    supertype: T,
    undefinition: &CapabilityType,
) -> Result<bool, UndefineError> {
    let definition_status = get_sub_status(snapshot, type_manager, type_, supertype.clone())
        .map_err(|source| UndefineError::UnexpectedConceptRead { source })?;
    match definition_status {
        DefinableStatus::DoesNotExist => Ok(false),
        DefinableStatus::ExistsSame(_) => Ok(true),
        DefinableStatus::ExistsDifferent(existing) => Err(UndefineError::TypeSubDefinedButDifferent {
            type_: label.clone().into_owned(),
            supertype: supertype
                .get_label(snapshot, type_manager)
                .map_err(|source| UndefineError::UnexpectedConceptRead { source })?
                .clone()
                .into_owned(),
            existing_supertype: existing
                .get_label(snapshot, type_manager)
                .map_err(|source| UndefineError::UnexpectedConceptRead { source })?
                .clone()
                .into_owned(),
            undefinition: undefinition.clone(),
        }),
    }
}

fn err_capability_kind_mismatch(
    left: &Label<'static>,
    right: &Label<'static>,
    capability: &CapabilityType,
    left_kind: Kind,
    right_kind: Kind,
) -> UndefineError {
    UndefineError::CapabilityKindMismatch {
        left: left.clone().into_owned(),
        right: right.clone().into_owned(),
        undefinition: capability.clone(),
        left_kind,
        right_kind,
    }
}

fn err_unsupported_capability(label: &Label<'static>, kind: Kind, capability: &CapabilityType) -> UndefineError {
    UndefineError::TypeCannotHaveCapability { label: label.to_owned(), kind, capability: capability.clone() }
}

typedb_error!(
    pub UndefineError(component = "Undefine execution", prefix = "UEX") {
        Unimplemented(1, "Unimplemented undefine functionality: {description}", description: String),
        UnexpectedConceptRead(2, "Concept read error during undefine query execution.", ( source: ConceptReadError )),
        DefinitionResolution(3, "Could not find symbol in undefine query.", ( typedb_source: SymbolResolutionError )),
        LiteralParseError(4, "Error parsing literal in undefine query.", ( source : LiteralParseError )),
        StructDoesNotExist(5, "Struct used in undefine query does not exist.\nSource:\n{undefinition}", undefinition: Struct),
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
            label: Label<'static>,
            undefinition: TypeQLLabel,
            ( typedb_source: ConceptWriteError )
        ),
        ValueTypeSymbolResolution(
            8,
            "Error resolving value type in undefine query.",
            ( typedb_source: SymbolResolutionError )
        ),
        UnsetSupertypeError(
            9,
            "Undefining supertype failed.\nSource: {sub}",
            sub: TypeQLSub,
            ( typedb_source: ConceptWriteError )
        ),
        UnsetOwnsError(
            10,
            "Undefining owns failed.\nSource: {owns}",
            owns: TypeQLOwns,
            ( typedb_source: ConceptWriteError )
        ),
        DeleteRoleTypeError(
            11,
            "Undefining relates (role type) failed.\nSource: {relates}",
            relates: TypeQLRelates,
            ( typedb_source: ConceptWriteError )
        ),
        UnsetPlaysError(
            12,
            "Undefining plays failed.\nSource: {plays}",
            plays: TypeQLPlays,
            ( typedb_source: ConceptWriteError )
        ),
        UnsetValueTypeError(
            13,
            "Undefining '{label}' to have value type '{value_type}' failed.",
            label: Label<'static>,
            value_type: ValueType,
            ( typedb_source: ConceptWriteError )
        ),
        UnsetTypeAnnotationError(
            14,
            "Undefining '{label}' to have annotation '{annotation_category}' failed.\nSource:\n{undefinition}",
            label: Label<'static>,
            annotation_category: AnnotationCategory,
            undefinition: AnnotationType,
            ( typedb_source: ConceptWriteError )
        ),
        UnsetCapabilityAnnotationError(
            15,
            "Undefining '{annotation_category}' failed.\nSource:\n{undefinition}",
            annotation_category: AnnotationCategory,
            undefinition: AnnotationCapability,
            ( typedb_source: ConceptWriteError )
        ),
        UnsetRelatesSpecialiseError(
            16,
            "For relation type '{label}', undefining `relates as' failed.\nSource:\n{undefinition}",
            label: Label<'static>,
            undefinition: Specialise,
            ( typedb_source: ConceptWriteError )
        ),
        TypeCannotHaveCapability(
            17,
            "Invalid undefine - the type '{label}' of kind '{kind}', which cannot have: '{capability}'. Maybe you wanted to undefine something else?",
            label: Label<'static>,
            kind: Kind,
            capability: CapabilityType
        ),
        TypeSubDefinedButDifferent(
            18,
            "Undefining failed because `{type_} sub {supertype}` is not defined, while {type_}'s defined supertype is {existing_supertype}. Maybe you meant `{type_} sub {existing_supertype}`?\nSource:\n{undefinition}",
            type_: Label<'static>,
            supertype: Label<'static>,
            existing_supertype: Label<'static>,
            undefinition: CapabilityType
        ),
        OwnsDefinedButDifferent(
            19,
            "Undefining failed because `{existing_owns}{ordering}` is defined with a different ordering. Maybe you meant `{existing_owns}{existing_ordering}`?\nSource:\n{undefinition}",
            existing_owns: String,
            existing_ordering: Ordering,
            ordering: Ordering,
            undefinition: CapabilityType
        ),
        RelatesDefinedButDifferent(
            20,
            "Undefining failed because `{existing_relates}{ordering}` is defined with a different ordering. Maybe you meant `{existing_relates}{existing_ordering}`?\nSource:\n{undefinition}",
            existing_relates: String,
            existing_ordering: Ordering,
            ordering: Ordering,
            undefinition: CapabilityType
        ),
        AttributeTypeValueTypeDefinedButDifferent(
            21,
            "Undefining failed because `{type_} value {value_type}` is not defined, while {type_}'s defined value type is {existing_value_type}. Maybe you meant `{type_} value {existing_value_type}`?\nSource:\n{undefinition}",
            type_: Label<'static>,
            value_type: ValueType,
            existing_value_type: ValueType,
            undefinition: CapabilityType
        ),
        RelatesSpecialiseDefinedButDifferent(
            22,
            "Undefining failed because specialisation `{specialise}` is defined for another specialised role type. Maybe you meant `{existing_specialise}`?\nSource:\n{undefinition}",
            specialise: String,
            existing_specialise: String,
            undefinition: Specialise
        ),
        IllegalAnnotation(
            23,
            "Illegal annotation",
            ( source: AnnotationError )
        ),
        CapabilityKindMismatch(
            28,
            "Undefining failed because the left type '{left}' is of kind '{left_kind}' isn't the same kind as the right type '{right}' which has kind '{right_kind}'. Maybe you wanted to undefine something else?\nSource:\n{undefinition}",
            left: Label<'static>,
            right: Label<'static>,
            undefinition: CapabilityType,
            left_kind: Kind,
            right_kind: Kind
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
