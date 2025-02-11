/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use answer::Type as TypeEnum;
use concept::{
    error::{ConceptReadError, ConceptWriteError},
    thing::thing_manager::ThingManager,
    type_::{
        annotation::{AnnotationCategory, AnnotationError},
        attribute_type::AttributeTypeAnnotation,
        type_manager::TypeManager,
        Capability, KindAPI, Ordering, OwnerAPI, PlayerAPI, TypeAPI,
    },
};
use encoding::{
    graph::type_::Kind,
    value::{label::Label, value_type::ValueType},
};
use error::typedb_error;
use function::{function_manager::FunctionManager, FunctionError};
use ir::{translation::tokens::translate_annotation_category, LiteralParseError};
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};
use typeql::{
    common::{error::TypeQLError, token::Keyword, Span, Spanned},
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
        filter_variants, resolve_attribute_type, resolve_object_type, resolve_owns_declared, resolve_plays_declared,
        resolve_relates, resolve_relates_declared, resolve_relation_type, resolve_role_type,
        resolve_struct_definition_key, resolve_typeql_type, resolve_value_type, type_ref_to_label_and_ordering,
        type_to_object_type, SymbolResolutionError,
    },
    definable_status::{
        get_capability_annotation_category_status, get_owns_status, get_plays_status, get_relates_status,
        get_sub_status, get_type_annotation_category_status, get_value_type_status, DefinableStatus,
        DefinableStatusMode,
    },
};

fn checked_identifier(identifier: &typeql::Identifier) -> Result<&str, UndefineError> {
    identifier.as_str_unreserved().map_err(|_source| {
        let TypeQLError::ReservedKeywordAsIdentifier { identifier } = _source else { unreachable!() };
        UndefineError::IllegalKeywordAsIdentifier { source_span: identifier.span(), identifier }
    })
}

pub(crate) fn execute(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    function_manager: &FunctionManager,
    undefine: Undefine,
) -> Result<(), UndefineError> {
    process_function_undefinitions(snapshot, function_manager, &undefine.undefinables)?;
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
    function_manager: &FunctionManager,
    undefinables: &[Undefinable],
) -> Result<(), UndefineError> {
    filter_variants!(Undefinable::Function : undefinables)
        .try_for_each(|function| undefine_function(snapshot, function_manager, function))?;
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
    snapshot: &mut impl WritableSnapshot,
    function_manager: &FunctionManager,
    function_undefinable: &Function,
) -> Result<(), UndefineError> {
    let name = checked_identifier(&function_undefinable.ident)?;
    function_manager.undefine_function(snapshot, name).map_err(|source| UndefineError::FunctionUndefinition {
        name: name.to_owned(),
        source_span: function_undefinable.span(),
        typedb_source: Box::new(source),
    })
}

fn undefine_specialise(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    specialise_undefinable: &Specialise,
) -> Result<(), UndefineError> {
    let label = Label::parse_from(
        checked_identifier(&specialise_undefinable.type_.ident)?,
        specialise_undefinable.type_.span(),
    );
    let relation_type = resolve_relation_type(snapshot, type_manager, &label)
        .map_err(|source| UndefineError::DefinitionResolution { typedb_source: source })?;

    let (role_label, _) = type_ref_to_label_and_ordering(&label, &specialise_undefinable.capability.related)
        .map_err(|typedb_source| UndefineError::DefinitionResolution { typedb_source })?;
    let relates = resolve_relates_declared(snapshot, type_manager, relation_type, role_label.name.as_str())
        .map_err(|source| UndefineError::DefinitionResolution { typedb_source: source })?;
    let specialised_relates = resolve_relates(
        snapshot,
        type_manager,
        relates.relation(),
        checked_identifier(&specialise_undefinable.specialised.ident)?,
    )
    .map_err(|typedb_source| UndefineError::DefinitionResolution { typedb_source })?;

    let definition_status = get_sub_status(snapshot, type_manager, relates.role(), specialised_relates.role())
        .map_err(|source| UndefineError::UnexpectedConceptRead { typedb_source: source })?;
    match definition_status {
        DefinableStatus::ExistsSame(_) => {
            relates.unset_specialise(snapshot, type_manager, thing_manager).map_err(|source| {
                UndefineError::UnsetRelatesSpecialiseError {
                    type_: label.clone(),
                    source_span: specialise_undefinable.span(),
                    typedb_source: source,
                }
            })
        }
        DefinableStatus::DoesNotExist => Err(UndefineError::RelatesSpecialiseNotDefined {
            type_: label,
            relates_key: Keyword::Relates,
            as_key: Keyword::As,
            specialising_role_name: relates
                .role()
                .get_label(snapshot, type_manager)
                .map_err(|source| UndefineError::UnexpectedConceptRead { typedb_source: source })?
                .name()
                .to_string(),
            specialised_role_name: specialised_relates
                .role()
                .get_label(snapshot, type_manager)
                .map_err(|source| UndefineError::UnexpectedConceptRead { typedb_source: source })?
                .name()
                .to_string(),
            source_span: specialise_undefinable.span(),
        }),
        DefinableStatus::ExistsDifferent(existing_specialised_role) => {
            Err(UndefineError::RelatesSpecialiseDefinedButDifferent {
                type_: label,
                relates_key: Keyword::Relates,
                as_key: Keyword::As,
                specialising_role_name: relates
                    .role()
                    .get_label(snapshot, type_manager)
                    .map_err(|source| UndefineError::UnexpectedConceptRead { typedb_source: source })?
                    .name()
                    .to_string(),
                specialised_role_name: specialised_relates
                    .role()
                    .get_label(snapshot, type_manager)
                    .map_err(|source| UndefineError::UnexpectedConceptRead { typedb_source: source })?
                    .name()
                    .to_string(),
                existing_specialised_role_name: existing_specialised_role
                    .get_label(snapshot, type_manager)
                    .map_err(|source| UndefineError::UnexpectedConceptRead { typedb_source: source })?
                    .name()
                    .to_string(),
                source_span: specialise_undefinable.span(),
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
    let label = Label::parse_from(
        checked_identifier(&annotation_undefinable.type_.ident)?,
        annotation_undefinable.type_.span(),
    );
    let annotation_category = translate_annotation_category(annotation_undefinable.annotation_category)
        .map_err(|typedb_source| UndefineError::LiteralParseError { typedb_source })?;

    match &annotation_undefinable.capability {
        CapabilityBase::Sub(_) => {
            return Err(UndefineError::IllegalAnnotation {
                typedb_source: AnnotationError::UnsupportedAnnotationForSub { category: annotation_category },
            })
        }
        CapabilityBase::Alias(_) => {
            return Err(UndefineError::IllegalAnnotation {
                typedb_source: AnnotationError::UnsupportedAnnotationForAlias { category: annotation_category },
            })
        }
        CapabilityBase::Owns(typeql_owns) => {
            let object_type = resolve_object_type(snapshot, type_manager, &label)
                .map_err(|source| UndefineError::DefinitionResolution { typedb_source: source })?;
            let (attr_label, ordering) = type_ref_to_label_and_ordering(&label, &typeql_owns.owned)
                .map_err(|typedb_source| UndefineError::DefinitionResolution { typedb_source })?;
            let attribute_type = resolve_attribute_type(snapshot, type_manager, &attr_label)
                .map_err(|typedb_source| UndefineError::DefinitionResolution { typedb_source })?;
            let owns = resolve_owns_declared(snapshot, type_manager, object_type, attribute_type)
                .map_err(|source| UndefineError::DefinitionResolution { typedb_source: source })?;

            let owns_definition_status = get_owns_status(
                snapshot,
                type_manager,
                object_type,
                attribute_type,
                ordering,
                DefinableStatusMode::Transitive,
            )
            .map_err(|source| UndefineError::UnexpectedConceptRead { typedb_source: source })?;
            match owns_definition_status {
                DefinableStatus::DoesNotExist | DefinableStatus::ExistsSame(_) => {}
                DefinableStatus::ExistsDifferent((_, existing_ordering)) => {
                    return Err(UndefineError::OwnsOfAnnotationDefinedButDifferent {
                        type_: label.clone(),
                        key: Keyword::Owns,
                        annotation: annotation_category,
                        attribute: attr_label,
                        ordering,
                        existing_ordering,
                        source_span: annotation_undefinable.span(),
                    })
                }
            }

            check_can_and_need_undefine_capability_annotation(
                snapshot,
                type_manager,
                owns,
                annotation_category,
                annotation_undefinable,
            )?;

            owns.unset_annotation(snapshot, type_manager, thing_manager, annotation_category)
        }
        CapabilityBase::Plays(typeql_plays) => {
            let object_type = resolve_object_type(snapshot, type_manager, &label)
                .map_err(|source| UndefineError::DefinitionResolution { typedb_source: source })?;
            let role_label = Label::build_scoped(
                checked_identifier(&typeql_plays.role.name.ident)?,
                checked_identifier(&typeql_plays.role.scope.ident)?,
                typeql_plays.role.span(),
            );
            let role_type = resolve_role_type(snapshot, type_manager, &role_label)
                .map_err(|typedb_source| UndefineError::DefinitionResolution { typedb_source })?;
            let plays = resolve_plays_declared(snapshot, type_manager, object_type, role_type)
                .map_err(|source| UndefineError::DefinitionResolution { typedb_source: source })?;

            check_can_and_need_undefine_capability_annotation(
                snapshot,
                type_manager,
                plays,
                annotation_category,
                annotation_undefinable,
            )?;

            plays.unset_annotation(snapshot, type_manager, thing_manager, annotation_category)
        }
        CapabilityBase::Relates(typeql_relates) => {
            let relation_type = resolve_relation_type(snapshot, type_manager, &label)
                .map_err(|source| UndefineError::DefinitionResolution { typedb_source: source })?;
            let (role_label, ordering) = type_ref_to_label_and_ordering(&label, &typeql_relates.related)
                .map_err(|typedb_source| UndefineError::DefinitionResolution { typedb_source })?;
            let relates = resolve_relates_declared(snapshot, type_manager, relation_type, role_label.name.as_str())
                .map_err(|source| UndefineError::DefinitionResolution { typedb_source: source })?;

            let relates_definition_status = get_relates_status(
                snapshot,
                type_manager,
                relation_type,
                &role_label,
                ordering,
                DefinableStatusMode::Transitive,
            )
            .map_err(|source| UndefineError::UnexpectedConceptRead { typedb_source: source })?;
            match relates_definition_status {
                DefinableStatus::DoesNotExist | DefinableStatus::ExistsSame(_) => {}
                DefinableStatus::ExistsDifferent((_, existing_ordering)) => {
                    return Err(UndefineError::RelatesOfAnnotationDefinedButDifferent {
                        type_: label.clone(),
                        key: Keyword::Relates,
                        annotation: annotation_category,
                        role: role_label,
                        ordering,
                        existing_ordering,
                        source_span: annotation_undefinable.span(),
                    })
                }
            }

            check_can_and_need_undefine_capability_annotation(
                snapshot,
                type_manager,
                relates,
                annotation_category,
                annotation_undefinable,
            )?;

            relates.unset_annotation(snapshot, type_manager, thing_manager, annotation_category)
        }
        CapabilityBase::ValueType(_) => {
            if !AttributeTypeAnnotation::is_value_type_annotation_category(annotation_category) {
                return Err(UndefineError::IllegalAnnotation {
                    typedb_source: AnnotationError::UnsupportedAnnotationForValueType { category: annotation_category },
                });
            }

            let attribute_type = resolve_attribute_type(snapshot, type_manager, &label)
                .map_err(|source| UndefineError::DefinitionResolution { typedb_source: source })?;
            let definition_status =
                get_type_annotation_category_status(snapshot, type_manager, attribute_type, annotation_category)
                    .map_err(|source| UndefineError::UnexpectedConceptRead { typedb_source: source })?;
            match definition_status {
                DefinableStatus::ExistsSame(_) => {
                    attribute_type.unset_annotation(snapshot, type_manager, annotation_category)
                }
                DefinableStatus::ExistsDifferent(_) => unreachable!("Annotation categories cannot differ"),
                DefinableStatus::DoesNotExist => {
                    return Err(UndefineError::CapabilityAnnotationNotDefined {
                        annotation: annotation_category,
                        source_span: annotation_undefinable.span(),
                    })
                }
            }
        }
    }
    .map_err(|err| UndefineError::UnsetCapabilityAnnotationError {
        annotation_category,
        source_span: annotation_undefinable.span(),
        typedb_source: err,
    })
}

fn undefine_type_capability(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    capability_undefinable: &CapabilityType,
) -> Result<(), UndefineError> {
    let label = Label::parse_from(
        checked_identifier(&capability_undefinable.type_.ident)?,
        capability_undefinable.type_.span(),
    );
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
    type_label: &Label,
    sub: &TypeQLSub,
    capability_undefinable: &CapabilityType,
) -> Result<(), UndefineError> {
    let supertype_label =
        Label::parse_from(checked_identifier(&sub.supertype_label.ident)?, sub.supertype_label.span());
    let supertype = resolve_typeql_type(snapshot, type_manager, &supertype_label)
        .map_err(|typedb_source| UndefineError::DefinitionResolution { typedb_source })?;

    match (&type_, supertype) {
        (TypeEnum::Entity(type_), TypeEnum::Entity(supertype)) => {
            let need_undefine = check_can_and_need_undefine_sub(
                snapshot,
                type_manager,
                type_label,
                *type_,
                supertype,
                capability_undefinable,
            )?;
            if need_undefine {
                type_.unset_supertype(snapshot, type_manager, thing_manager).map_err(|source| {
                    UndefineError::UnsetSupertypeError { source_span: sub.span(), typedb_source: source }
                })?;
            }
        }
        (TypeEnum::Relation(type_), TypeEnum::Relation(supertype)) => {
            let need_undefine = check_can_and_need_undefine_sub(
                snapshot,
                type_manager,
                type_label,
                *type_,
                supertype,
                capability_undefinable,
            )?;
            if need_undefine {
                type_.unset_supertype(snapshot, type_manager, thing_manager).map_err(|source| {
                    UndefineError::UnsetSupertypeError { source_span: sub.span(), typedb_source: source }
                })?;
            }
        }
        (TypeEnum::Attribute(type_), TypeEnum::Attribute(supertype)) => {
            let need_undefine = check_can_and_need_undefine_sub(
                snapshot,
                type_manager,
                type_label,
                *type_,
                supertype,
                capability_undefinable,
            )?;
            if need_undefine {
                type_.unset_supertype(snapshot, type_manager, thing_manager).map_err(|source| {
                    UndefineError::UnsetSupertypeError { source_span: sub.span(), typedb_source: source }
                })?;
            }
        }
        (TypeEnum::RoleType(_), TypeEnum::RoleType(_)) => unreachable!("RoleType's sub is controlled by specialise"),
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
    _type_label: &Label,
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
    type_label: &Label,
    owns: &TypeQLOwns,
    capability_undefinable: &CapabilityType,
) -> Result<(), UndefineError> {
    let (attr_label, ordering) = type_ref_to_label_and_ordering(type_label, &owns.owned)
        .map_err(|typedb_source| UndefineError::DefinitionResolution { typedb_source })?;
    let attribute_type = resolve_attribute_type(snapshot, type_manager, &attr_label)
        .map_err(|typedb_source| UndefineError::DefinitionResolution { typedb_source })?;

    let object_type = type_to_object_type(&type_)
        .map_err(|_| err_unsupported_capability(type_label, type_.kind(), capability_undefinable))?;

    let definition_status =
        get_owns_status(snapshot, type_manager, object_type, attribute_type, ordering, DefinableStatusMode::Transitive)
            .map_err(|source| UndefineError::UnexpectedConceptRead { typedb_source: source })?;
    match definition_status {
        DefinableStatus::ExistsSame(_) => object_type
            .unset_owns(snapshot, type_manager, thing_manager, attribute_type)
            .map_err(|source| UndefineError::UnsetOwnsError { source_span: owns.span(), typedb_source: source }),
        DefinableStatus::DoesNotExist => Err(UndefineError::OwnsNotDefined {
            type_: type_label.clone(),
            key: Keyword::Owns,
            attribute: attr_label,
            ordering,
            source_span: capability_undefinable.span(),
        }),
        DefinableStatus::ExistsDifferent((_, existing_ordering)) => Err(UndefineError::OwnsDefinedButDifferent {
            type_: type_label.clone(),
            key: Keyword::Owns,
            attribute: attr_label,
            ordering,
            existing_ordering,
            source_span: capability_undefinable.span(),
        }),
    }
}

fn undefine_type_capability_plays(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    type_: TypeEnum,
    type_label: &Label,
    plays: &TypeQLPlays,
    capability_undefinable: &CapabilityType,
) -> Result<(), UndefineError> {
    let role_label = Label::build_scoped(
        checked_identifier(&plays.role.name.ident)?,
        checked_identifier(&plays.role.scope.ident)?,
        plays.role.span(),
    );
    let role_type = resolve_role_type(snapshot, type_manager, &role_label)
        .map_err(|typedb_source| UndefineError::DefinitionResolution { typedb_source })?;

    let object_type = type_to_object_type(&type_)
        .map_err(|_| err_unsupported_capability(type_label, type_.kind(), capability_undefinable))?;

    let definition_status =
        get_plays_status(snapshot, type_manager, object_type, role_type, DefinableStatusMode::Transitive)
            .map_err(|source| UndefineError::UnexpectedConceptRead { typedb_source: source })?;
    match definition_status {
        DefinableStatus::ExistsSame(_) => object_type
            .unset_plays(snapshot, type_manager, thing_manager, role_type)
            .map_err(|source| UndefineError::UnsetPlaysError { source_span: plays.span(), typedb_source: source }),
        DefinableStatus::DoesNotExist => Err(UndefineError::PlaysNotDefined {
            type_: type_label.clone(),
            key: Keyword::Plays,
            role: role_label.clone(),
            source_span: capability_undefinable.span(),
        }),
        DefinableStatus::ExistsDifferent(_) => unreachable!("Plays cannot differ"),
    }
}

fn undefine_type_capability_relates(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    type_: TypeEnum,
    type_label: &Label,
    relates: &TypeQLRelates,
    capability_undefinable: &CapabilityType,
) -> Result<(), UndefineError> {
    let (role_label, ordering) = type_ref_to_label_and_ordering(type_label, &relates.related)
        .map_err(|typedb_source| UndefineError::DefinitionResolution { typedb_source })?;

    let TypeEnum::Relation(relation_type) = &type_ else {
        return Err(err_unsupported_capability(type_label, type_.kind(), capability_undefinable));
    };

    // Not Transitive: when deleting owns or plays, we "unset" it, and the TypeManager knows how to
    // validate this operation against a specific owner / player type. When we delete a role type,
    // it does not know if this operation is valid for a relation type. So we need to find the declared
    // role type for the specific relation type here.
    let definition_status = get_relates_status(
        snapshot,
        type_manager,
        *relation_type,
        &role_label,
        ordering,
        DefinableStatusMode::Declared,
    )
    .map_err(|source| UndefineError::UnexpectedConceptRead { typedb_source: source })?;
    match definition_status {
        DefinableStatus::ExistsSame(None) => unreachable!("Expected existing relates definition"),
        DefinableStatus::ExistsSame(Some((existing_relates, _))) => {
            existing_relates.role().delete(snapshot, type_manager, thing_manager).map_err(|source| {
                UndefineError::DeleteRoleTypeError { source_span: relates.span(), typedb_source: source }
            })
        }
        DefinableStatus::DoesNotExist => Err(UndefineError::RelatesNotDefined {
            type_: type_label.clone(),
            key: Keyword::Relates,
            role: role_label.clone(),
            ordering,
            source_span: capability_undefinable.span(),
        }),
        DefinableStatus::ExistsDifferent((_, existing_ordering)) => Err(UndefineError::RelatesDefinedButDifferent {
            type_: type_label.clone(),
            key: Keyword::Relates,
            role: role_label.clone(),
            ordering,
            existing_ordering,
            source_span: capability_undefinable.span(),
        }),
    }
}

fn undefine_type_capability_value_type(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    type_: TypeEnum,
    type_label: &Label,
    typeql_value_type: &TypeQLValueType,
    capability_undefinable: &CapabilityType,
) -> Result<(), UndefineError> {
    let TypeEnum::Attribute(attribute_type) = &type_ else {
        return Err(err_unsupported_capability(type_label, type_.kind(), capability_undefinable));
    };
    let value_type = resolve_value_type(snapshot, type_manager, &typeql_value_type.value_type)
        .map_err(|typedb_source| UndefineError::ValueTypeSymbolResolution { typedb_source })?;

    let definition_status = get_value_type_status(
        snapshot,
        type_manager,
        *attribute_type,
        value_type.clone(),
        DefinableStatusMode::Transitive,
    )
    .map_err(|source| UndefineError::UnexpectedConceptRead { typedb_source: source })?;
    match definition_status {
        DefinableStatus::ExistsSame(_) => attribute_type
            .unset_value_type(snapshot, type_manager, thing_manager)
            .map_err(|source| UndefineError::UnsetValueTypeError {
                label: type_label.to_owned(),
                value_type,
                source_span: typeql_value_type.span(),
                typedb_source: source,
            }),
        DefinableStatus::DoesNotExist => Err(UndefineError::AttributeTypeValueTypeNotDefined {
            type_: type_label.clone(),
            key: Keyword::Value,
            value_type,
            source_span: capability_undefinable.span(),
        }),
        DefinableStatus::ExistsDifferent(existing_value_type) => {
            Err(UndefineError::AttributeTypeValueTypeDefinedButDifferent {
                type_: type_label.clone(),
                key: Keyword::Value,
                value_type,
                existing_value_type,
                source_span: capability_undefinable.span(),
            })
        }
    }
}

fn undefine_type_annotation(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    annotation_undefinable: &AnnotationType,
) -> Result<(), UndefineError> {
    let label = Label::parse_from(
        checked_identifier(&annotation_undefinable.type_.ident)?,
        annotation_undefinable.type_.span(),
    );
    let type_ = resolve_typeql_type(snapshot, type_manager, &label)
        .map_err(|source| UndefineError::DefinitionResolution { typedb_source: source })?;
    let annotation_category = translate_annotation_category(annotation_undefinable.annotation_category)
        .map_err(|typedb_source| UndefineError::LiteralParseError { typedb_source })?;
    match type_ {
        TypeEnum::Entity(entity_type) => {
            check_can_and_need_undefine_type_annotation(
                snapshot,
                type_manager,
                &label,
                entity_type,
                annotation_category,
                annotation_undefinable,
            )?;
            entity_type.unset_annotation(snapshot, type_manager, annotation_category)
        }
        TypeEnum::Relation(relation_type) => {
            check_can_and_need_undefine_type_annotation(
                snapshot,
                type_manager,
                &label,
                relation_type,
                annotation_category,
                annotation_undefinable,
            )?;
            relation_type.unset_annotation(snapshot, type_manager, annotation_category)
        }
        TypeEnum::Attribute(attribute_type) => {
            if AttributeTypeAnnotation::is_value_type_annotation_category(annotation_category) {
                return Err(UndefineError::IllegalAnnotation {
                    typedb_source: AnnotationError::UnsupportedAnnotationForAttributeType {
                        category: annotation_category,
                    },
                });
            }
            check_can_and_need_undefine_type_annotation(
                snapshot,
                type_manager,
                &label,
                attribute_type,
                annotation_category,
                annotation_undefinable,
            )?;
            attribute_type.unset_annotation(snapshot, type_manager, annotation_category)
        }
        TypeEnum::RoleType(_) => unreachable!("Role annotations are syntactically on relates"),
    }
    .map_err(|err| UndefineError::UnsetTypeAnnotationError {
        typedb_source: err,
        label,
        annotation_category,
        source_span: annotation_undefinable.span(),
    })
}

fn undefine_type(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    label_undefinable: &TypeQLLabel,
) -> Result<(), UndefineError> {
    let label = Label::parse_from(checked_identifier(&label_undefinable.ident)?, label_undefinable.span());
    let type_ = resolve_typeql_type(snapshot, type_manager, &label)
        .map_err(|source| UndefineError::DefinitionResolution { typedb_source: source })?;

    match type_ {
        TypeEnum::Entity(entity_type) => entity_type.delete(snapshot, type_manager, thing_manager),
        TypeEnum::Relation(relation_type) => relation_type.delete(snapshot, type_manager, thing_manager),
        TypeEnum::Attribute(attribute_type) => attribute_type.delete(snapshot, type_manager, thing_manager),
        TypeEnum::RoleType(_) => unreachable!("Role undefinition is processed through relates undefinition"),
    }
    .map_err(|err| UndefineError::TypeDeleteError {
        typedb_source: err,
        label,
        source_span: label_undefinable.span(),
    })
}

fn undefine_struct(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    struct_undefinable: &Struct,
) -> Result<(), UndefineError> {
    let name = checked_identifier(&struct_undefinable.ident)?;
    let struct_key = resolve_struct_definition_key(snapshot, type_manager, name)
        .map_err(|source| UndefineError::DefinitionResolution { typedb_source: source })?;

    type_manager.delete_struct(snapshot, thing_manager, &struct_key).map_err(|err| UndefineError::StructDeleteError {
        typedb_source: err,
        struct_name: name.to_owned(),
        source_span: struct_undefinable.span(),
    })
}

fn check_can_and_need_undefine_sub<T: TypeAPI>(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    label: &Label,
    type_: T,
    supertype: T,
    declaration: &CapabilityType,
) -> Result<bool, UndefineError> {
    let definition_status = get_sub_status(snapshot, type_manager, type_, supertype)
        .map_err(|source| UndefineError::UnexpectedConceptRead { typedb_source: source })?;
    match definition_status {
        DefinableStatus::ExistsSame(_) => Ok(true),
        DefinableStatus::DoesNotExist => Err(UndefineError::TypeSubNotDefined {
            type_: label.clone(),
            key: Keyword::Sub,
            supertype: supertype
                .get_label(snapshot, type_manager)
                .map_err(|source| UndefineError::UnexpectedConceptRead { typedb_source: source })?
                .clone(),
            source_span: declaration.span(),
        }),
        DefinableStatus::ExistsDifferent(existing) => Err(UndefineError::TypeSubDefinedButDifferent {
            type_: label.clone(),
            key: Keyword::Sub,
            supertype: supertype
                .get_label(snapshot, type_manager)
                .map_err(|source| UndefineError::UnexpectedConceptRead { typedb_source: source })?
                .clone(),
            existing_supertype: existing
                .get_label(snapshot, type_manager)
                .map_err(|source| UndefineError::UnexpectedConceptRead { typedb_source: source })?
                .clone(),
            source_span: declaration.span(),
        }),
    }
}

fn check_can_and_need_undefine_type_annotation<T: KindAPI>(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    label: &Label,
    type_: T,
    annotation_category: AnnotationCategory,
    declaration: &AnnotationType,
) -> Result<bool, UndefineError> {
    let definition_status = get_type_annotation_category_status(snapshot, type_manager, type_, annotation_category)
        .map_err(|source| UndefineError::UnexpectedConceptRead { typedb_source: source })?;
    match definition_status {
        DefinableStatus::ExistsSame(_) => Ok(true),
        DefinableStatus::ExistsDifferent(_) => unreachable!("Annotation categories cannot differ"),
        DefinableStatus::DoesNotExist => Err(UndefineError::TypeAnnotationNotDefined {
            type_: label.clone(),
            annotation: annotation_category,
            source_span: declaration.span(),
        }),
    }
}

fn check_can_and_need_undefine_capability_annotation<CAP: Capability>(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    capability: CAP,
    annotation_category: AnnotationCategory,
    declaration: &AnnotationCapability,
) -> Result<bool, UndefineError> {
    let definition_status =
        get_capability_annotation_category_status(snapshot, type_manager, &capability, annotation_category)
            .map_err(|source| UndefineError::UnexpectedConceptRead { typedb_source: source })?;
    match definition_status {
        DefinableStatus::ExistsSame(_) => Ok(true),
        DefinableStatus::ExistsDifferent(_) => unreachable!("Annotation categories cannot differ"),
        DefinableStatus::DoesNotExist => Err(UndefineError::CapabilityAnnotationNotDefined {
            annotation: annotation_category,
            source_span: declaration.span(),
        }),
    }
}

fn err_capability_kind_mismatch(
    left: &Label,
    right: &Label,
    capability: &CapabilityType,
    left_kind: Kind,
    right_kind: Kind,
) -> UndefineError {
    UndefineError::CapabilityKindMismatch {
        left: left.clone(),
        right: right.clone(),
        left_kind,
        right_kind,
        source_span: capability.span(),
    }
}

fn err_unsupported_capability(label: &Label, kind: Kind, capability: &CapabilityType) -> UndefineError {
    UndefineError::TypeCannotHaveCapability { type_: label.to_owned(), kind, source_span: capability.span() }
}

typedb_error! {
    pub UndefineError(component = "Undefine execution", prefix = "UEX") {
        Unimplemented(1, "Unimplemented undefine functionality: {description}", description: String),
        UnexpectedConceptRead(2, "Concept read error during undefine query execution.", typedb_source: Box<ConceptReadError>),
        DefinitionResolution(3, "Could not find symbol in undefine query.", typedb_source: Box<SymbolResolutionError>),
        LiteralParseError(4, "Error parsing literal in undefine query.", typedb_source: LiteralParseError),
        StructDoesNotExist(5, "Struct used in undefine query does not exist.", source_span: Option<Span>),
        StructDeleteError(
            6,
            "Error removing struct type '{struct_name}'.",
            struct_name: String,
            source_span: Option<Span>,
            typedb_source: Box<ConceptWriteError>
        ),
        TypeDeleteError(
            7,
            "Error removing type '{label}'.",
            label: Label,
            source_span: Option<Span>,
            typedb_source: Box<ConceptWriteError>
        ),
        ValueTypeSymbolResolution(
            8,
            "Error resolving value type in undefine query.",
            typedb_source: Box<SymbolResolutionError>
        ),
        UnsetSupertypeError(
            9,
            "Undefining supertype failed.",
            source_span: Option<Span>,
            typedb_source: Box<ConceptWriteError>
        ),
        UnsetOwnsError(
            10,
            "Undefining owns failed.",
            source_span: Option<Span>,
            typedb_source: Box<ConceptWriteError>
        ),
        DeleteRoleTypeError(
            11,
            "Undefining relates (role type) failed.",
            source_span: Option<Span>,
            typedb_source: Box<ConceptWriteError>
        ),
        UnsetPlaysError(
            12,
            "Undefining plays failed.",
            source_span: Option<Span>,
            typedb_source: Box<ConceptWriteError>
        ),
        UnsetValueTypeError(
            13,
            "Undefining '{label}' to have value type '{value_type}' failed.",
            label: Label,
            value_type: ValueType,
            source_span: Option<Span>,
            typedb_source: Box<ConceptWriteError>
        ),
        UnsetTypeAnnotationError(
            14,
            "Undefining '{label}' to have annotation '{annotation_category}' failed.",
            label: Label,
            annotation_category: AnnotationCategory,
            source_span: Option<Span>,
            typedb_source: Box<ConceptWriteError>
        ),
        UnsetCapabilityAnnotationError(
            15,
            "Undefining '{annotation_category}' failed.",
            annotation_category: AnnotationCategory,
            source_span: Option<Span>,
            typedb_source: Box<ConceptWriteError>
        ),
        UnsetRelatesSpecialiseError(
            16,
            "For relation type '{type_}', undefining `relates as' failed.",
            type_: Label,
            source_span: Option<Span>,
            typedb_source: Box<ConceptWriteError>
        ),
        TypeCannotHaveCapability(
            17,
            "Invalid undefine - the type '{type_}' of kind '{kind}', which cannot this declaration. Maybe you wanted to undefine something else?",
            type_: Label,
            kind: Kind,
            source_span: Option<Span>,
        ),
        TypeSubNotDefined(
            18,
            "Undefining '{key} {supertype}' for type '{type_}' failed since there is no defined '{type_} {key} {supertype}'.",
            type_: Label,
            key: Keyword,
            supertype: Label,
            source_span: Option<Span>,
        ),
        TypeSubDefinedButDifferent(
            19,
            "Undefining '{key} {supertype}' for type '{type_}' failed since there is no defined '{type_} {key} {supertype}', while {type_}'s defined '{key}' is '{existing_supertype}'.",
            type_: Label,
            key: Keyword,
            supertype: Label,
            existing_supertype: Label,
            source_span: Option<Span>,
        ),
        OwnsNotDefined(
            20,
            "Undefining '{key} {attribute}{ordering}' for type '{type_}' failed since there is no defined '{type_} {key} {attribute}{ordering}'.",
            type_: Label,
            key: Keyword,
            attribute: Label,
            ordering: Ordering,
            source_span: Option<Span>,
        ),
        OwnsDefinedButDifferent(
            21,
            "Undefining '{key} {attribute}{ordering}' for type '{type_}' failed since there is no defined '{type_} {key} {attribute}{ordering}', while {type_}'s defined '{key}' is '{attribute}{existing_ordering}'.",
            type_: Label,
            key: Keyword,
            attribute: Label,
            ordering: Ordering,
            existing_ordering: Ordering,
            source_span: Option<Span>,
        ),
        OwnsOfAnnotationDefinedButDifferent(
            22,
            "Undefining annotation '{annotation}' of `{key} {attribute}{ordering}` for type '{type_}' failed since there is no defined '{type_} {key} {attribute}{ordering}', while {type_}'s defined '{key}' is '{attribute}{existing_ordering}'.",
            type_: Label,
            key: Keyword,
            annotation: AnnotationCategory,
            attribute: Label,
            ordering: Ordering,
            existing_ordering: Ordering,
            source_span: Option<Span>,
        ),
        RelatesNotDefined(
            23,
            "Undefining '{key} {role}{ordering}' for type '{type_}' failed since there is no defined '{type_} {key} {role}{ordering}'.",
            type_: Label,
            key: Keyword,
            role: Label,
            ordering: Ordering,
            source_span: Option<Span>,
        ),
        RelatesDefinedButDifferent(
            24,
            "Undefining '{key} {role}{ordering}' for type '{type_}' failed since there is no defined '{type_} {key} {role}{ordering}', while {type_}'s defined '{key}' is '{role}{existing_ordering}'.",
            type_: Label,
            key: Keyword,
            role: Label,
            ordering: Ordering,
            existing_ordering: Ordering,
            source_span: Option<Span>,
        ),
        RelatesOfAnnotationDefinedButDifferent(
            25,
            "Undefining annotation '{annotation}' of `{key} {role}{ordering}` for type '{type_}' failed since there is no defined '{type_} {key} {role}{ordering}', while {type_}'s defined '{key}' is '{role}{existing_ordering}'.",
            type_: Label,
            key: Keyword,
            annotation: AnnotationCategory,
            role: Label,
            ordering: Ordering,
            existing_ordering: Ordering,
            source_span: Option<Span>,
        ),
        PlaysNotDefined(
            26,
            "Undefining '{key} {role}' for type '{type_}' failed since there is no defined '{type_} {key} {role}'.",
            type_: Label,
            key: Keyword,
            role: Label,
            source_span: Option<Span>,
        ),
        AttributeTypeValueTypeNotDefined(
            27,
            "Undefining '{key} {value_type}' for type '{type_}' failed since there is no defined '{type_} {key} {value_type}'.",
            type_: Label,
            key: Keyword,
            value_type: ValueType,
            source_span: Option<Span>,
        ),
        AttributeTypeValueTypeDefinedButDifferent(
            28,
            "Undefining '{key} {value_type}' for type '{type_}' failed since there is no defined '{type_} {key} {value_type}', while {type_}'s defined '{key}' is '{existing_value_type}'.",
            type_: Label,
            key: Keyword,
            value_type: ValueType,
            existing_value_type: ValueType,
            source_span: Option<Span>,
        ),
        RelatesSpecialiseNotDefined(
            29,
            "Undefining '{as_key} {specialised_role_name}' for '{type_} {relates_key} {specialising_role_name}' failed since there is no defined '{type_} {relates_key} {specialising_role_name} {as_key} {specialised_role_name}'.",
            type_: Label,
            relates_key: Keyword,
            as_key: Keyword,
            specialising_role_name: String,
            specialised_role_name: String,
            source_span: Option<Span>,
        ),
        RelatesSpecialiseDefinedButDifferent(
            30,
            "Undefining '{as_key} {specialised_role_name}' for '{type_} {relates_key} {specialising_role_name}' failed since there is no defined '{type_} {relates_key} {specialising_role_name} {as_key} {specialised_role_name}', while there is a defined specialisation '{type_} {relates_key} {specialising_role_name} {as_key} {existing_specialised_role_name}'.",
            type_: Label,
            relates_key: Keyword,
            as_key: Keyword,
            specialising_role_name: String,
            specialised_role_name: String,
            existing_specialised_role_name: String,
            source_span: Option<Span>,
        ),
        TypeAnnotationNotDefined(
            31,
            "Undefining annotation '{annotation}' for type '{type_}' failed since there is no defined '{type_} {annotation}'.",
            type_: Label,
            annotation: AnnotationCategory,
            source_span: Option<Span>,
        ),
        CapabilityAnnotationNotDefined(
            32,
            "Undefining annotation '{annotation}' for a capability failed since there is no defined annotation of this category.",
            annotation: AnnotationCategory,
            source_span: Option<Span>,
        ),
        IllegalAnnotation(
            33,
            "Illegal annotation",
            typedb_source: AnnotationError
        ),
        CapabilityKindMismatch(
            34,
            "Undefining failed because the left type '{left}' is of kind '{left_kind}' isn't the same kind as the right type '{right}' which has kind '{right_kind}'. Maybe you wanted to undefine something else?",
            left: Label,
            right: Label,
            left_kind: Kind,
            right_kind: Kind,
            source_span: Option<Span>,
        ),
        FunctionUndefinition(
            35,
            "Undefining the function '{name}' failed",
            name: String,
            source_span: Option<Span>,
            typedb_source: Box<FunctionError>
        ),
        IllegalKeywordAsIdentifier(
            37,
            "The reserved keyword '{identifier}' cannot be used as an identifier.",
            identifier: typeql::Identifier,
            source_span: Option<Span>,
        ),
    }
}
