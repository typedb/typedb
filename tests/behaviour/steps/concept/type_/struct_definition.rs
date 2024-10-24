/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;

use cucumber::gherkin::Step;
use macro_rules_attribute::apply;

use crate::{
    concept::type_::BehaviourConceptTestExecutionError,
    generic_step,
    params::{self, check_boolean},
    transaction_context::{with_read_tx, with_schema_tx},
    util, Context,
};

#[apply(generic_step)]
#[step(expr = "create struct: {type_label}{may_error}")]
pub async fn struct_create(context: &mut Context, type_label: params::Label, may_error: params::MayError) {
    with_schema_tx!(context, |tx| {
        may_error.check_concept_write_without_read_errors(&tx.type_manager.create_struct(
            Arc::get_mut(&mut tx.snapshot).unwrap(),
            type_label.into_typedb().scoped_name().as_str().to_owned(),
        ));
    });
}

#[apply(generic_step)]
#[step(expr = "delete struct: {type_label}{may_error}")]
pub async fn struct_delete(context: &mut Context, type_label: params::Label, may_error: params::MayError) {
    with_schema_tx!(context, |tx| {
        if let Some(definition_key) = &tx
            .type_manager
            .get_struct_definition_key(tx.snapshot.as_ref(), type_label.into_typedb().scoped_name().as_str())
            .unwrap()
        {
            may_error.check_concept_write_without_read_errors(&tx.type_manager.delete_struct(
                Arc::get_mut(&mut tx.snapshot).unwrap(),
                &tx.thing_manager,
                definition_key,
            ));
        } else {
            may_error.check::<(), _>(Err(BehaviourConceptTestExecutionError::CannotFindStructDefinition));
        }
    });
}

#[apply(generic_step)]
#[step(expr = "struct\\({type_label}\\) {exists_or_doesnt}")]
pub async fn struct_exists(context: &mut Context, type_label: params::Label, exists: params::ExistsOrDoesnt) {
    with_read_tx!(context, |tx| {
        let type_label = type_label.into_typedb();
        let definition_key_opt = &tx
            .type_manager
            .get_struct_definition_key(tx.snapshot.as_ref(), type_label.scoped_name().as_str())
            .unwrap();
        exists.check(definition_key_opt, &format!("struct definition key for {}", type_label));
        if let Some(definition_key) = definition_key_opt {
            let struct_definition =
                &tx.type_manager.get_struct_definition(tx.snapshot.as_ref(), definition_key.clone());
            exists.check_result(struct_definition, &format!("struct definition for {}", type_label));
        }
    });
}

// TODO: {value_type} should be {struct_or_value_type}
#[apply(generic_step)]
#[step(
    expr = "struct\\({type_label}\\) create field: {type_label}, with value type: {value_type}{optional}{may_error}"
)]
pub async fn struct_create_field_with_value_type(
    context: &mut Context,
    type_label: params::Label,
    field_label: params::Label,
    value_type: params::ValueType,
    optional: params::Optional,
    may_error: params::MayError,
) {
    with_schema_tx!(context, |tx| {
        let definition_key = &tx
            .type_manager
            .get_struct_definition_key(tx.snapshot.as_ref(), type_label.into_typedb().scoped_name().as_str())
            .unwrap()
            .unwrap();
        let parsed_value_type = value_type.into_typedb(&tx.type_manager, tx.snapshot.as_ref());
        may_error.check_concept_write_without_read_errors(&tx.type_manager.create_struct_field(
            Arc::get_mut(&mut tx.snapshot).unwrap(),
            definition_key.clone(),
            field_label.into_typedb().scoped_name().as_str(),
            parsed_value_type,
            optional.into_typedb(),
        ));
    });
}

#[apply(generic_step)]
#[step(expr = "struct\\({type_label}\\) delete field: {type_label}{may_error}")]
pub async fn struct_delete_field(
    context: &mut Context,
    type_label: params::Label,
    field_label: params::Label,
    may_error: params::MayError,
) {
    with_schema_tx!(context, |tx| {
        let definition_key = &tx
            .type_manager
            .get_struct_definition_key(tx.snapshot.as_ref(), type_label.into_typedb().scoped_name().as_str())
            .unwrap()
            .unwrap();
        may_error.check_concept_write_without_read_errors(&tx.type_manager.delete_struct_field(
            Arc::get_mut(&mut tx.snapshot).unwrap(),
            &tx.thing_manager,
            definition_key.clone(),
            field_label.into_typedb().scoped_name().as_str(),
        ));
    });
}

#[apply(generic_step)]
#[step(expr = "struct\\({type_label}\\) get fields {contains_or_doesnt}:")]
pub async fn struct_get_fields_contains_or_doesnt(
    context: &mut Context,
    type_label: params::Label,
    contains_or_doesnt: params::ContainsOrDoesnt,
    step: &Step,
) {
    let expected_fields: Vec<String> = util::iter_table(step).map(|str| str.to_owned()).collect();
    with_read_tx!(context, |tx| {
        let definition_key = &tx
            .type_manager
            .get_struct_definition_key(tx.snapshot.as_ref(), type_label.into_typedb().scoped_name().as_str())
            .unwrap()
            .unwrap();
        let struct_definition =
            &tx.type_manager.get_struct_definition(tx.snapshot.as_ref(), definition_key.clone()).unwrap();
        let actual_fields: Vec<String> =
            struct_definition.field_names.keys().cloned().map(|key| key.to_owned()).collect();
        contains_or_doesnt.check(&expected_fields, &actual_fields);
    });
}

#[apply(generic_step)]
#[step(expr = "struct\\({type_label}\\) get field\\({type_label}\\) get value type: {value_type}")]
pub async fn struct_get_field_get_value_type(
    context: &mut Context,
    type_label: params::Label,
    field_label: params::Label,
    value_type: params::ValueType,
) {
    with_read_tx!(context, |tx| {
        let definition_key = &tx
            .type_manager
            .get_struct_definition_key(tx.snapshot.as_ref(), type_label.into_typedb().scoped_name().as_str())
            .unwrap()
            .unwrap();
        let struct_definition =
            &tx.type_manager.get_struct_definition(tx.snapshot.as_ref(), definition_key.clone()).unwrap();
        let actual_value_type = struct_definition
            .fields
            .get(struct_definition.field_names.get(field_label.into_typedb().scoped_name().as_str()).unwrap())
            .unwrap()
            .value_type
            .clone();
        assert_eq!(value_type.into_typedb(&tx.type_manager, tx.snapshot.as_ref()), actual_value_type);
    });
}

#[apply(generic_step)]
#[step(expr = "struct\\({type_label}\\) get field\\({type_label}\\) is optional: {boolean}")]
pub async fn struct_get_field_is_optional(
    context: &mut Context,
    type_label: params::Label,
    field_label: params::Label,
    is_optional: params::Boolean,
) {
    with_read_tx!(context, |tx| {
        let definition_key = &tx
            .type_manager
            .get_struct_definition_key(tx.snapshot.as_ref(), type_label.into_typedb().scoped_name().as_str())
            .unwrap()
            .unwrap();
        let struct_definition =
            &tx.type_manager.get_struct_definition(tx.snapshot.as_ref(), definition_key.clone()).unwrap();
        let actual_is_optional = struct_definition
            .fields
            .get(struct_definition.field_names.get(field_label.into_typedb().scoped_name().as_str()).unwrap())
            .unwrap()
            .optional;
        check_boolean!(is_optional, actual_is_optional);
    });
}
