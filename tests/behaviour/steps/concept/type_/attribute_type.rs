/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use macro_rules_attribute::apply;

use crate::{
    generic_step, params,
    transaction_context::{with_read_tx, with_schema_tx},
    Context,
};

#[apply(generic_step)]
#[step(expr = "attribute\\({type_label}\\) set value-type: {value_type}")]
pub async fn attribute_type_set_value_type(
    context: &mut Context,
    type_label: params::Label,
    value_type: params::ValueType,
) {
    with_schema_tx!(context, |tx| {
        let attribute_type =
            tx.type_manager.get_attribute_type(&tx.snapshot, &type_label.to_typedb()).unwrap().unwrap();
        attribute_type.set_value_type(&mut tx.snapshot, &tx.type_manager, value_type.to_typedb()).unwrap();
    });
}

#[apply(generic_step)]
#[step(expr = "attribute\\({type_label}\\) get value type: {value_type}")]
pub async fn attribute_type_get_value_type(
    context: &mut Context,
    type_label: params::Label,
    value_type: params::ValueType,
) {
    with_read_tx!(context, |tx| {
        let attribute_type =
            tx.type_manager.get_attribute_type(&tx.snapshot, &type_label.to_typedb()).unwrap().unwrap();
        assert_eq!(
            value_type.to_typedb(),
            attribute_type.get_value_type(&tx.snapshot, &tx.type_manager).unwrap().unwrap()
        );
    });
}

// #[apply(generic_step)]
// #[step(expr = "attribute\\({type_label}\\)) get owners, with annotations: {annotations}; contain:")]
// pub async fn TODO(context: &mut Context, type_label: params::Label, ...) { todo!(); }
// #[apply(generic_step)]
// #[step(expr = "attribute\\({type_label}\\)) get owners, with annotations: {annotations}; do not contain:")]
// pub async fn TODO(context: &mut Context, type_label: params::Label, ...) { todo!(); }
// #[apply(generic_step)]
// #[step(expr = "attribute\\({type_label}\\)) get owners explicit, with annotations: {annotations}; contain:")]
// pub async fn TODO(context: &mut Context, type_label: params::Label, ...) { todo!(); }
//
// #[apply(generic_step)]
// #[step(expr = "attribute\\({type_label}\\)) get owners explicit, with annotations: {annotations}; do not contain:")]
// pub async fn TODO(context: &mut Context, type_label: params::Label, ...) { todo!(); }
// #[apply(generic_step)]
// #[step(expr = "attribute\\({type_label}\\)) get owners contain:")]
// pub async fn TODO(context: &mut Context, type_label: params::Label, ...) { todo!(); }
// #[apply(generic_step)]
// #[step(expr = "attribute\\({type_label}\\)) get owners do not contain:")]
// pub async fn TODO(context: &mut Context, type_label: params::Label, ...) { todo!(); }
// #[apply(generic_step)]
// #[step(expr = "attribute\\({type_label}\\)) get owners explicit contain:")]
// pub async fn TODO(context: &mut Context, type_label: params::Label, ...) { todo!(); }
// #[step(expr = "attribute\\({type_label}\\)) get owners explicit do not contain:")]
// pub async fn TODO(context: &mut Context, type_label: params::Label, ...) { todo!(); }
