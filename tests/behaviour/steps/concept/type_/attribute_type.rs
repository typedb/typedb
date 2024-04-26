/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use macro_rules_attribute::apply;
use crate::{
    generic_step, tx_as_read, tx_as_schema,
    Context,
    params::{MayError, Boolean},
    transaction_context::{ActiveTransaction}

};

use crate::params;

#[apply(generic_step)]
#[step(expr = "put attribute type: {type_label}, with value type: {value_type}")]
pub async fn put_attribute_type(context: &mut Context, type_label: params::Label, value_type: params::ValueType){
    let tx = context.transaction().unwrap();
    tx_as_schema! (tx, {
        let attribute_type = tx.type_manager().create_attribute_type(&type_label.to_typedb(), false).unwrap();
        attribute_type.set_value_type(tx.type_manager(), value_type.to_typedb())
    });
}

#[apply(generic_step)]
#[step(expr = "attribute\\({type_label}\\) get value type: {value_type}")]
pub async fn attribute_type_get_value_type(context: &mut Context, type_label: params::Label, value_type: params::ValueType) {
    let tx = context.transaction().unwrap();
    tx_as_read! (tx, {
        let attribute_type = tx.type_manager().get_attribute_type(&type_label.to_typedb()).unwrap().unwrap();
        assert_eq!(value_type.to_typedb(), attribute_type.get_value_type(tx.type_manager()).unwrap().unwrap());
    });
}
//
// #[apply(generic_step)]
// #[step(expr = "attribute\\({type_label}\\)) as\\({value_type}\\) set regex: {}")]
// pub async fn attribute_type_set_regex(context: &mut Context, type_label: params::Label, value_type: params::ValueType) { todo!(); }
// #[apply(generic_step)]
// #[step(expr = "attribute\\({type_label}\\)) as\\({value_type}\\) unset regex")]
// pub async fn attribute_type_unset_regex(context: &mut Context, type_label: params::Label, value_type: params::ValueType) { todo!(); }
// #[apply(generic_step)]
// #[step(expr = "attribute\\({type_label}\\)) as\\({value_type}\\) get regex: {}")]
// pub async fn attribute_type_get_regex(context: &mut Context, type_label: params::Label, value_type: params::ValueType) { todo!(); }
// #[apply(generic_step)]
// #[step(expr = "attribute\\({type_label}\\)) as\\({value_type}\\) does not have any regex")]
// pub async fn attribute_type_no_regex(context: &mut Context, type_label: params::Label, value_type: params::ValueType) { todo!(); }
//
// #[apply(generic_step)]
// #[step(expr = "attribute\\({type_label}\\) as\\({value_type}\\) get subtypes contain:")]
// pub async fn attribute_type_subtypes_contain(context: &mut Context, type_label: params::Label, value_type: params::ValueType) {
//     todo!()
// }
//
// #[apply(generic_step)]
// #[step(expr = "attribute\\({type_label}\\) as\\({value_type}\\) get subtypes do not contain:")]
// pub async fn attribute_type_subtypes_do_not_contain(context: &mut Context, type_label: params::Label, value_type: params::ValueType) { todo!(); }

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