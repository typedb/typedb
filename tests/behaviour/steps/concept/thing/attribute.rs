/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use macro_rules_attribute::apply;

use crate::{
    generic_step, params,
    transaction_context::{with_read_tx, with_write_tx},
    Context,
};

#[apply(generic_step)]
#[step(expr = r"{var} = attribute\({type_label}\) put instance with value: {value}")]
pub async fn attribute_put_instance_with_value(
    context: &mut Context,
    var: params::Var,
    type_label: params::Label,
    value: params::Value,
) {
    let att = with_write_tx!(context, |tx| {
        let attribute_type =
            tx.type_manager.get_attribute_type(&tx.snapshot, &type_label.to_typedb()).unwrap().unwrap();
        let value = value.to_typedb(attribute_type.get_value_type(&tx.snapshot, &tx.type_manager).unwrap().unwrap());
        tx.thing_manager.create_attribute(&mut tx.snapshot, attribute_type, value).unwrap()
    });
    context.attributes.insert(var.name, Some(att));
}

#[apply(generic_step)]
#[step(expr = r"attribute {var} exists")]
pub async fn attribute_exists(context: &mut Context, var: params::Var) {
    assert!(context.attributes.contains_key(&var.name), "no variable {} in context.things", var.name);
}

#[apply(generic_step)]
#[step(expr = r"attribute {var} has type: {type_label}")]
pub async fn attribute_has_type(context: &mut Context, var: params::Var, type_label: params::Label) {
    let attribute_type = context.attributes[&var.name].as_ref().unwrap().type_();
    with_read_tx!(context, |tx| {
        assert_eq!(attribute_type.get_label(&tx.snapshot, &tx.type_manager).unwrap(), type_label.to_typedb())
    });
}

#[apply(generic_step)]
#[step(expr = r"attribute {var} has value type: {value_type}")]
pub async fn attribute_has_value_type(context: &mut Context, var: params::Var, value_type: params::ValueType) {
    let attribute_type = context.attributes[&var.name].as_ref().unwrap().type_();
    with_read_tx!(context, |tx| {
        assert_eq!(
            attribute_type.get_value_type(&tx.snapshot, &tx.type_manager).unwrap().unwrap(),
            value_type.to_typedb()
        );
    });
}

#[apply(generic_step)]
#[step(expr = r"attribute {var} has value: {value}")]
pub async fn attribute_has_value(context: &mut Context, var: params::Var, value: params::Value) {
    let mut attribute = context.attributes[&var.name].clone().unwrap();
    let attribute_type = attribute.type_();
    with_read_tx!(context, |tx| {
        let value = value.to_typedb(attribute_type.get_value_type(&tx.snapshot, &tx.type_manager).unwrap().unwrap());
        assert_eq!(attribute.get_value(&tx.snapshot, &tx.thing_manager).unwrap(), value);
    });
}

#[apply(generic_step)]
#[step(expr = r"{var} = attribute\({type_label}\) get instance with value: {value}")]
pub async fn attribute_get_instance_with_value(
    context: &mut Context,
    var: params::Var,
    type_label: params::Label,
    value: params::Value,
) {
    let att = with_read_tx!(context, |tx| {
        let attribute_type =
            tx.type_manager.get_attribute_type(&tx.snapshot, &type_label.to_typedb()).unwrap().unwrap();
        let value = value.to_typedb(attribute_type.get_value_type(&tx.snapshot, &tx.type_manager).unwrap().unwrap());
        tx.thing_manager.get_attribute_with_value(&tx.snapshot, attribute_type, value).unwrap()
    });
    context.attributes.insert(var.name, att);
}
