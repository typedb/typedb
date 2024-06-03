/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use concept::{
    error::ConceptWriteError,
    thing::{
        attribute::Attribute,
        object::{Object, ObjectAPI},
    },
    type_::attribute_type::AttributeType,
};
use itertools::Itertools;
use macro_rules_attribute::apply;

use crate::{
    generic_step, params,
    transaction_context::{with_read_tx, with_write_tx},
    Context,
};

pub(super) fn object_set_has_impl(
    context: &mut Context,
    object: &Object<'static>,
    attribute: &Attribute<'static>,
) -> Result<(), ConceptWriteError> {
    with_write_tx!(context, |tx| object.set_has_unordered(
        &mut tx.snapshot,
        &tx.thing_manager,
        attribute.as_reference()
    ))
}

pub(super) fn object_set_has_ordered_impl(
    context: &mut Context,
    object: &Object<'static>,
    attribute_type: AttributeType<'static>,
    attributes: Vec<Attribute<'static>>,
) -> Result<(), ConceptWriteError> {
    with_write_tx!(context, |tx| object.set_has_ordered(
        &mut tx.snapshot,
        &tx.thing_manager,
        attribute_type,
        attributes
    ))
}

fn object_unset_has_impl(
    context: &mut Context,
    object: &Object<'static>,
    key: &Attribute<'static>,
) -> Result<(), ConceptWriteError> {
    with_write_tx!(context, |tx| object.unset_has_unordered(&mut tx.snapshot, &tx.thing_manager, key.as_reference()))
}

#[apply(generic_step)]
#[step(expr = r"{object_root_label} {var} set has: {var}{may_error}")]
async fn object_set_has(
    context: &mut Context,
    object_root: params::ObjectRootLabel,
    object_var: params::Var,
    attribute_var: params::Var,
    may_error: params::MayError,
) {
    let object = context.objects[&object_var.name].as_ref().unwrap().object.to_owned();
    object_root.assert(&object.type_());
    let attribute = context.attributes[&attribute_var.name].as_ref().unwrap().to_owned();
    may_error.check(&object_set_has_impl(context, &object, &attribute));
}

#[apply(generic_step)]
#[step(expr = r"{object_root_label} {var} set has\({type_label}[]\): {vars}{may_error}")]
async fn object_set_has_list(
    context: &mut Context,
    object_root: params::ObjectRootLabel,
    object_var: params::Var,
    attribute_type_label: params::Label,
    attribute_vars: params::Vars,
    may_error: params::MayError,
) {
    let object = context.objects[&object_var.name].as_ref().unwrap().object.to_owned();
    object_root.assert(&object.type_());
    let attribute_type = with_read_tx!(context, |tx| {
        tx.type_manager.get_attribute_type(&tx.snapshot, &attribute_type_label.to_typedb()).unwrap().unwrap()
    });
    let attributes = attribute_vars
        .names
        .into_iter()
        .map(|attr_name| context.attributes[&attr_name].as_ref().unwrap().to_owned())
        .collect_vec();
    may_error.check(&object_set_has_ordered_impl(context, &object, attribute_type, attributes));
}

#[apply(generic_step)]
#[step(expr = r"{object_root_label} {var} unset has: {var}")]
async fn object_unset_has(
    context: &mut Context,
    object_root: params::ObjectRootLabel,
    object_var: params::Var,
    attribute_var: params::Var,
) {
    let object = context.objects[&object_var.name].as_ref().unwrap().object.to_owned();
    object_root.assert(&object.type_());
    let attribute = context.attributes[&attribute_var.name].as_ref().unwrap().to_owned();
    object_unset_has_impl(context, &object, &attribute).unwrap();
}
