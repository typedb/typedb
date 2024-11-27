/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;

use concept::{
    error::ConceptWriteError,
    thing::{
        attribute::Attribute,
        object::{Object, ObjectAPI},
    },
    type_::{attribute_type::AttributeType, OwnerAPI},
};
use itertools::Itertools;
use lending_iterator::LendingIterator;
use macro_rules_attribute::apply;

use crate::{
    generic_step, params,
    params::check_boolean,
    transaction_context::{with_read_tx, with_write_tx},
    Context,
};

pub(super) fn object_set_has_impl(
    context: &mut Context,
    object: &Object<'static>,
    attribute: &Attribute<'static>,
) -> Result<(), Box<ConceptWriteError>> {
    with_write_tx!(context, |tx| object.set_has_unordered(
        Arc::get_mut(&mut tx.snapshot).unwrap(),
        &tx.thing_manager,
        attribute.as_reference()
    ))
}

pub(super) fn object_set_has_ordered_impl(
    context: &mut Context,
    object: &Object<'static>,
    attribute_type: AttributeType,
    attributes: Vec<Attribute<'static>>,
) -> Result<(), Box<ConceptWriteError>> {
    with_write_tx!(context, |tx| object.set_has_ordered(
        Arc::get_mut(&mut tx.snapshot).unwrap(),
        &tx.thing_manager,
        attribute_type,
        attributes
    ))
}

fn object_unset_has_impl(
    context: &mut Context,
    object: &Object<'static>,
    key: &Attribute<'static>,
) -> Result<(), Box<ConceptWriteError>> {
    with_write_tx!(context, |tx| object.unset_has_unordered(
        Arc::get_mut(&mut tx.snapshot).unwrap(),
        &tx.thing_manager,
        key.as_reference()
    ))
}

fn object_unset_has_ordered_impl(
    context: &mut Context,
    object: &Object<'static>,
    attribute_type_label: params::Label,
) -> Result<(), Box<ConceptWriteError>> {
    with_write_tx!(context, |tx| {
        let attribute_type = tx
            .type_manager
            .get_attribute_type(tx.snapshot.as_ref(), &attribute_type_label.into_typedb())
            .unwrap()
            .unwrap();
        object.unset_has_ordered(Arc::get_mut(&mut tx.snapshot).unwrap(), &tx.thing_manager, attribute_type)
    })
}

#[apply(generic_step)]
#[step(expr = r"{object_kind} {var} set has: {var}{may_error}")]
async fn object_set_has(
    context: &mut Context,
    object_kind: params::ObjectKind,
    object_var: params::Var,
    attribute_var: params::Var,
    may_error: params::MayError,
) {
    let object = context.objects[&object_var.name].as_ref().unwrap().object.to_owned();
    object_kind.assert(&object.type_());
    let attribute = context.attributes[&attribute_var.name].as_ref().unwrap().to_owned();
    // TODO: The interesting error (CannotGetOwnsDoesntExist) is a ConceptError, so we need to expect it
    // However, there are other random ConceptErrors that can make the test look like "it passes" while it
    // is just broken
    may_error.check(object_set_has_impl(context, &object, &attribute));
}

#[apply(generic_step)]
#[step(expr = r"{object_kind} {var} set has\({type_label}[]\): {vars}{may_error}")]
async fn object_set_has_list(
    context: &mut Context,
    object_kind: params::ObjectKind,
    object_var: params::Var,
    attribute_type_label: params::Label,
    attribute_vars: params::Vars,
    may_error: params::MayError,
) {
    let object = context.objects[&object_var.name].as_ref().unwrap().object.to_owned();
    object_kind.assert(&object.type_());
    let attribute_type = with_read_tx!(context, |tx| {
        tx.type_manager.get_attribute_type(tx.snapshot.as_ref(), &attribute_type_label.into_typedb()).unwrap().unwrap()
    });
    let attributes = attribute_vars
        .names
        .into_iter()
        .map(|attr_name| context.attributes[&attr_name].as_ref().unwrap().to_owned())
        .collect_vec();
    may_error.check_concept_write_without_read_errors(&object_set_has_ordered_impl(
        context,
        &object,
        attribute_type,
        attributes,
    ));
}

#[apply(generic_step)]
#[step(expr = r"{object_kind} {var} unset has: {var}")]
async fn object_unset_has(
    context: &mut Context,
    object_kind: params::ObjectKind,
    object_var: params::Var,
    attribute_var: params::Var,
) {
    let object = context.objects[&object_var.name].as_ref().unwrap().object.to_owned();
    object_kind.assert(&object.type_());
    let attribute = context.attributes[&attribute_var.name].as_ref().unwrap().to_owned();
    object_unset_has_impl(context, &object, &attribute).unwrap();
}

#[apply(generic_step)]
#[step(expr = r"{object_kind} {var} unset has: {type_label}[]")]
async fn object_unset_has_ordered(
    context: &mut Context,
    object_kind: params::ObjectKind,
    object_var: params::Var,
    attribute_type_label: params::Label,
) {
    let object = context.objects[&object_var.name].as_ref().unwrap().object.to_owned();
    object_kind.assert(&object.type_());
    object_unset_has_ordered_impl(context, &object, attribute_type_label).unwrap();
}

#[apply(generic_step)]
#[step(expr = r"{var} = {object_kind} {var} get has\({type_label}[]\)")]
async fn object_get_has_list(
    context: &mut Context,
    attribute_var: params::Var,
    object_kind: params::ObjectKind,
    object_var: params::Var,
    attribute_type_label: params::Label,
) {
    let object = context.objects[&object_var.name].as_ref().unwrap().object.to_owned();
    object_kind.assert(&object.type_());
    let attributes = with_read_tx!(context, |tx| {
        let attribute_type = tx
            .type_manager
            .get_attribute_type(tx.snapshot.as_ref(), &attribute_type_label.into_typedb())
            .unwrap()
            .unwrap();
        object
            .get_has_type_ordered(tx.snapshot.as_ref(), &tx.thing_manager, attribute_type)
            .unwrap()
            .into_iter()
            .map(|attr| attr.into_owned())
            .collect()
    });
    context.attribute_lists.insert(attribute_var.name, attributes);
}

#[apply(generic_step)]
#[step(expr = r"{object_kind} {var} get has\({type_label}[]\) is {vars}: {boolean}")]
async fn object_get_has_list_is(
    context: &mut Context,
    object_kind: params::ObjectKind,
    object_var: params::Var,
    attribute_type_label: params::Label,
    attribute_vars: params::Vars,
    is: params::Boolean,
) {
    let object = context.objects[&object_var.name].as_ref().unwrap().object.to_owned();
    object_kind.assert(&object.type_());
    let actuals = with_read_tx!(context, |tx| {
        let attribute_type = tx
            .type_manager
            .get_attribute_type(tx.snapshot.as_ref(), &attribute_type_label.into_typedb())
            .unwrap()
            .unwrap();
        object
            .get_has_type_ordered(tx.snapshot.as_ref(), &tx.thing_manager, attribute_type)
            .unwrap()
            .into_iter()
            .map(|attr| attr.into_owned())
            .collect_vec()
    });
    let attributes = attribute_vars
        .names
        .into_iter()
        .map(|attr_name| context.attributes[&attr_name].as_ref().unwrap().to_owned())
        .collect_vec();
    check_boolean!(is, actuals == attributes)
}

#[apply(generic_step)]
#[step(expr = r"{object_kind} {var} get has {is_empty_or_not}")]
async fn object_get_has_is_empty(
    context: &mut Context,
    object_kind: params::ObjectKind,
    object_var: params::Var,
    is_empty_or_not: params::IsEmptyOrNot,
) {
    let object = context.objects[&object_var.name].as_ref().unwrap().object.to_owned();
    object_kind.assert(&object.type_());
    let actuals = with_read_tx!(context, |tx| {
        object
            .get_has_unordered(tx.snapshot.as_ref(), &tx.thing_manager)
            .map_static(|res| {
                let (attribute, _count) = res.unwrap();
                attribute.into_owned()
            })
            .collect::<Vec<_>>()
    });

    is_empty_or_not.check(actuals.is_empty());
}

#[apply(generic_step)]
#[step(expr = r"{object_kind} {var} get has {contains_or_doesnt}: {var}")]
async fn object_get_has(
    context: &mut Context,
    object_kind: params::ObjectKind,
    object_var: params::Var,
    contains_or_doesnt: params::ContainsOrDoesnt,
    attribute_var: params::Var,
) {
    let object = context.objects[&object_var.name].as_ref().unwrap().object.to_owned();
    object_kind.assert(&object.type_());
    let attribute = context.attributes[&attribute_var.name].as_ref().unwrap();
    let actuals = with_read_tx!(context, |tx| {
        object
            .get_has_unordered(tx.snapshot.as_ref(), &tx.thing_manager)
            .map_static(|res| {
                let (attribute, _count) = res.unwrap();
                attribute.into_owned()
            })
            .collect::<Vec<_>>()
    });
    contains_or_doesnt.check(std::slice::from_ref(attribute), &actuals);
}

#[apply(generic_step)]
#[step(expr = r"{object_kind} {var} get has\({type_label}\) {contains_or_doesnt}: {var}")]
async fn object_get_has_type(
    context: &mut Context,
    object_kind: params::ObjectKind,
    object_var: params::Var,
    attribute_type_label: params::Label,
    contains_or_doesnt: params::ContainsOrDoesnt,
    attribute_var: params::Var,
) {
    let object = context.objects[&object_var.name].as_ref().unwrap().object.to_owned();
    object_kind.assert(&object.type_());
    let attribute = context.attributes[&attribute_var.name].as_ref().unwrap();
    let actuals = with_read_tx!(context, |tx| {
        let attribute_type = tx
            .type_manager
            .get_attribute_type(tx.snapshot.as_ref(), &attribute_type_label.into_typedb())
            .unwrap()
            .unwrap();
        object
            .get_has_type_unordered(tx.snapshot.as_ref(), &tx.thing_manager, attribute_type)
            .map_static(|res| {
                let (attribute, _count) = res.unwrap();
                attribute.into_owned()
            })
            .collect::<Vec<_>>()
    });
    contains_or_doesnt.check(std::slice::from_ref(attribute), &actuals);
}

#[apply(generic_step)]
#[step(expr = r"{object_kind} {var} get key has; {contains_or_doesnt}: {var}")]
async fn object_get_has_with_annotations(
    context: &mut Context,
    object_kind: params::ObjectKind,
    object_var: params::Var,
    contains_or_doesnt: params::ContainsOrDoesnt,
    attribute_var: params::Var,
) {
    let object = context.objects[&object_var.name].as_ref().unwrap().object.to_owned();
    object_kind.assert(&object.type_());
    let attribute = context.attributes[&attribute_var.name].as_ref().unwrap();
    let actuals = with_read_tx!(context, |tx| {
        let attribute_types = object
            .type_()
            .get_owns_declared(tx.snapshot.as_ref(), &tx.type_manager)
            .unwrap()
            .into_iter()
            .filter(|owns| owns.is_key(tx.snapshot.as_ref(), &tx.type_manager).unwrap())
            .map(|owns| owns.attribute())
            .collect_vec();
        attribute_types
            .into_iter()
            .flat_map(|attribute_type| {
                object
                    .get_has_type_unordered(tx.snapshot.as_ref(), &tx.thing_manager, attribute_type)
                    .map_static(|res| {
                        let (attribute, _count) = res.unwrap();
                        attribute.into_owned()
                    })
                    .collect::<Vec<_>>()
            })
            .collect_vec()
    });
    contains_or_doesnt.check(std::slice::from_ref(attribute), &actuals);
}
