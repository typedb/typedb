/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;

use concept::{
    error::ConceptWriteError,
    thing::{object::Object, ThingAPI},
    type_::{object_type::ObjectType, TypeAPI},
};
use itertools::Itertools;
use macro_rules_attribute::apply;
use params::{self, check_boolean};
use test_utils::assert_matches;

use crate::{
    concept::thing::{
        attribute::{attribute_put_instance_with_value_impl, get_attribute_by_value},
        has::object_set_has_impl,
    },
    generic_step,
    thing_util::ObjectWithKey,
    transaction_context::{with_read_tx, with_write_tx},
    Context,
};

fn object_create_instance_impl(
    context: &mut Context,
    object_type_label: params::Label,
) -> Result<Object, Box<ConceptWriteError>> {
    with_write_tx!(context, |tx| {
        let object_type =
            tx.type_manager.get_object_type(tx.snapshot.as_ref(), &object_type_label.into_typedb()).unwrap().unwrap();
        match object_type {
            ObjectType::Entity(entity_type) => {
                tx.thing_manager.create_entity(Arc::get_mut(&mut tx.snapshot).unwrap(), entity_type).map(Object::Entity)
            }
            ObjectType::Relation(relation_type) => tx
                .thing_manager
                .create_relation(Arc::get_mut(&mut tx.snapshot).unwrap(), relation_type)
                .map(Object::Relation),
        }
    })
}

#[apply(generic_step)]
#[step(expr = r"{object_kind}\({type_label}\) create new instance{may_error}")]
async fn object_create_instance(
    context: &mut Context,
    object_kind: params::ObjectKind,
    object_type_label: params::Label,
    may_error: params::MayError,
) {
    let result = object_create_instance_impl(context, object_type_label);
    may_error.check(result.as_ref());
    if !may_error.expects_error() {
        let object = result.unwrap();
        object_kind.assert(&object.type_());
    }
}

#[apply(generic_step)]
#[step(expr = r"{var} = {object_kind}\({type_label}\) create new instance{may_error}")]
async fn object_create_instance_var(
    context: &mut Context,
    var: params::Var,
    object_kind: params::ObjectKind,
    object_type_label: params::Label,
    may_error: params::MayError,
) {
    let result = object_create_instance_impl(context, object_type_label);
    may_error.check(result.as_ref());
    if !may_error.expects_error() {
        let object = result.unwrap();
        object_kind.assert(&object.type_());
        context.objects.insert(var.name, Some(ObjectWithKey::new(object)));
    }
}

#[apply(generic_step)]
#[step(expr = r"{var} = {object_kind}\({type_label}\) create new instance with key\({type_label}\): {value}")]
async fn object_create_instance_with_key_var(
    context: &mut Context,
    var: params::Var,
    object_kind: params::ObjectKind,
    object_type_label: params::Label,
    key_type_label: params::Label,
    value: params::Value,
) {
    let object = object_create_instance_impl(context, object_type_label).unwrap();
    object_kind.assert(&object.type_());
    let key = attribute_put_instance_with_value_impl(context, key_type_label, value).unwrap();
    object_set_has_impl(context, &object, &key).unwrap();
    context.objects.insert(var.name, Some(ObjectWithKey::new_with_key(object, key)));
}

#[apply(generic_step)]
#[step(expr = r"delete {object_kind}: {var}")]
async fn delete_object(context: &mut Context, object_kind: params::ObjectKind, var: params::Var) {
    let object = context.objects[&var.name].as_ref().unwrap().object;
    object_kind.assert(&object.type_());
    with_write_tx!(context, |tx| { object.delete(Arc::get_mut(&mut tx.snapshot).unwrap(), &tx.thing_manager).unwrap() })
}

#[apply(generic_step)]
#[step(expr = r"delete {object_kind} of type: {type_label}")]
async fn delete_objects_of_type(context: &mut Context, object_kind: params::ObjectKind, type_label: params::Label) {
    with_write_tx!(context, |tx| {
        let object_type =
            tx.type_manager.get_object_type(tx.snapshot.as_ref(), &type_label.into_typedb()).unwrap().unwrap();
        object_kind.assert(&object_type);
        match object_type {
            ObjectType::Entity(entity_type) => {
                let mut entity_iterator = tx.thing_manager.get_entities_in(tx.snapshot.as_ref(), entity_type);
                for entity in entity_iterator {
                    entity.unwrap().delete(Arc::get_mut(&mut tx.snapshot).unwrap(), &tx.thing_manager).unwrap();
                }
            }
            ObjectType::Relation(relation_type) => {
                let mut relation_iterator = tx.thing_manager.get_relations_in(tx.snapshot.as_ref(), relation_type);
                for relation in relation_iterator {
                    relation.unwrap().delete(Arc::get_mut(&mut tx.snapshot).unwrap(), &tx.thing_manager).unwrap();
                }
            }
        }
    })
}

#[apply(generic_step)]
#[step(expr = r"{object_kind} {var} is deleted: {boolean}")]
async fn object_is_deleted(
    context: &mut Context,
    object_kind: params::ObjectKind,
    var: params::Var,
    is_deleted: params::Boolean,
) {
    let object = &context.objects[&var.name].as_ref().unwrap().object;
    object_kind.assert(&object.type_());
    let object_type = object.type_();
    let objects: Vec<Object> = with_read_tx!(context, |tx| {
        tx.thing_manager.get_objects_in(tx.snapshot.as_ref(), object_type).map(|result| result.unwrap()).collect()
    });
    check_boolean!(is_deleted, !objects.contains(object));
}

#[apply(generic_step)]
#[step(expr = r"{object_kind} {var} has type: {type_label}")]
async fn object_has_type(
    context: &mut Context,
    object_kind: params::ObjectKind,
    var: params::Var,
    type_label: params::Label,
) {
    let object_type = context.objects[&var.name].as_ref().unwrap().object.type_();
    object_kind.assert(&object_type);
    with_read_tx!(context, |tx| {
        assert_eq!(object_type.get_label(tx.snapshot.as_ref(), &tx.type_manager).unwrap(), type_label.into_typedb())
    });
}

#[apply(generic_step)]
#[step(expr = r"{var} = {object_kind}\({type_label}\) get instance with key\({type_label}\): {value}")]
async fn object_get_instance_with_value(
    context: &mut Context,
    var: params::Var,
    object_kind: params::ObjectKind,
    object_type_label: params::Label,
    key_type_label: params::Label,
    value: params::Value,
) {
    let Some(key) = get_attribute_by_value(context, key_type_label, value.clone()).unwrap() else {
        // no key - no object
        context.objects.insert(var.name, None);
        return;
    };

    let owner = with_read_tx!(context, |tx| {
        let object_type =
            tx.type_manager.get_object_type(tx.snapshot.as_ref(), &object_type_label.into_typedb()).unwrap().unwrap();
        object_kind.assert(&object_type);
        let mut owners = key.get_owners_by_type(tx.snapshot.as_ref(), &tx.thing_manager, object_type);
        let owner = owners.next().transpose().unwrap().map(|(owner, count)| {
            assert_eq!(count, 1, "found {count} keys owned by the same object, expected 1");
            owner
        });
        assert_matches!(owners.next(), None, "multiple objects found with key {:?}", key);
        owner
    });
    context.objects.insert(var.name, owner.map(|owner| ObjectWithKey::new_with_key(owner, key)));
}

#[apply(generic_step)]
#[step(expr = r"{object_kind}\({type_label}\) get instances {is_empty_or_not}")]
async fn object_instances_is_empty(
    context: &mut Context,
    object_kind: params::ObjectKind,
    type_label: params::Label,
    is_empty_or_not: params::IsEmptyOrNot,
) {
    with_read_tx!(context, |tx| {
        let object_type =
            tx.type_manager.get_object_type(tx.snapshot.as_ref(), &type_label.into_typedb()).unwrap().unwrap();
        object_kind.assert(&object_type);
        is_empty_or_not.check(tx.thing_manager.get_objects_in(tx.snapshot.as_ref(), object_type).next().is_none());
    });
}

#[apply(generic_step)]
#[step(expr = r"{object_kind}\({type_label}\) get instances {contains_or_doesnt}: {var}")]
async fn object_instances_contain(
    context: &mut Context,
    object_kind: params::ObjectKind,
    type_label: params::Label,
    containment: params::ContainsOrDoesnt,
    var: params::Var,
) {
    let object = &context.objects.get(&var.name).expect("no variable {} in context.").as_ref().unwrap().object;
    object_kind.assert(&object.type_());
    with_read_tx!(context, |tx| {
        let object_type =
            tx.type_manager.get_object_type(tx.snapshot.as_ref(), &type_label.into_typedb()).unwrap().unwrap();
        let actuals: Vec<Object> =
            tx.thing_manager.get_objects_in(tx.snapshot.as_ref(), object_type).map(|result| result.unwrap()).collect();
        containment.check(std::slice::from_ref(object), &actuals);
    });
}

#[apply(generic_step)]
#[step(expr = r"attribute {var} get owners {contains_or_doesnt}: {var}")]
async fn attribute_owners_contains(
    context: &mut Context,
    attribute_var: params::Var,
    contains_or_doesnt: params::ContainsOrDoesnt,
    owner_var: params::Var,
) {
    // FIXME Object owner could be relation
    let object = context.objects[&owner_var.name].as_ref().unwrap().object.to_owned();
    let attribute = context.attributes[&attribute_var.name].as_ref().unwrap();
    let actuals = with_read_tx!(context, |tx| {
        attribute
            .get_owners(tx.snapshot.as_ref(), &tx.thing_manager)
            .map(|res| {
                let (attribute, _count) = res.unwrap();
                attribute
            })
            .collect_vec()
    });
    contains_or_doesnt.check(std::slice::from_ref(&object), &actuals)
}
