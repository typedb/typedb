/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use concept::{
    error::ConceptWriteError,
    thing::{attribute::Attribute, entity::Entity, object::Object, ThingAPI},
};
use macro_rules_attribute::apply;

use crate::{
    assert::assert_matches,
    concept::thing::attribute::{attribute_put_instance_with_value_impl, get_attribute_by_value},
    generic_step, params,
    thing_util::EntityWithKey,
    transaction_context::{with_read_tx, with_write_tx},
    Context,
};

fn entity_create_instance_impl(
    context: &mut Context,
    entity_type_label: params::Label,
) -> Result<Entity<'static>, ConceptWriteError> {
    with_write_tx!(context, |tx| {
        let entity_type =
            tx.type_manager.get_entity_type(&tx.snapshot, &entity_type_label.to_typedb()).unwrap().unwrap();
        tx.thing_manager.create_entity(&mut tx.snapshot, entity_type)
    })
}

fn entity_set_has_impl(
    context: &mut Context,
    entity: &Entity<'static>,
    key: &Attribute<'static>,
) -> Result<(), ConceptWriteError> {
    with_write_tx!(context, |tx| entity.set_has_unordered(&mut tx.snapshot, &tx.thing_manager, key.as_reference()))
}

fn entity_unset_has_impl(
    context: &mut Context,
    entity: &Entity<'static>,
    key: &Attribute<'static>,
) -> Result<(), ConceptWriteError> {
    with_write_tx!(context, |tx| entity.unset_has_unordered(&mut tx.snapshot, &tx.thing_manager, key.as_reference()))
}

#[apply(generic_step)]
#[step(expr = r"{var} = entity\({type_label}\) create new instance")]
async fn entity_create_instance_var(context: &mut Context, var: params::Var, entity_type_label: params::Label) {
    let entity = entity_create_instance_impl(context, entity_type_label).unwrap();
    context.entities.insert(var.name, Some(EntityWithKey::new(entity)));
}

#[apply(generic_step)]
#[step(expr = r"{var} = entity\({type_label}\) create new instance with key\({type_label}\): {value}")]
async fn entity_create_instance_with_key_var(
    context: &mut Context,
    var: params::Var,
    entity_type_label: params::Label,
    key_type_label: params::Label,
    value: params::Value,
) {
    let entity = entity_create_instance_impl(context, entity_type_label).unwrap();
    let key = attribute_put_instance_with_value_impl(context, key_type_label, value).unwrap();
    entity_set_has_impl(context, &entity, &key).unwrap();
    context.entities.insert(var.name, Some(EntityWithKey::new_with_key(entity, key)));
}

#[apply(generic_step)]
#[step(expr = r"entity {var} set has: {var}(; ){may_error}")]
async fn entity_set_has(
    context: &mut Context,
    entity_var: params::Var,
    attribute_var: params::Var,
    may_error: params::MayError,
) {
    let entity = context.entities.get(&entity_var.name).unwrap().as_ref().unwrap().entity.to_owned();
    let attribute = context.attributes.get(&attribute_var.name).unwrap().as_ref().unwrap().to_owned();
    may_error.check(&entity_set_has_impl(context, &entity, &attribute));
}

#[apply(generic_step)]
#[step(expr = r"entity {var} unset has: {var}")]
async fn entity_unset_has(context: &mut Context, entity_var: params::Var, attribute_var: params::Var) {
    let entity = context.entities.get(&entity_var.name).unwrap().as_ref().unwrap().entity.to_owned();
    let attribute = context.attributes.get(&attribute_var.name).unwrap().as_ref().unwrap().to_owned();
    entity_unset_has_impl(context, &entity, &attribute).unwrap();
}

#[apply(generic_step)]
#[step(expr = r"entity {var} get has {contains_or_doesnt}: {var}")]
async fn entity_get_has(
    context: &mut Context,
    entity_var: params::Var,
    contains_or_doesnt: params::ContainsOrDoesnt,
    attribute_var: params::Var,
) {
    let entity = context.entities.get(&entity_var.name).unwrap().as_ref().unwrap().entity.to_owned();
    let attribute = context.attributes.get(&attribute_var.name).unwrap().as_ref().unwrap();
    let actuals = with_read_tx!(context, |tx| {
        entity
            .get_has(&tx.snapshot, &tx.thing_manager)
            .collect_cloned_vec(|(attribute, _count)| attribute.into_owned())
            .unwrap()
    });
    contains_or_doesnt.check(std::slice::from_ref(attribute), &actuals);
}

#[apply(generic_step)]
#[step(expr = r"entity {var} get has\({type_label}\) {contains_or_doesnt}: {var}")]
async fn entity_get_has_type(
    context: &mut Context,
    entity_var: params::Var,
    attribute_type_label: params::Label,
    contains_or_doesnt: params::ContainsOrDoesnt,
    attribute_var: params::Var,
) {
    let entity = context.entities[&entity_var.name].as_ref().unwrap().entity.to_owned();
    let attribute = context.attributes[&attribute_var.name].as_ref().unwrap();
    let actuals = with_read_tx!(context, |tx| {
        let attribute_type =
            tx.type_manager.get_attribute_type(&tx.snapshot, &attribute_type_label.to_typedb()).unwrap().unwrap();
        entity
            .get_has_type(&tx.snapshot, &tx.thing_manager, attribute_type)
            .unwrap()
            .collect_cloned_vec(|(attribute, _count)| attribute.into_owned())
            .unwrap()
    });
    contains_or_doesnt.check(std::slice::from_ref(attribute), &actuals);
}

#[apply(generic_step)]
#[step(expr = r"entity {var} exists")]
async fn entity_exists(context: &mut Context, var: params::Var) {
    let entity = context.entities.get(&var.name).expect("no variable {} in context.");
    assert!(entity.is_some(), "variable {} does not exist", var.name);
}

#[apply(generic_step)]
#[step(expr = r"delete entity: {var}")]
async fn delete_entity(context: &mut Context, var: params::Var) {
    let entity = context.entities[&var.name].as_ref().unwrap().entity.clone();
    with_write_tx!(context, |tx| { entity.delete(&mut tx.snapshot, &tx.thing_manager).unwrap() })
}

#[apply(generic_step)]
#[step(expr = r"entity {var} is deleted: {boolean}")]
async fn entity_is_deleted(context: &mut Context, var: params::Var, is_deleted: params::Boolean) {
    let entity = &context.entities[&var.name].as_ref().unwrap().entity;
    let entity_type = entity.type_();
    let entities =
        with_read_tx!(context, |tx| { tx.thing_manager.get_entities_in(&tx.snapshot, entity_type).collect_cloned() });
    is_deleted.check(!entities.contains(entity));
}

#[apply(generic_step)]
#[step(expr = r"entity {var} has type: {type_label}")]
async fn entity_has_type(context: &mut Context, var: params::Var, type_label: params::Label) {
    let entity_type = context.entities[&var.name].as_ref().unwrap().entity.type_();
    with_read_tx!(context, |tx| {
        assert_eq!(entity_type.get_label(&tx.snapshot, &tx.type_manager).unwrap(), type_label.to_typedb())
    });
}

#[apply(generic_step)]
#[step(expr = r"{var} = entity\({type_label}\) get instance with key\({type_label}\): {value}")]
async fn entity_get_instance_with_value(
    context: &mut Context,
    var: params::Var,
    entity_type_label: params::Label,
    key_type_label: params::Label,
    value: params::Value,
) {
    let key = get_attribute_by_value(context, key_type_label, value).unwrap().unwrap();
    let owner = with_read_tx!(context, |tx| {
        let entity_type =
            tx.type_manager.get_entity_type(&tx.snapshot, &entity_type_label.to_typedb()).unwrap().unwrap();
        let mut owners = key.get_owners_by_type(&tx.snapshot, &tx.thing_manager, entity_type);
        let (owner, count) = owners.next().unwrap().unwrap();
        let owner = owner.unwrap_entity().into_owned();
        assert_eq!(count, 1, "found {count} keys owned by the same entity, expected 1");
        assert!(owners.next().is_none(), "multiple entities found with key {:?}", key);
        owner
    });
    context.entities.insert(var.name, Some(EntityWithKey::new_with_key(owner, key)));
}

#[apply(generic_step)]
#[step(expr = r"entity\({type_label}\) get instances is empty")]
async fn entity_instances_is_empty(context: &mut Context, type_label: params::Label) {
    with_read_tx!(context, |tx| {
        let entity_type = tx.type_manager.get_entity_type(&tx.snapshot, &type_label.to_typedb()).unwrap().unwrap();
        assert_matches!(tx.thing_manager.get_entities_in(&tx.snapshot, entity_type).next(), None);
    });
}

#[apply(generic_step)]
#[step(expr = r"entity\({type_label}\) get instances {contains_or_doesnt}: {var}")]
async fn entity_instances_contain(
    context: &mut Context,
    type_label: params::Label,
    containment: params::ContainsOrDoesnt,
    var: params::Var,
) {
    let entity = &context.entities.get(&var.name).expect("no variable {} in context.").as_ref().unwrap().entity;
    with_read_tx!(context, |tx| {
        let entity_type = tx.type_manager.get_entity_type(&tx.snapshot, &type_label.to_typedb()).unwrap().unwrap();
        let actuals = tx.thing_manager.get_entities_in(&tx.snapshot, entity_type).collect_cloned();
        containment.check(std::slice::from_ref(entity), &actuals);
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
    let entity = context.entities.get(&owner_var.name).unwrap().as_ref().unwrap().entity.to_owned();
    let attribute = context.attributes.get(&attribute_var.name).unwrap().as_ref().unwrap();
    let actuals = with_read_tx!(context, |tx| {
        attribute
            .get_owners(&tx.snapshot, &tx.thing_manager)
            .collect_cloned_vec(|(owner, _count)| owner.into_owned())
            .unwrap()
    });
    contains_or_doesnt.check(std::slice::from_ref(&Object::Entity(entity)), &actuals)
}
