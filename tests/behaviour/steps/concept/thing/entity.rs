/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use concept::{error::ConceptWriteError, thing::attribute::Attribute};
use macro_rules_attribute::apply;

use crate::{
    concept::thing::attribute::{attribute_put_instance_with_value_impl, get_attribute_by_value},
    generic_step, params,
    thing_util::EntityWithKey,
    transaction_context::{with_read_tx, with_write_tx},
    Context,
};

#[apply(generic_step)]
#[step(expr = r"{var} = entity\({type_label}\) create new instance with key\({type_label}\): {value}")]
async fn entity_create_instance_with_key_var(
    context: &mut Context,
    var: params::Var,
    entity_type_label: params::Label,
    key_type_label: params::Label,
    value: params::Value,
) {
    let key = attribute_put_instance_with_value_impl(context, key_type_label, value).unwrap();
    let entity = with_write_tx!(context, |tx| {
        let entity_type =
            tx.type_manager.get_entity_type(&tx.snapshot, &entity_type_label.to_typedb()).unwrap().unwrap();
        let entity = tx.thing_manager.create_entity(&mut tx.snapshot, entity_type).unwrap();
        entity.set_has_unordered(&mut tx.snapshot, &tx.thing_manager, key.as_reference()).unwrap();
        entity
    });
    context.entities.insert(var.name, Some(EntityWithKey::new(entity, key)));
}

#[apply(generic_step)]
#[step(expr = r"entity {var} exists")]
async fn entity_exists(context: &mut Context, var: params::Var) {
    let entity = context.entities.get(&var.name).expect("no variable {} in context.");
    assert!(entity.is_some(), "variable {} does not exist", var.name);
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
    context.entities.insert(var.name, Some(EntityWithKey::new(owner, key)));
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
