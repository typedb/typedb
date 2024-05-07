/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use cucumber::gherkin::Step;
use macro_rules_attribute::apply;
use crate::{with_type, generic_step, tx_as_read, tx_as_write, tx_as_schema, Context,
            params::{Boolean, MayError, ContainsOrDoesnt, RootLabel, Label},
            transaction_context::{ActiveTransaction},
            util};
use ::concept::type_::{
    TypeAPI, OwnerAPI, PlayerAPI,
    object_type::ObjectType,
    Ordering
};
use encoding::graph::type_::Kind;
use crate::params::Annotation;

#[macro_export]
macro_rules! with_type {
    ($tx:ident, $kind:ident, $label:ident, $assign_to:ident, $block:block) => {
        match $kind.to_typedb() {
            Kind::Attribute => {
                let $assign_to = $tx.type_manager.get_attribute_type(&$tx.snapshot, &$label.to_typedb()).unwrap().unwrap();
                $block
            },
            Kind::Entity => {
                let $assign_to = $tx.type_manager.get_entity_type(&$tx.snapshot, &$label.to_typedb()).unwrap().unwrap();
                $block
            },
            Kind::Relation => {
                let $assign_to = $tx.type_manager.get_relation_type(&$tx.snapshot, &$label.to_typedb()).unwrap().unwrap();
                $block
            },
            Kind::Role => unreachable!(),
        };
    };
}

fn get_as_object_type(tx: &ActiveTransaction, kind: Kind, label: &Label) -> ObjectType<'static> {
    tx_as_read! (tx, {
        match kind {
            Kind::Entity => {
                let type_ = tx.type_manager.get_entity_type(&tx.snapshot, &label.to_typedb()).unwrap().unwrap();
                return ObjectType::Entity(type_)
            },
            Kind::Relation => {
                let type_ = tx.type_manager.get_relation_type(&tx.snapshot, &label.to_typedb()).unwrap().unwrap();
                return ObjectType::Relation(type_)
            },
            _ => unreachable!("Attribute type as ObjectType is deprecated."),
        };
    })
}

#[apply(generic_step)]
#[step(expr = "put {root_label} type: {type_label}")]
pub async fn type_put(context: &mut Context, root_label: RootLabel, type_label: Label) {
    let tx = context.transaction().unwrap();
    tx_as_schema!(tx, {
        match root_label.to_typedb() {
            Kind::Entity => { tx.type_manager.create_entity_type(&mut tx.snapshot, &type_label.to_typedb(), false).unwrap(); },
            Kind::Relation => { tx.type_manager.create_relation_type(&mut tx.snapshot, &type_label.to_typedb(), false).unwrap(); },
            Kind::Attribute => {tx.type_manager.create_attribute_type(&mut tx.snapshot, &type_label.to_typedb(), false).unwrap(); }
            Kind::Role => unreachable!(),
        }
    });
}

#[apply(generic_step)]
#[step(expr = "delete {root_label} type: {type_label}(; ){may_error}")]
pub async fn type_delete(context: &mut Context, root_label: RootLabel, type_label: Label, may_error: MayError) {
    let tx = context.transaction().unwrap();
    tx_as_schema! (tx, {
        with_type! (tx, root_label, type_label, type_, {
            let res = type_.delete(&mut tx.snapshot, &tx.type_manager);
            may_error.check(&res);
        });
    });
}
//
#[apply(generic_step)]
#[step(expr = "{root_label}\\({type_label}\\) exists: {boolean}")]
pub async fn type_exists(context: &mut Context, root_label: RootLabel, type_label: Label, exists: Boolean) {
    let tx = context.transaction().unwrap();
    tx_as_read! (tx, {
        match root_label.to_typedb() {
            Kind::Attribute => {
                let type_ = tx.type_manager.get_attribute_type(&tx.snapshot, &type_label.to_typedb()).unwrap();
                exists.check(type_.is_some());
            },
            Kind::Entity => {
                let type_ = tx.type_manager.get_entity_type(&tx.snapshot, &type_label.to_typedb()).unwrap();
                exists.check(type_.is_some());
            },
            Kind::Relation => {
                let type_ = tx.type_manager.get_relation_type(&tx.snapshot, &type_label.to_typedb()).unwrap();
                exists.check(type_.is_some());
            },
            Kind::Role => {
                let type_ = tx.type_manager.get_role_type(&tx.snapshot, &type_label.to_typedb()).unwrap();
                exists.check(type_.is_some());
            },
        };
    });
}

#[apply(generic_step)]
#[step(expr = "{root_label}\\({type_label}\\) set label: {type_label}")]
pub async fn type_set_label(context: &mut Context, root_label: RootLabel, type_label: Label, to_label: Label) {
    let tx = context.transaction().unwrap();
    tx_as_schema! (tx, {
        with_type!(tx, root_label, type_label, type_, {
            type_.set_label(&mut tx.snapshot, &tx.type_manager, &to_label.to_typedb()).unwrap()
        });
    });
}

#[apply(generic_step)]
#[step(expr = "{root_label}\\({type_label}\\) get label: {type_label}")]
pub async fn type_get_label(context: &mut Context, root_label: RootLabel, type_label: Label, expected: Label) {
    let tx = context.transaction().unwrap();
    tx_as_read! (tx, {
        with_type!(tx, root_label, type_label, type_, {
            let actual_label = type_.get_label(&tx.snapshot, &tx.type_manager);
            assert_eq!(expected.to_typedb().scoped_name(), actual_label.unwrap().scoped_name());
        });
    });
}

#[apply(generic_step)]
#[step(expr = "{root_label}\\({type_label}\\) set annotation: {annotation}(; ){may_error}")]
pub async fn type_set_annotation(context: &mut Context, root_label: RootLabel, type_label: Label, annotation: Annotation, may_error: MayError) {
    let tx = context.transaction().unwrap();
    tx_as_write! (tx, {
        with_type!(tx, root_label, type_label, type_, {
            let res = type_.set_annotation(&mut tx.snapshot, &tx.type_manager,  annotation.to_typedb().into());
            may_error.check(&res);
        });
    });
}

#[apply(generic_step)]
#[step(expr = "{root_label}\\({type_label}\\) get annotations {contains_or_doesnt}: {annotation}")]
pub async fn type_is_abstract(context: &mut Context, root_label: RootLabel, type_label: Label, contains_or_doesnt: ContainsOrDoesnt, annotation: Annotation) {
    let tx = context.transaction().unwrap();
    tx_as_read! (tx, {
        with_type!(tx, root_label, type_label, type_, {
            let actual_contains = type_.get_annotations(&tx.snapshot, &tx.type_manager).unwrap().contains(&annotation.to_typedb().into());
            assert_eq!(contains_or_doesnt.expected_contains(), actual_contains);
        });
    });
}

#[apply(generic_step)]
#[step(expr = "{root_label}\\({type_label}\\) set supertype: {type_label}(; ){may_error}")]
pub async fn type_set_supertype(context: &mut Context, root_label: RootLabel, type_label: Label, supertype_label: Label, may_error: MayError) {
    let tx = context.transaction().unwrap();
    tx_as_schema! (tx, {
        match root_label.to_typedb() {
            Kind::Attribute => {
                let thistype = tx.type_manager.get_attribute_type(&tx.snapshot, &type_label.to_typedb()).unwrap().unwrap();
                let supertype = tx.type_manager.get_attribute_type(&tx.snapshot, &supertype_label.to_typedb()).unwrap().unwrap();
                let res = thistype.set_supertype(&mut tx.snapshot, &tx.type_manager, supertype);
                may_error.check(&res);
            },
            Kind::Entity => {
                let thistype = tx.type_manager.get_entity_type(&tx.snapshot, &type_label.to_typedb()).unwrap().unwrap();
                let supertype = tx.type_manager.get_entity_type(&tx.snapshot, &supertype_label.to_typedb()).unwrap().unwrap();
                let res = thistype.set_supertype(&mut tx.snapshot, &tx.type_manager, supertype);
                may_error.check(&res);
            },
            Kind::Relation => {
                let thistype = tx.type_manager.get_relation_type(&tx.snapshot, &type_label.to_typedb()).unwrap().unwrap();
                let supertype = tx.type_manager.get_relation_type(&tx.snapshot, &supertype_label.to_typedb()).unwrap().unwrap();
                let res = thistype.set_supertype(&mut tx.snapshot, &tx.type_manager, supertype);
                may_error.check(&res);
            },
            Kind::Role => {
                let thistype = tx.type_manager.get_role_type(&tx.snapshot, &type_label.to_typedb()).unwrap().unwrap();
                let supertype = tx.type_manager.get_role_type(&tx.snapshot, &supertype_label.to_typedb()).unwrap().unwrap();
                let res = thistype.set_supertype(&mut tx.snapshot, &tx.type_manager, supertype);
                may_error.check(&res);
            },
        };
    });
}

#[apply(generic_step)]
#[step(expr = "{root_label}\\({type_label}\\) get supertype: {type_label}")]
pub async fn type_get_supertype(context: &mut Context, root_label: RootLabel, type_label: Label, supertype_label: Label) {
    let tx = context.transaction().unwrap();
    tx_as_read! (tx, {
        with_type!(tx, root_label, type_label, type_, {
            let supertype = type_.get_supertype(&tx.snapshot, &tx.type_manager).unwrap().unwrap();
            assert_eq!(supertype_label.to_typedb().scoped_name(), supertype.get_label(&tx.snapshot, &tx.type_manager).unwrap().scoped_name())
        });
    });
}

#[apply(generic_step)]
#[step(expr = "{root_label}\\({type_label}\\) get supertypes {contains_or_doesnt}:")]
pub async fn get_supertypes_contain(context: &mut Context, root_label: RootLabel, type_label: Label, contains: ContainsOrDoesnt, step: &Step) {
    let expected_labels: Vec<String> = util::iter_table(step).map(|str| { str.to_string() }).collect::<Vec<String>>();
    let tx = context.transaction().unwrap();
    tx_as_read! (tx, {
        with_type!(tx, root_label, type_label, type_, {
            let supertype_labels: Vec<String> = type_.get_supertypes(&tx.snapshot, &tx.type_manager)
            .unwrap().iter().map(|supertype| { supertype.get_label(&tx.snapshot, &tx.type_manager).unwrap().scoped_name().as_str().to_string() })
            .collect::<Vec<String>>();
            contains.check(expected_labels, supertype_labels.into_iter().collect());
        });
    });
}

#[apply(generic_step)]
#[step(expr = "{root_label}\\({type_label}\\) get subtypes {contains_or_doesnt}:")]
pub async fn get_subtypes_contain(context: &mut Context, root_label: RootLabel, type_label: Label, contains: ContainsOrDoesnt, step: &Step) {
    let expected_labels: Vec<String> = util::iter_table(step).map(|str| { str.to_string() }).collect::<Vec<String>>();
    let tx = context.transaction().unwrap();
    tx_as_read! (tx, {
        with_type!(tx, root_label, type_label, type_, {
            let subtype_labels: Vec<String> = type_.get_subtypes(&tx.snapshot, &tx.type_manager)
            .unwrap().iter().map(|subtype| { subtype.get_label(&tx.snapshot, &tx.type_manager).unwrap().scoped_name().as_str().to_string() })
            .collect::<Vec<String>>();
            contains.check(expected_labels, subtype_labels.into_iter().collect());
        });
    });
}

// Owns
#[apply(generic_step)]
#[step(expr = "{root_label}\\({type_label}\\) set owns: {type_label}")]
pub async fn set_owns(context: &mut Context, root_label: RootLabel, type_label: Label, attribute_type_label: Label) {
    let tx = context.transaction().unwrap();
    let object_type = get_as_object_type(tx, root_label.to_typedb(), &type_label);
    tx_as_schema! (tx, {
        let attr_type = tx.type_manager.get_attribute_type(&tx.snapshot, &attribute_type_label.to_typedb()).unwrap().unwrap();
        object_type.set_owns(&mut tx.snapshot, &tx.type_manager, attr_type, Ordering::Unordered).unwrap();
    });
}
#[apply(generic_step)]
#[step(expr = "{root_label}\\({type_label}\\) unset owns: {type_label}")]
pub async fn unset_owns(context: &mut Context, root_label: RootLabel, type_label: Label, attribute_type_label: Label) {
    let tx = context.transaction().unwrap();
    let object_type = get_as_object_type(tx, root_label.to_typedb(), &type_label);
    tx_as_schema! (tx, {
        let attr_type = tx.type_manager.get_attribute_type(&tx.snapshot, &attribute_type_label.to_typedb()).unwrap().unwrap();
        object_type.delete_owns(&mut tx.snapshot, &tx.type_manager, attr_type).unwrap();
    });
}

#[apply(generic_step)]
#[step(expr = "{root_label}\\({type_label}\\) get owns: {type_label}; set override: {type_label}")]
pub async fn get_owns_set_override(context: &mut Context, root_label: RootLabel, type_label: Label, attr_type_label: Label, overridden_type_label: Label) {
    let tx = context.transaction().unwrap();
    let object_type = get_as_object_type(tx, root_label.to_typedb(), &type_label);
    tx_as_schema! (tx, {
        println!("Will get attribute type?");
        let attr_type = tx.type_manager.get_attribute_type(&tx.snapshot, &attr_type_label.to_typedb()).unwrap().unwrap();
        println!("Did get attribute type; Will get owns?");
        let owns = object_type.get_owns_attribute(&tx.snapshot, &tx.type_manager, attr_type).unwrap().unwrap();
        println!("Did get owns; Will get overridden attribute type?");
        let overridden_attr_type = tx.type_manager.get_attribute_type(&tx.snapshot, &overridden_type_label.to_typedb()).unwrap().unwrap();
        println!("Did get overridden attribute type; Will get overridden owns?");
        let overridden_owns = object_type.get_owns_attribute_transitive(&tx.snapshot, &tx.type_manager, overridden_attr_type).unwrap().unwrap();
        owns.set_override(&mut tx.snapshot, &tx.type_manager, overridden_owns);
    });
}

#[apply(generic_step)]
#[step(expr = "{root_label}\\({type_label}\\) get owns: {type_label}, set annotation: {annotation}")]
pub async fn get_owns_set_annotation(context: &mut Context, root_label: RootLabel, type_label: Label, attr_type_label: Label, annotation: Annotation) {
    let tx = context.transaction().unwrap();
    let object_type = get_as_object_type(tx, root_label.to_typedb(), &type_label);
    tx_as_schema! (tx, {
        let attr_type = tx.type_manager.get_attribute_type(&tx.snapshot, &attr_type_label.to_typedb()).unwrap().unwrap();
        let owns = object_type.get_owns_attribute(&tx.snapshot, &tx.type_manager, attr_type).unwrap().unwrap();
        owns.set_annotation(&mut tx.snapshot, &tx.type_manager, annotation.to_typedb().into());
    });
}

#[apply(generic_step)]
#[step(expr = "{root_label}\\({type_label}\\) get owns {contains_or_doesnt}:")]
pub async fn get_owns_contain(context: &mut Context, root_label: RootLabel, type_label: Label, contains: ContainsOrDoesnt, step: &Step) {
    let expected_labels: Vec<String> = util::iter_table(step).map(|str| { str.to_string() }).collect::<Vec<String>>();
    let tx = context.transaction().unwrap();
    let object_type = get_as_object_type(tx, root_label.to_typedb(), &type_label);
    tx_as_read! (tx, {
        let actual_labels = object_type.get_owns(&tx.snapshot, &tx.type_manager).unwrap().iter().map(|owns| {
            owns.attribute().get_label(&tx.snapshot, &tx.type_manager).unwrap().scoped_name().as_str().to_string()
        }).collect::<Vec<String>>();
        contains.check(expected_labels, actual_labels);
    });
}

#[apply(generic_step)]
#[step(expr = "{root_label}\\({type_label}\\) get owns overridden\\({type_label}\\) exists: {boolean}")]
pub async fn get_owns_overridden_exists(context: &mut Context, root_label: RootLabel, type_label: Label, attr_type_label: Label, _exists: Boolean) {
    let tx = context.transaction().unwrap();
    let _object_type = get_as_object_type(tx, root_label.to_typedb(), &type_label);
    tx_as_read! (tx, {
        let _attr_type_opt = tx.type_manager.get_attribute_type(&tx.snapshot, &attr_type_label.to_typedb()).unwrap();
        todo!("Overridden owns");
        // let overriden_type_opt = todo!();
        // exists.check(overriden_type_opt.is_some());
    });
}

#[apply(generic_step)]
#[step(expr = "{root_label}\\({type_label}\\) get owns overridden\\({type_label}\\) get label: {type_label}")]
pub async fn get_owns_overridden_get_label(context: &mut Context, root_label: RootLabel, type_label: Label, attr_type_label: Label, expected_overridden: Label) {
    let tx = context.transaction().unwrap();
    let owner = get_as_object_type(tx, root_label.to_typedb(), &type_label);
    tx_as_read! (tx, {
        let attr_type = tx.type_manager.get_attribute_type(&tx.snapshot, &attr_type_label.to_typedb()).unwrap().unwrap();
        // TODO: This doesn't include ones that are already hidden. So you can't override one that's already overridden.
        let owns = owner.get_owns_attribute_transitive(&tx.snapshot, &tx.type_manager, attr_type).unwrap().unwrap();
        let overridden_owns_opt = owns.get_override(&tx.snapshot, &tx.type_manager).unwrap();
        let overridden_owns = overridden_owns_opt.as_ref().unwrap();
        let actual_type_label = overridden_owns.attribute().get_label(&tx.snapshot, &tx.type_manager).unwrap().scoped_name().as_str().to_string();
        assert_eq!(expected_overridden.to_typedb().scoped_name().as_str().to_string(), actual_type_label);
    });
}

// Plays
#[apply(generic_step)]
#[step(expr = "{root_label}\\({type_label}\\) set plays role: {type_label}(; ){may_error}")]
pub async fn set_plays_role(context: &mut Context, root_label: RootLabel, type_label: Label, role_label: Label, may_error: MayError) {
    let tx = context.transaction().unwrap();
    let object_type = get_as_object_type(tx, root_label.to_typedb(), &type_label);
    tx_as_schema! (tx, {
        let role_type = tx.type_manager.get_role_type(&tx.snapshot, &role_label.to_typedb()).unwrap().unwrap();
        let res = object_type.set_plays(&mut tx.snapshot, &tx.type_manager, role_type);
        may_error.check(&res);
    });
}

#[apply(generic_step)]
#[step(expr = "{root_label}\\({type_label}\\) unset plays role: {type_label}(; ){may_error}")]
pub async fn unset_plays_role(context: &mut Context, root_label: RootLabel, type_label: Label, role_label: Label, may_error: MayError) {
    let tx = context.transaction().unwrap();
    let object_type = get_as_object_type(tx, root_label.to_typedb(), &type_label);
    tx_as_schema! (tx, {
        let role_type = tx.type_manager.get_role_type(&tx.snapshot, &role_label.to_typedb()).unwrap().unwrap();
        let res = object_type.delete_plays(&mut tx.snapshot, &tx.type_manager, role_type);
        may_error.check(&res);
    });
}

#[apply(generic_step)]
#[step(expr = "{root_label}\\({type_label}\\) get plays role: {type_label}; set override: {type_label}(; ){may_error}")]
pub async fn get_plays_set_override(context: &mut Context, root_label: RootLabel, type_label: Label, role_label: Label, overridden_role_label: Label, may_error: MayError) {
    let tx = context.transaction().unwrap();
    let object_type = get_as_object_type(tx, root_label.to_typedb(), &type_label);
    tx_as_schema! (tx, {
        let role_type = tx.type_manager.get_role_type(&tx.snapshot, &role_label.to_typedb()).unwrap().unwrap();
        let overridden_role_type = tx.type_manager.get_role_type(&tx.snapshot, &overridden_role_label.to_typedb()).unwrap().unwrap();
        let plays = object_type.get_plays_role(&mut tx.snapshot, &tx.type_manager, role_type).unwrap();
        todo!("Override plays");
    });
}

#[apply(generic_step)]
#[step(expr = "{root_label}\\({type_label}\\) get plays roles {contains_or_doesnt}:(; ){may_error}")]
pub async fn get_plays_roles_contain(context: &mut Context, root_label: RootLabel, type_label: Label, contains: ContainsOrDoesnt, may_error: MayError, step: &Step) {
    let expected_labels: Vec<String> = util::iter_table(step).map(|str| { str.to_string() }).collect::<Vec<String>>();
    let tx = context.transaction().unwrap();
    let object_type = get_as_object_type(tx, root_label.to_typedb(), &type_label);
    tx_as_read! (tx, {
        let actual_labels = object_type.get_plays(&tx.snapshot, &tx.type_manager).unwrap().iter().map(|plays| {
            plays.role().get_label(&tx.snapshot, &tx.type_manager).unwrap().scoped_name().as_str().to_string()
        }).collect::<Vec<String>>();
        println!("Actual roles played: {:?}", actual_labels);
        contains.check(expected_labels, actual_labels);
    });
}


// // #[apply(generic_step)]
// // #[step(expr = "{root_label}\\({type_label}\\) get owns explicit attribute types contain:")]
// // pub async fn TODO(context: &mut Context, root_label: RootLabel, type_label: Label, ...) { todo!(); }
// //
// // #[apply(generic_step)]
// // #[step(expr = "{root_label}\\({type_label}\\) get owns explicit attribute types do not contain:")]
// // pub async fn TODO(context: &mut Context, root_label: RootLabel, type_label: Label, ...) { todo!(); }
// //
// // #[apply(generic_step)]
// // #[step(expr = "{root_label}\\({type_label}\\) get playing roles contain:")]
// // pub async fn TODO(context: &mut Context, root_label: RootLabel, type_label: Label, ...) { todo!(); }
// //
// // #[apply(generic_step)]
// // #[step(expr = "{root_label}\\({type_label}\\) get playing roles do not contain:")]
// // pub async fn TODO(context: &mut Context, root_label: RootLabel, type_label: Label, ...) { todo!(); }
// //
// // #[apply(generic_step)]
// // #[step(expr = "{root_label}\\({type_label}\\) get playing roles explicit contain:")]
// // pub async fn TODO(context: &mut Context, root_label: RootLabel, type_label: Label, ...) { todo!(); }
// //
// // #[apply(generic_step)]
// // #[step(expr = "{root_label}\\({type_label}\\) get playing roles explicit do not contain:")]
// // pub async fn TODO(context: &mut Context, root_label: RootLabel, type_label: Label, ...) { todo!(); }
// //
//
//
// // TODO: thing type root - Deprecated?
//
// // #[apply(generic_step)]
// // #[step(expr = "thing type root get supertypes contain:")]
// // pub async fn TODO(context: &mut Context, root_label: RootLabel, type_label: Label, ...) { todo!(); }
// //
// // #[apply(generic_step)]
// // #[step(expr = "thing type root get supertypes do not contain:")]
// // pub async fn TODO(context: &mut Context, root_label: RootLabel, type_label: Label, ...) { todo!(); }
// //
// // #[apply(generic_step)]
// // #[step(expr = "thing type root get subtypes contain:")]
// // pub async fn TODO(context: &mut Context, root_label: RootLabel, type_label: Label, ...) { todo!(); }
// //
// //
// // #[apply(generic_step)]
// // #[step(expr = "thing type root get subtypes do not contain:")]
// // pub async fn TODO(context: &mut Context, root_label: RootLabel, type_label: Label, ...) { todo!(); }
