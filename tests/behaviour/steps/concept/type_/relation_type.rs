/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use ::concept::type_::Ordering;
use macro_rules_attribute::apply;

use crate::{
    generic_step,
    params::{Label, MayError},
    transaction_context::with_schema_tx,
    Context,
};

#[apply(generic_step)]
#[step(expr = "relation\\({type_label}\\) create role: {type_label}(; ){may_error}")]
pub async fn relation_type_create_role(
    context: &mut Context,
    type_label: Label,
    role_label: Label,
    may_error: MayError,
) {
    with_schema_tx!(context, |tx| {
        let relation_type = tx.type_manager.get_relation_type(&tx.snapshot, &type_label.to_typedb()).unwrap().unwrap();
        let res = relation_type.create_relates(
            &mut tx.snapshot,
            &tx.type_manager,
            role_label.to_typedb().name().as_str(),
            Ordering::Unordered,
        );
        may_error.check(&res);
    });
}

#[apply(generic_step)]
#[step(expr = "relation\\({type_label}\\) get role\\({type_label}\\); set override: {type_label}(; ){may_error}")]
pub async fn relation_type_override_role(
    context: &mut Context,
    type_label: Label,
    role_label: Label,
    overridden_label: Label,
    may_error: MayError,
) {
    with_schema_tx!(context, |tx| {
        let relation_type = tx.type_manager.get_relation_type(&tx.snapshot, &type_label.to_typedb()).unwrap().unwrap();
        let relates = relation_type
            .get_relates_role(&mut tx.snapshot, &tx.type_manager, role_label.to_typedb().name().as_str())
            .unwrap()
            .unwrap();
        let relation_supertype =  relation_type.get_supertype(&tx.snapshot, &tx.type_manager).unwrap().unwrap();

        {
        let overridden_relates_opt = relation_supertype
            .get_relates_role_transitive(&mut tx.snapshot, &tx.type_manager, overridden_label.to_typedb().name().as_str())
            .unwrap();
        debug_assert!(overridden_relates_opt.is_some(), "Ah, get_role_transitive returned None");
        }

        let overridden_relates = relation_supertype
            .get_relates_role(&mut tx.snapshot, &tx.type_manager, overridden_label.to_typedb().name().as_str()) // MUST get_relates_role_transitive
            .unwrap().unwrap();

        // TODO: Is it ok to just set supertype here?
        let res = relates.role().set_supertype(&mut tx.snapshot, &tx.type_manager, overridden_relates.role());
        may_error.check(&res);
    });
}
