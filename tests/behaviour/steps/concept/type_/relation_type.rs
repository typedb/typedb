/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use macro_rules_attribute::apply;
use crate::{generic_step, tx_as_schema, Context,
            params::{MayError,  Label},
            transaction_context::{ActiveTransaction}};
use ::concept::type_::{
    Ordering
};
use crate::params::Annotation;

#[apply(generic_step)]
#[step(expr = "relation\\({type_label}\\) create role: {type_label}(; ){may_error}")]
pub async fn relation_type_create_role(context: &mut Context, type_label: Label, role_label: Label, may_error: MayError) {
    let tx = context.transaction().unwrap();
    tx_as_schema!(tx, {
        let relation_type = tx.type_manager.get_relation_type(&tx.snapshot, &type_label.to_typedb()).unwrap().unwrap();
        let res = relation_type.create_relates(&mut tx.snapshot, &tx.type_manager, role_label.to_typedb().name().as_str(), Ordering::Unordered);
        may_error.check(&res);
    });
}

#[apply(generic_step)]
#[step(expr = "relation\\({type_label}\\) get role: {type_label}; set override: {type_label}(; ){may_error}")]
pub async fn relation_type_override_role(context: &mut Context, type_label: Label, role_label: Label, overridden_label: Label,  may_error: MayError) {
    let tx = context.transaction().unwrap();
    tx_as_schema!(tx, {
        let relation_type = tx.type_manager.get_relation_type(&tx.snapshot, &type_label.to_typedb()).unwrap().unwrap();
        let relates = relation_type.get_relates_role(&mut tx.snapshot, &tx.type_manager, role_label.to_typedb().name().as_str()).unwrap().unwrap();
        let overridden_relates = relation_type.get_relates_role(&mut tx.snapshot, &tx.type_manager, overridden_label.to_typedb().name().as_str()).unwrap().unwrap();
        // TODO: Is it ok to just set supertype here?
        let res = relates.role().set_supertype(&mut tx.snapshot, &tx.type_manager, overridden_relates.role());
        may_error.check(&res);
    });
}