/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use macro_rules_attribute::apply;

use crate::{generic_step, params, transaction_context::ActiveTransaction, tx_as_read, tx_as_schema, Context};

#[apply(generic_step)]
#[step(expr = "attribute\\({type_label}\\) set value-type: {value_type}")]
pub async fn attribute_type_set_value_type(context: &mut Context, type_label: params::Label, value_type: params::ValueType){
    let tx = context.transaction().unwrap();
    tx_as_schema! (tx, {
        let attribute_type = tx.type_manager.create_attribute_type(&mut tx.snapshot, &type_label.to_typedb(), false).unwrap();
        attribute_type.set_value_type(&mut tx.snapshot, &tx.type_manager, value_type.to_typedb())
    });
}

