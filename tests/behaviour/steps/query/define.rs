/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use cucumber::gherkin::Step;
use macro_rules_attribute::apply;
use crate::{
    generic_step,
    params::{self, check_boolean},
    transaction_context::{with_read_tx, with_write_tx},
    Context,
};
use query::query_manager::QueryManager;
use crate::params::MayError;
use crate::transaction_context::with_schema_tx;


#[apply(generic_step)]
#[step(expr = r"typeql define{may_error}")]
async fn attribute_put_instance_with_value(
    context: &mut Context,
    may_error: MayError,
    step: &Step,
) {
    let define_typeql = step.docstring.as_ref().unwrap().as_str();
    with_schema_tx!(context, |tx| {
        let result = QueryManager::new().execute(&mut tx.snapshot, &tx.type_manager, define_typeql);
        assert_eq!(may_error.expects_error(), result.is_err(), "{:?}", result);
    });

}
