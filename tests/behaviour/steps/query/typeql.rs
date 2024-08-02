/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use cucumber::gherkin::Step;
use macro_rules_attribute::apply;
use query::query_manager::QueryManager;

use crate::{
    generic_step,
    params::{self, check_boolean, MayError},
    transaction_context::{with_read_tx, with_schema_tx, with_write_tx},
    Context,
};

#[apply(generic_step)]
#[step(expr = r"typeql define{may_error}")]
async fn typeql_define(context: &mut Context, may_error: MayError, step: &Step) {
    let typeql_define = step.docstring.as_ref().unwrap().as_str();
    with_schema_tx!(context, |tx| {
        let result = QueryManager::new().execute(&mut tx.snapshot, &tx.type_manager, typeql_define);
        assert_eq!(may_error.expects_error(), result.is_err(), "{:?}", result);
    });
}

#[apply(generic_step)]
#[step(expr = r"typeql insert{may_error}")]
async fn typeql_insert(context: &mut Context, may_error: MayError, step: &Step) {
    let typeql_insert = step.docstring.as_ref().unwrap().as_str();
    with_write_tx!(context, |tx| {
        println!("TypeQL insert is ignored");
    });
}

#[apply(generic_step)]
#[step(expr = r"get answers of typeql get")]
async fn typeql_get(context: &mut Context, step: &Step) {
    let typeql_get = step.docstring.as_ref().unwrap().as_str();
    with_read_tx!(context, |tx| {
        // Can't read_tx because execute always takes a mut snapshot
        // let result = QueryManager::new().execute(&mut tx.snapshot, &tx.type_manager, typeql_get);
        println!("TypeQL get is ignored!")
    });
}

#[apply(generic_step)]
#[step(expr = r"uniquely identify answer concepts")]
async fn uniquely_identify_answer_concepts(context: &mut Context, step: &Step) {
    let table = step.table.as_ref().unwrap();
    with_read_tx!(context, |tx| {
        // Can't read_tx because execute always takes a mut snapshot
        println!("Uniquely identify is ignored!")
    });
}
