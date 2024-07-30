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


#[apply(generic_step)]
#[step(expr = r"typeql define")]
async fn attribute_put_instance_with_value(
    context: &mut Context,
    step: &Step,
) {
    todo!()
}
