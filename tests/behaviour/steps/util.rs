/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, env};

use cucumber::gherkin::Step;
use macro_rules_attribute::apply;

use crate::{Context, generic_step};

pub(crate) fn iter_table(step: &Step) -> impl Iterator<Item = &str> {
    step.table().unwrap().rows.iter().flatten().map(String::as_str)
}

pub(crate) fn iter_table_map(step: &Step) -> impl Iterator<Item = HashMap<&str, &str>> {
    let (keys, rows) = step.table().unwrap().rows.split_first().unwrap();
    rows.iter().map(|row| keys.iter().zip(row).map(|(k, v)| (k.as_str(), v.as_str())).collect())
}

#[apply(generic_step)]
#[step(expr = "set time zone: {word}")]
async fn set_time_zone(_: &mut Context, time_zone: String) {
    unsafe {
        // SAFETY: must ensure that there are no other threads concurrently writing or
        // reading(!) the environment through functions or global variables other than the ones in
        // this module
        // (currently not upheld)
        env::set_var("TZ", time_zone);
    }
}
