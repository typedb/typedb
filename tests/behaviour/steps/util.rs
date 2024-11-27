/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
    env,
};

use cucumber::gherkin::Step;
use macro_rules_attribute::apply;

use crate::{generic_step, json::JSON, Context};

pub(crate) fn iter_table(step: &Step) -> impl Iterator<Item = &str> {
    step.table().unwrap().rows.iter().flatten().map(String::as_str)
}

pub(crate) fn iter_table_map(step: &Step) -> impl Iterator<Item = HashMap<&str, &str>> {
    let (keys, rows) = step.table().unwrap().rows.split_first().unwrap();
    rows.iter().map(|row| keys.iter().zip(row).map(|(k, v)| (k.as_str(), v.as_str())).collect())
}

pub fn list_contains_json(list: &[JSON], json: &JSON) -> bool {
    list.iter().any(|list_json| jsons_equal_up_to_reorder(list_json, json))
}

pub(crate) fn parse_json(json: &str) -> JSON {
    fn serde_json_into_answer_json(json: serde_json::Value) -> JSON {
        match json {
            serde_json::Value::Null => JSON::Null,
            serde_json::Value::Bool(bool) => JSON::Boolean(bool),
            serde_json::Value::Number(number) => JSON::Number(number.as_f64().unwrap()),
            serde_json::Value::String(string) => JSON::String(Cow::Owned(string)),
            serde_json::Value::Array(array) => {
                JSON::Array(array.into_iter().map(serde_json_into_answer_json).collect())
            }
            serde_json::Value::Object(object) => {
                JSON::Object(object.into_iter().map(|(k, v)| (Cow::Owned(k), serde_json_into_answer_json(v))).collect())
            }
        }
    }

    match serde_json::from_str(json).map(serde_json_into_answer_json) {
        Ok(result) => result,
        Err(err) => panic!("Could not parse expected json answer: {:?}", err),
    }
}

fn jsons_equal_up_to_reorder(lhs: &JSON, rhs: &JSON) -> bool {
    match (lhs, rhs) {
        (JSON::Object(lhs), JSON::Object(rhs)) => {
            if lhs.len() != rhs.len() {
                return false;
            }
            lhs.iter().all(|(key, lhs_value)| match rhs.get(key) {
                Some(rhs_value) => jsons_equal_up_to_reorder(lhs_value, rhs_value),
                None => false,
            })
        }
        (JSON::Array(lhs), JSON::Array(rhs)) => {
            if lhs.len() != rhs.len() {
                return false;
            }
            let mut rhs_matches = HashSet::new();
            for item in lhs {
                match rhs
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| !rhs_matches.contains(i))
                    .find_map(|(i, rhs_item)| jsons_equal_up_to_reorder(item, rhs_item).then_some(i))
                {
                    Some(idx) => {
                        rhs_matches.insert(idx);
                    }
                    None => return false,
                }
            }
            true
        }
        (JSON::String(lhs), JSON::String(rhs)) => lhs == rhs,
        (&JSON::Number(lhs), &JSON::Number(rhs)) => equals_approximate(lhs, rhs),
        (JSON::Boolean(lhs), JSON::Boolean(rhs)) => lhs == rhs,
        (JSON::Null, JSON::Null) => true,
        _ => false,
    }
}

pub fn equals_approximate(first: f64, second: f64) -> bool {
    const EPS: f64 = 1e-10;
    (first - second).abs() < EPS
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
