/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::borrow::Cow;

use encoding::value::value::Value;
use query::given_rows::GivenRowEntry;

// Helpers
pub trait IDMaker {
    const ID_TYPE: &'static str;
    fn make_id(id: i64) -> GivenRowEntry;

    fn schema() -> String {
        let id_type = Self::ID_TYPE;
        format!(
            r#"
            define
                attribute id, value {id_type};
                relation r1, relates e1, relates e2;
                entity e1, plays r1:e1, owns id;
                entity e2, plays r1:e2, owns id;
            "#
        )
    }
}

pub struct LongStringIDMaker {}

impl IDMaker for LongStringIDMaker {
    const ID_TYPE: &'static str = "string";
    fn make_id(id: i64) -> GivenRowEntry {
        GivenRowEntry::Value(Value::String(Cow::Owned(format!("id_longer_than_16_bytes__{id}"))))
    }
}

pub struct ShortStringIDMaker {}

impl IDMaker for ShortStringIDMaker {
    const ID_TYPE: &'static str = "string";
    fn make_id(id: i64) -> GivenRowEntry {
        GivenRowEntry::Value(Value::String(Cow::Owned(format!("smol_{id}"))))
    }
}

pub struct IntegerIDMaker {}

impl IDMaker for IntegerIDMaker {
    const ID_TYPE: &'static str = "integer";
    fn make_id(id: i64) -> GivenRowEntry {
        GivenRowEntry::Value(Value::Integer(id))
    }
}
