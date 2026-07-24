/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::borrow::Cow;

use encoding::value::value::Value;
use query::given_rows::GivenRowEntry;
use rand::{Rng, SeedableRng, prelude::SmallRng, thread_rng};

pub struct RandomDataGen {
    rng: SmallRng,
}

impl RandomDataGen {
    pub fn new() -> Self {
        let seed = thread_rng().r#gen();
        Self { rng: SmallRng::seed_from_u64(seed) }
    }

    pub fn string(&mut self, len: usize) -> String {
        (0..len).map(|_| self.rng.sample(rand::distributions::Alphanumeric) as char).collect()
    }

    pub fn integer_in(&mut self, min: i64, max: i64) -> i64 {
        self.rng.gen_range(min..=max)
    }

    pub fn integer(&mut self) -> i64 {
        self.integer_in(i64::MIN, i64::MAX)
    }

    pub fn entry_string(&mut self, len: usize) -> GivenRowEntry {
        GivenRowEntry::Value(Value::String(Cow::Owned(self.string(len))))
    }

    pub fn entry_integer_in(&mut self, min: i64, max: i64) -> GivenRowEntry {
        GivenRowEntry::Value(Value::Integer(self.integer_in(min, max)))
    }

    pub fn entry_integer(&mut self) -> GivenRowEntry {
        GivenRowEntry::Value(Value::Integer(self.integer()))
    }
}
