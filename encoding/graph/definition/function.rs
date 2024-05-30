/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use bytes::Bytes;
use resource::constants::snapshot::BUFFER_VALUE_INLINE;


// TODO: we can probably just treat the function as a string blob, since we don't have to
//       index or assign anything in the internals

struct FunctionDefinition<'a> {
    bytes: Bytes<'a, BUFFER_VALUE_INLINE>
}

impl<'a> FunctionDefinition<'a> {

    fn new(bytes: Bytes<'a, BUFFER_VALUE_INLINE>) -> Self {
        Self { bytes }
    }

    // TODO: take in TypeQL or string version?
    fn build(definition: &str) -> Self {
        todo!()
    }
}
