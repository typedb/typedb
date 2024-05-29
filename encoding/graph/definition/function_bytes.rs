/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use bytes::Bytes;
use resource::constants::snapshot::BUFFER_VALUE_INLINE;

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct FunctionBytes<'a, const INLINE_LENGTH: usize> {
    bytes: Bytes<'a, INLINE_LENGTH>,
}

trait FunctionEncodable {
    fn encode(&self) -> FunctionBytes<'_, { BUFFER_VALUE_INLINE }>;
}
