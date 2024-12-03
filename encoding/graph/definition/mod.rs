/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use bytes::Bytes;
use resource::constants::snapshot::BUFFER_VALUE_INLINE;

pub mod definition_key;
pub mod definition_key_generator;
pub mod function;
pub mod r#struct;

pub trait DefinitionValueEncoding {
    fn from_bytes(value: &[u8]) -> Self;

    fn into_bytes(self) -> Option<Bytes<'static, BUFFER_VALUE_INLINE>>;
}
