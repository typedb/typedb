/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

pub mod server {
    pub const ASCII_LOGO: &str = include_str!("typedb-ascii.txt");
}

pub mod snapshot {
    pub const BUFFER_KEY_INLINE: usize = 64;
    pub const BUFFER_VALUE_INLINE: usize = 64;
}

pub mod storage {
    pub const TIMELINE_WINDOW_SIZE: usize = 100;
}

pub mod encoding {
    pub const LABEL_NAME_STRING_INLINE: usize = 64;
    pub const LABEL_SCOPE_STRING_INLINE: usize = 64;
    pub const LABEL_SCOPED_NAME_STRING_INLINE: usize = LABEL_NAME_STRING_INLINE + LABEL_SCOPE_STRING_INLINE;
}
