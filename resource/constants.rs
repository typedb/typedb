/*
 *  Copyright (C) 2023 Vaticle
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as
 *  published by the Free Software Foundation, either version 3 of the
 *  License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

pub mod snapshot {
    pub const BUFFER_KEY_INLINE: usize = 64;
    pub const BUFFER_VALUE_INLINE: usize = 64;
}

pub mod encoding {
    pub const LABEL_NAME_STRING_INLINE: usize = 64;
    pub const LABEL_SCOPE_STRING_INLINE: usize = 64;
    pub const LABEL_SCOPED_NAME_STRING_INLINE: usize = LABEL_NAME_STRING_INLINE + LABEL_SCOPE_STRING_INLINE;
}
