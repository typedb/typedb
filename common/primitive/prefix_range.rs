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

struct PrefixRange<T> where T: Ord {
    // inclusive
    start: T,
    // exclusive or unbounded
    end: Option<T>,
}

impl<T> PrefixRange<T> where T: Ord {
    pub fn new_unbounded(start: T) -> Self {
        Self { start, end: None }
    }

    pub fn new(start: T, end: T) -> Self {
        Self { start, end: Some(end) }
    }

    pub fn start(&self) -> &T {
        &self.start
    }

    pub fn end(&self) -> &Option<T> {
        &self.end
    }
}