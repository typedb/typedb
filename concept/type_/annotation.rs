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

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub enum Annotation {
    Abstract(AnnotationAbstract),
    Duplicate(AnnotationDuplicate),
    Independent(AnnotationIndependent),
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub struct AnnotationAbstract {}

impl Default for AnnotationAbstract {
    fn default() -> Self {
        Self::new()
    }
}

impl AnnotationAbstract {
    pub fn new() -> Self {
        AnnotationAbstract {}
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub struct AnnotationDuplicate {}

impl Default for AnnotationDuplicate {
    fn default() -> Self {
        Self::new()
    }
}

impl AnnotationDuplicate {
    pub fn new() -> Self {
        AnnotationDuplicate {}
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub struct AnnotationIndependent {}

impl Default for AnnotationIndependent {
    fn default() -> Self {
        Self::new()
    }
}

impl AnnotationIndependent {
    pub fn new() -> Self {
        AnnotationIndependent {}
    }
}