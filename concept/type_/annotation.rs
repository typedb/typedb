/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub enum Annotation {
    Abstract(AnnotationAbstract),
    Distinct(AnnotationDistinct),
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
pub struct AnnotationDistinct {}

impl Default for AnnotationDistinct {
    fn default() -> Self {
        Self::new()
    }
}

impl AnnotationDistinct {
    pub fn new() -> Self {
        AnnotationDistinct {}
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
