/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use serde::{Deserialize, Serialize};

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub enum Annotation {
    Abstract(AnnotationAbstract),
    Distinct(AnnotationDistinct),
    Independent(AnnotationIndependent),
    Cardinality(AnnotationCardinality),
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

#[derive(Serialize, Deserialize, Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub struct AnnotationCardinality {
    // ##########################################################################
    // ###### WARNING: any changes here may break backwards compatibility! ######
    // ##########################################################################
    start_inclusive: u64,
    end_inclusive: Option<u64>,
}

impl AnnotationCardinality {
    pub const fn new(start_inclusive: u64, end_inclusive: Option<u64>) -> Self {
        AnnotationCardinality { start_inclusive, end_inclusive }
    }

    pub fn is_valid(&self, count: u64) -> bool {
        self.start_inclusive <= count && (
            self.end_inclusive.is_none() || count <= self.end_inclusive.clone().unwrap()
        )
    }
}
