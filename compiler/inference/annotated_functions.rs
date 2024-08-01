/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use encoding::graph::definition::definition_key::DefinitionKey;
use ir::program::{function::Function, function_signature::FunctionIDAPI};

use crate::inference::type_annotations::FunctionAnnotations;

pub struct AnnotatedCommittedFunctions {
    ir: Box<[Option<Function>]>,
    annotations: Box<[Option<FunctionAnnotations>]>,
}

impl AnnotatedCommittedFunctions {
    pub fn new(ir: Box<[Option<Function>]>, annotations: Box<[Option<FunctionAnnotations>]>) -> Self {
        Self { ir, annotations }
    }

    pub fn empty() -> Self {
        Self { ir: Box::new([]), annotations: Box::new([]) }
    }
}

pub trait AnnotatedFunctions {
    type ID;

    fn get_function(&self, id: Self::ID) -> Option<&Function>;

    fn get_annotations(&self, id: Self::ID) -> Option<&FunctionAnnotations>;
}

impl AnnotatedFunctions for AnnotatedCommittedFunctions {
    type ID = DefinitionKey<'static>;

    fn get_function(&self, id: Self::ID) -> Option<&Function> {
        self.ir.get(id.as_usize())?.as_ref()
    }

    fn get_annotations(&self, id: Self::ID) -> Option<&FunctionAnnotations> {
        self.annotations.get(id.as_usize())?.as_ref()
    }
}

// May hold IR & Annotations for either Schema functions or Preamble functions
// For schema functions, The index does not correspond to function_id.as_usize().
pub struct AnnotatedUncommittedFunctions {
    ir: Box<[Function]>,
    annotations: Box<[FunctionAnnotations]>,
}

impl AnnotatedUncommittedFunctions {
    pub fn new(ir: Box<[Function]>, annotations: Box<[FunctionAnnotations]>) -> Self {
        Self { ir, annotations }
    }

    pub fn iter_functions(&self) -> impl Iterator<Item = &Function> {
        self.ir.iter()
    }

    pub fn into_parts(self) -> (Box<[Function]>, Box<[FunctionAnnotations]>) {
        let Self { ir, annotations } = self;
        (ir, annotations)
    }
}

impl AnnotatedFunctions for AnnotatedUncommittedFunctions {
    type ID = usize;

    fn get_function(&self, id: Self::ID) -> Option<&Function> {
        self.ir.get(id)
    }

    fn get_annotations(&self, id: Self::ID) -> Option<&FunctionAnnotations> {
        self.annotations.get(id)
    }
}
