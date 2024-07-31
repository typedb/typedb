/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use encoding::graph::definition::definition_key::DefinitionKey;
use ir::program::{function::FunctionIR, function_signature::FunctionIDTrait};
use crate::inference::type_annotations::FunctionAnnotations;

pub struct CompiledSchemaFunctions {
    ir: Box<[Option<FunctionIR>]>,
    annotations: Box<[Option<FunctionAnnotations>]>,
}

impl CompiledSchemaFunctions {
    pub fn new(ir: Box<[Option<FunctionIR>]>, annotations: Box<[Option<FunctionAnnotations>]>) -> Self {
        Self { ir, annotations }
    }

    pub fn empty() -> Self {
        Self { ir: Box::new([]), annotations: Box::new([]) }
    }
}

pub trait CompiledFunctions {
    type KeyType;

    fn get_function_ir(&self, id: Self::KeyType) -> Option<&FunctionIR>;

    fn get_function_annotations(&self, id: Self::KeyType) -> Option<&FunctionAnnotations>;
}

impl CompiledFunctions for CompiledSchemaFunctions {
    type KeyType = DefinitionKey<'static>;

    fn get_function_ir(&self, id: Self::KeyType) -> Option<&FunctionIR> {
        self.ir.get(id.as_usize())?.as_ref()
    }

    fn get_function_annotations(&self, id: Self::KeyType) -> Option<&FunctionAnnotations> {
        self.annotations.get(id.as_usize())?.as_ref()
    }
}

// May hold IR & Annotations for either Schema functions or Preamble functions
// For schema functions, The index does not correspond to function_id.as_usize().
pub struct CompiledLocalFunctions {
    ir: Box<[FunctionIR]>,
    annotations: Box<[FunctionAnnotations]>,
}

impl CompiledLocalFunctions {
    pub fn new(ir: Box<[FunctionIR]>, annotations: Box<[FunctionAnnotations]>) -> Self {
        Self { ir, annotations }
    }

    pub fn iter_ir(&self) -> impl Iterator<Item = &FunctionIR> {
        self.ir.iter()
    }

    pub fn into_parts(self) -> (Box<[FunctionIR]>, Box<[FunctionAnnotations]>) {
        let Self { ir, annotations } = self;
        (ir, annotations)
    }
}

impl CompiledFunctions for CompiledLocalFunctions {
    type KeyType = usize;

    fn get_function_ir(&self, id: Self::KeyType) -> Option<&FunctionIR> {
        self.ir.get(id)
    }

    fn get_function_annotations(&self, id: Self::KeyType) -> Option<&FunctionAnnotations> {
        self.annotations.get(id)
    }
}
