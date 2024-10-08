/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::Arc,
};

use answer::{variable::Variable, Type};
use compiler::{
    expression::{block_compiler::compile_expressions, compiled_expression::CompiledExpression},
    insert::type_check::check_annotations,
    annotation::{
        annotated_functions::{AnnotatedUnindexedFunctions, IndexedAnnotatedFunctions},
        type_annotations::{ConstraintTypeAnnotations, TypeAnnotations},
        type_inference::{infer_types_for_functions, resolve_value_types},
    },
    reduce::ReduceInstruction,
};
use compiler::annotation::match_inference::infer_types;
use concept::type_::type_manager::TypeManager;
use encoding::value::value_type::{
    ValueTypeCategory,
    ValueTypeCategory::{Double, Long},
};
use ir::{
    pattern::constraint::Constraint,
    program::{
        block::Block,
        function::Function,
        modifier::{Limit, Offset, Require, Select, Sort},
        reduce::{Reduce, Reducer},
        ParameterRegistry, VariableRegistry,
    },
    translation::pipeline::TranslatedStage,
};
use storage::snapshot::ReadableSnapshot;

use crate::error::QueryError;
