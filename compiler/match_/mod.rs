/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, sync::Arc};

use answer::variable::Variable;
use concept::thing::statistics::Statistics;
use ir::program::block::{FunctionalBlock, VariableRegistry};

use self::{inference::type_annotations::TypeAnnotations, planner::pattern_plan::MatchProgram};
use crate::expression::compiled_expression::CompiledExpression;

pub mod inference;
pub mod instructions;
mod optimisation;
pub mod planner;
