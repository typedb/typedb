/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{error::Error, fmt};

use crate::{expression::ExpressionCompileError, match_::inference::TypeInferenceError};
pub mod inference;
pub mod instructions;
mod optimisation;
pub mod planner;
