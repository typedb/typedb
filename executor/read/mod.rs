/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use storage::snapshot::ReadableSnapshot;

pub mod expression_executor;
mod pattern_instructions;
pub(crate) mod recursive_executor;
pub(super) mod step_executors;
pub(crate) mod subpattern_executor;
