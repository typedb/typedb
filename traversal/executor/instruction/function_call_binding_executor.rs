/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use ir::pattern::constraint::FunctionCallBinding;

use crate::executor::Position;

pub(crate) struct FunctionCallBindingIteratorExecutor {
    function_call_binding: FunctionCallBinding<Position>,
}
