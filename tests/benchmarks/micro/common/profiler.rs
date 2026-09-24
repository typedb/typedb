/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

pub fn transaction_options_with_profiling() -> options::TransactionOptions {
    let mut tx_options = options::TransactionOptions::default();
    tx_options.tmp_enable_profiling = Some(true);
    tx_options
}
