/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#![allow(unexpected_cfgs, reason = "features defined in Bazel targets aren't currently communicated to Cargo")]

use http_steps::Context;

#[tokio::test]
async fn test() {
    assert!(Context::test("tests/behaviour/service/http/debug/debug.feature").await);
}
