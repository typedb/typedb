/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use steps::Context;

#[tokio::test]
async fn test_a() {
    assert!(Context::test("tests/behaviour/debug/debug.feature", true).await);
}

#[tokio::test]
async fn test_z() {
    assert!(Context::test("tests/behaviour/debug/debug.feature", true).await);
}
