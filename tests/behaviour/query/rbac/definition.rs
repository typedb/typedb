/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#![allow(unexpected_cfgs, reason = "features defined in Bazel targets aren't currently communicated to Cargo")]

use steps::Context;

#[tokio::test]
async fn test_rbac_definition() {
    #[cfg(feature = "bazel")]
    let path = "../typedb_behaviour+/query/rbac/definition.feature";

    #[cfg(not(feature = "bazel"))]
    let path = "bazel-typedb/external/typedb_behaviour+/query/rbac/definition.feature";

    assert!(Context::test(path, true).await);
}
