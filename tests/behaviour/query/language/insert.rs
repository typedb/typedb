/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use steps::Context;

#[tokio::test]
async fn test() {
    // Bazel specific path: when running the test in bazel, the external data from
    // @vaticle_typedb_behaviour is stored in a directory that is a sibling to
    // the working directory.
    #[cfg(feature = "bazel")]
    let path = "../vaticle_typedb_behaviour/query/language/insert.feature";

    #[cfg(not(feature = "bazel"))]
    let path = "bazel-typedb/external/vaticle_typedb_behaviour/query/language/insert.feature";

    assert!(Context::test(path, true).await);
}
