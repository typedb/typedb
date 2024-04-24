# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

package(default_visibility = ["//test/behaviour:__subpackages__"])
load("@vaticle_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")

java_library(
    name = "steps",
    srcs = [
        "DatabaseSteps.java"
    ],
    deps = [
        # Internal dependencies
        "//test/behaviour/connection:steps",

        # Internal Repository Dependencies
        "@vaticle_typeql//common/java:common",

        # External Maven Dependencies
        "@maven//:junit_junit",
        "@maven//:io_cucumber_cucumber_java",
    ]
)

java_test(
    name = "test",
    srcs = [
        "DatabaseTest.java"
    ],
    test_class = "com.vaticle.typedb.core.test.behaviour.connection.database.DatabaseTest",
    deps = [
        # External Maven Dependencies
        "@maven//:io_cucumber_cucumber_junit",
    ],
    runtime_deps = [
        ":steps",
        "//test/behaviour/config:parameters",
        "//test/behaviour/connection/transaction:steps",
        "//test/behaviour/connection/session:steps",
        "//test/behaviour/query:steps",
    ],
    data = [
        "@vaticle_typedb_behaviour//connection:database.feature",
    ],
    size = "medium",
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*"]),
    license_type = "mpl-header",
)
