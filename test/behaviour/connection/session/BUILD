# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

package(default_visibility = ["//test/behaviour:__subpackages__"])
load("@vaticle_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@vaticle_dependencies//builder/java:rules.bzl", "host_compatible_java_library")

host_compatible_java_library(
    name = "steps",
    srcs = [
        "SessionSteps.java",
    ],
    deps = [
        "//test/behaviour/connection:steps",

        # Internal Repository Dependencies
        "@vaticle_typeql//common/java:common",

        # External Maven Dependencies
        "@maven//:junit_junit",
        "@maven//:io_cucumber_cucumber_java",
    ],
    native_libraries_deps = [
        # Internal dependencies
        "//:typedb",
        "//common:common",
    ],
)

java_test(
    name = "test",
    srcs = [
        "SessionTest.java"
    ],
    test_class = "com.vaticle.typedb.core.test.behaviour.connection.session.SessionTest",
    deps = [
        # External Maven Dependencies
        "@maven//:io_cucumber_cucumber_junit",
    ],
    runtime_deps = [
        ":steps",
        "//test/behaviour/config:parameters",
        "//test/behaviour/connection/database:steps",
        "//test/behaviour/connection/transaction:steps",
        "//test/behaviour/query:steps",
    ],
    data = [
        "@vaticle_typedb_behaviour//connection:session.feature",
    ],
    size = "medium",
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*"]),
    license_type = "mpl-header",
)
