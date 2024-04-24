# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

package(default_visibility = ["//test/behaviour/typeql/language/define:__subpackages__"])

load("@vaticle_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@vaticle_dependencies//builder/java:rules.bzl", "host_compatible_java_library")

host_compatible_java_library(
    name = "steps",
    srcs = [
        "TypeQLSteps.java",
    ],
    native_libraries_deps = [
        "//concept:concept",
        "//common:common",
        "//common/test:util",
        "//traversal:traversal",
        "//pattern:pattern",
    ],
    visibility = ["//visibility:public"],
    runtime_deps = [
        "//test/behaviour/connection/database:steps",
        "//test/behaviour/connection/transaction:steps",
    ],
    deps = [
        # Package dependencies
        "//test/behaviour/connection:steps",
        "//test/behaviour/exception",

        # External dependencies from Vaticle
        "@vaticle_typeql//common/java:common",
        "@vaticle_typeql//java:typeql-lang",
        "@vaticle_typeql//java/common",
        "@vaticle_typeql//java/query",

        # External dependencies from Maven
        "@maven//:com_eclipsesource_minimal_json_minimal_json",
        "@maven//:junit_junit",
        "@maven//:io_cucumber_cucumber_java",
    ],
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*"]),
    license_type = "mpl-header",
)
