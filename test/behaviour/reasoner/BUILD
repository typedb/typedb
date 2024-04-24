# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@vaticle_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@vaticle_dependencies//builder/java:rules.bzl", "host_compatible_java_library")

host_compatible_java_library(
    name = "steps",
    srcs = [
        "ReasonerSteps.java"
    ],
    native_libraries_deps = [
        # Package dependencies
        "//:typedb",
        "//common:common",
        "//database:database",
        "//concept:concept",
        "//test/behaviour/reasoner/verification:verification",
    ],
    deps = [

        # External dependencies from Vaticle
        "@vaticle_typeql//common/java:common",
        "@vaticle_typeql//java:typeql-lang",
        "@vaticle_typeql//java/common",
        "@vaticle_typeql//java/query",

        # External dependencies from Maven
        "@maven//:junit_junit",
        "@maven//:io_cucumber_cucumber_java",
    ],
    visibility = ["//visibility:public"],
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*"]),
    license_type = "mpl-header",
)
