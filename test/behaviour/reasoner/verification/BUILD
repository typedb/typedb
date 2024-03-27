# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@vaticle_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@vaticle_dependencies//builder/java:rules.bzl", "native_java_libraries")

native_java_libraries(
    name = "verification",
    srcs = glob(["*.java"]),
    visibility = ["//visibility:public"],
    native_libraries_deps = [
        "//:typedb",
        "//common:common",
        "//database:database",
        "//concept:concept",
        "//traversal:traversal",
        "//logic:logic",
        "//pattern:pattern",
        "//reasoner:reasoner",
    ],
    deps = [
        "@vaticle_typeql//common/java:common",
        "@vaticle_typeql//java:typeql-lang",
        "@vaticle_typeql//java/common",
        "@vaticle_typeql//java/query",
        "@vaticle_typeql//java/pattern",

        "@maven//:com_google_code_findbugs_jsr305",
    ],
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*"]),
    license_type = "mpl-header",
)
