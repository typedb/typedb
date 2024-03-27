# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@vaticle_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@vaticle_dependencies//builder/java:rules.bzl", "native_java_libraries")
package(default_visibility = ["//visibility:public"])

native_java_libraries(
    name = "util",
    srcs = glob(["*.java"]),
    deps = [
        # External Maven Dependencies
        "@maven//:junit_junit",
        "@maven//:com_eclipsesource_minimal_json_minimal_json",
    ],
    native_libraries_deps = [
        # Internal dependencies
        "//common:common",
    ]
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*", "*/*"]),
    license_type = "mpl-header",
)
