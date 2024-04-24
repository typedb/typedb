# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

package(default_visibility = ["//test/behaviour/concept/thing:__subpackages__"])
load("@vaticle_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@vaticle_dependencies//builder/java:rules.bzl", "host_compatible_java_library")

host_compatible_java_library(
    name = "steps",
    srcs = [
        "ThingSteps.java"
    ],
    deps = [
        "//test/behaviour/config:parameters",
        "//test/behaviour/concept/type/thingtype:steps",
        "//test/behaviour/connection:steps",

        # Vaticle Dependencies
        "@vaticle_typeql//common/java:common",
        "@vaticle_typeql//java/common",

        # External Maven Dependencies
        "@maven//:junit_junit",
        "@maven//:io_cucumber_cucumber_java",
    ],
    native_libraries_deps = [
        # Internal dependencies
        "//concept:concept",
        "//common:common",
        "//common/test:util",
    ],
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*"]),
    license_type = "mpl-header",
)
