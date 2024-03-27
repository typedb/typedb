# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@vaticle_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@vaticle_dependencies//builder/java:rules.bzl", "native_java_libraries")
package(
    default_visibility = [
        "//concept:__pkg__",
        "//database:__pkg__",
        "//graph:__pkg__",
        "//traversal:__pkg__",
        "//logic:__pkg__"
    ]
)

native_java_libraries(
    name = "encoding",
    srcs = glob(["*.java", "*/*.java"]),
    deps = [
        # External dependencies from Vaticle
        "@vaticle_typeql//common/java:common",
        "@vaticle_typeql//java/common:common",

        # External Maven Dependencies
        "@maven//:com_google_code_findbugs_jsr305",
    ],
    native_libraries_deps = [
        # Internal dependencies
        "//common:common",
    ],
    tags = ["maven_coordinates=com.vaticle.typedb:typedb-core-encoding:{pom_version}"],
    visibility = ["//visibility:public"],
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*", "*/*"]),
    license_type = "mpl-header",
)
