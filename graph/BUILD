# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@vaticle_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@vaticle_dependencies//builder/java:rules.bzl", "native_java_libraries")
package(
    default_visibility = [
        "//concept:__pkg__",
        "//database:__pkg__",
        "//traversal:__pkg__",
        "//reasoner:__pkg__"
    ]
)

native_java_libraries(
    name = "graph",
    srcs = glob(["*.java", "*/*.java", "*/*/*.java"]),
    deps = [
        # External dependencies from Vaticle
        "@vaticle_typeql//common/java:common",

        # External dependencies from Vaticle
        "@vaticle_typeql//java:typeql-lang",
        "@vaticle_typeql//java/common:common",
        "@vaticle_typeql//java/pattern:pattern",

        # External Maven Dependencies
        "@maven//:com_google_code_findbugs_jsr305",
    ],
    native_libraries_deps = [
        # Internal dependencies
        "//common:common",
        "//encoding:encoding",
    ],
    tags = ["maven_coordinates=com.vaticle.typedb:typedb-core-graph:{pom_version}"],
    visibility = ["//visibility:public"],
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*", "*/*", "*/*/*"]),
    license_type = "mpl-header",
)
