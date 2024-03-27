# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@vaticle_dependencies//library/ortools:deps.bzl", "google_or_tools")
load("@vaticle_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@vaticle_dependencies//builder/java:rules.bzl", "native_java_libraries")

package(
    default_visibility = [
        "//pattern:__subpackages__",
        "//query:__subpackages__",
        "//reasoner:__subpackages__",
        "//database:__pkg__",
    ]
)

native_java_libraries(
    name = "traversal",
    srcs = glob(["*.java", "*/*.java", "*/*/*.java"]),
    native_libraries_deps = [
        # Internal dependencies
        "//common:common",
        "//concurrent:concurrent",
        "//graph:graph",
        "//encoding:encoding",
    ],
    deps = [
        # Vaticle Dependencies
        "@vaticle_typeql//common/java:common",
        "@vaticle_typeql//java/common:common",
        "@vaticle_typeql//java/pattern:pattern",
        "@vaticle_typeql//java/query:query",

        # External dependencies from Vaticle

        # External dependencies from Maven
        "@maven//:com_github_ben_manes_caffeine_caffeine",
        "@maven//:com_google_code_findbugs_jsr305",
        "@maven//:org_slf4j_slf4j_api",
    ],
    tags = ["maven_coordinates=com.vaticle.typedb:typedb-core-traversal:{pom_version}"],
    visibility = ["//visibility:public"],
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*", "*/*", "*/*/*"]),
    license_type = "mpl-header",
)
