# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@vaticle_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@vaticle_dependencies//builder/java:rules.bzl", "native_java_libraries")
load("@vaticle_dependencies//builder/java:rules.bzl", "host_compatible_java_test")

package(default_visibility = [
    "//query:__subpackages__",
    "//reasoner:__subpackages__",
    "//database:__pkg__",
    "//server:__pkg__",
    "//test/integration/reasoner:__pkg__",
    "//test/behaviour/reasoner:__subpackages__",
    "//test/benchmark/reasoner/iam:__subpackages__",
    "//test/benchmark/reasoner/synthetic:__subpackages__"
])

native_java_libraries(
    name = "reasoner",
    srcs = glob(
        ["*.java",
        "processor/*.java",
        "processor/reactive/*.java",
        "processor/reactive/common/*.java",
        "controller/*.java",
        "answer/*.java",
        "common/*.java",
        "planner/*.java"
        ], exclude = ["*Test.java", "**/*Test.java"],
    ),
    native_libraries_deps = [
        # Internal dependencies
        "//common:common",
        "//concurrent:concurrent",
        "//concept:concept",
        "//graph:graph",
        "//logic:logic",
        "//pattern:pattern",
        "//traversal:traversal",
    ],
    deps = [
        # External dependencies from Vaticle
        "@vaticle_typeql//common/java:common",
        "@vaticle_typeql//java/common",
        "@vaticle_typeql//java/pattern",
        "@vaticle_typeql//java/query",

        # External Maven Dependencies
        "@maven//:com_google_code_findbugs_jsr305",
        "@maven//:org_slf4j_slf4j_api"
    ],
    runtime_deps = [
        "@maven//:ch_qos_logback_logback_classic",
    ],
    tags = ["maven_coordinates=com.vaticle.typedb:typedb-core-reasoner:{pom_version}"],
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*", "*/*", "*/*/*", "*/*/*/*"]),
    license_type = "mpl-header",
)
