# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@vaticle_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@vaticle_dependencies//builder/java:rules.bzl", "native_java_libraries")
load("@vaticle_dependencies//builder/java:rules.bzl", "host_compatible_java_test")

package(
    default_visibility = [
        "//:__pkg__",
        "//database:__pkg__",
        "//reasoner:__pkg__",
        "//test:__subpackages__",
    ],
)

native_java_libraries(
    name = "logic",
    srcs = glob(
        [
            "*.java",
            "tool/*.java",
            "**/*.java",
        ],
        exclude = [
            "*Test.java",
            "*/*Test.java",
        ],
    ),
    native_libraries_deps = [
        # Internal dependencies
        "//common:common",
        "//concept:concept",
        "//graph:graph",
        "//encoding:encoding",
        "//pattern:pattern",
        "//traversal:traversal",
    ],
    tags = ["maven_coordinates=com.vaticle.typedb:typedb-core-logic:{pom_version}"],
    visibility = ["//visibility:public"],
    runtime_deps = [
        "@maven//:ch_qos_logback_logback_classic",
    ],
    deps = [
        # Internal Repository Dependencies
        "@vaticle_typeql//common/java:common",
        "@vaticle_typeql//java/common",
        "@vaticle_typeql//java/pattern",

        # External Maven Dependencies
        "@maven//:com_google_code_findbugs_jsr305",
        "@maven//:org_slf4j_slf4j_api",
    ],
)

host_compatible_java_test(
    name = "test-concludable",
    srcs = [
        "resolvable/ConcludableTest.java",
    ],
    native_libraries_deps = [
        "//common:common",
        "//logic:logic",
        "//pattern:pattern",
    ],
    test_class = "com.vaticle.typedb.core.logic.resolvable.ConcludableTest",
    deps = [
        # Internal dependencies

        # External dependencies from Vaticle
        "@vaticle_typeql//common/java:common",
        "@vaticle_typeql//java:typeql-lang",
    ],
)

host_compatible_java_test(
    name = "test-retrievable",
    srcs = [
        "resolvable/RetrievableTest.java",
    ],
    native_libraries_deps = [
        # Internal dependencies
        "//common:common",
        "//logic:logic",
        "//pattern:pattern",
    ],
    test_class = "com.vaticle.typedb.core.logic.resolvable.RetrievableTest",
    deps = [

        # External dependencies from Vaticle
        "@vaticle_typeql//common/java:common",
        "@vaticle_typeql//java:typeql-lang",
    ],
)

host_compatible_java_test(
    name = "test-unifier",
    srcs = [
        "resolvable/UnifierTest.java",
    ],
    native_libraries_deps = [
        "//common:common",
        "//logic:logic",
        "//pattern:pattern",
        "//traversal:traversal",
    ],
    test_class = "com.vaticle.typedb.core.logic.resolvable.UnifierTest",
    deps = [
        # Internal dependencies

        # External dependencies from Vaticle
        "@vaticle_typeql//common/java:common",
        "@vaticle_typeql//java/common",
        "@vaticle_typeql//java:typeql-lang",
    ],
)

checkstyle_test(
    name = "checkstyle",
    include = glob([
        "*",
        "*/*",
    ]),
    license_type = "mpl-header",
)
