# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@vaticle_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@vaticle_dependencies//builder/java:rules.bzl", "native_java_libraries")
load("@vaticle_dependencies//builder/java:rules.bzl", "host_compatible_java_test")

package(
    default_visibility = [
        "//concept:__pkg__",
        "//logic:__subpackages__",
        "//query:__subpackages__",
        "//reasoner:__subpackages__",
        "//test/behaviour:__subpackages__",
        "//test/integration:__subpackages__",
        "//test/behaviour/reasoner:__subpackages__",
    ]
)

native_java_libraries(
    name = "pattern",
    srcs = glob([
        "*.java",
        "constraint/*.java",
        "constraint/common/*.java",
        "constraint/thing/*.java",
        "constraint/type/*.java",
        "constraint/value/*.java",
        "variable/*.java",
        "equivalence/*.java",
    ], exclude = ["*Test.java", "*/*Test.java"],),
    deps = [
        # External dependencies from Vaticle
        "@vaticle_typeql//common/java:common",
        "@vaticle_typeql//java/builder:builder",
        "@vaticle_typeql//java/common:common",
        "@vaticle_typeql//java/pattern:pattern",

        # External dependencies from Maven
        "@maven//:com_google_code_findbugs_jsr305",
    ],
    native_libraries_deps = [
        # Internal dependencies
        "//common:common",
        "//encoding:encoding",
        "//traversal:traversal",
    ],
    tags = ["maven_coordinates=com.vaticle.typedb:typedb-core-query-pattern:{pom_version}"],
)

host_compatible_java_test(
    name = "test-conjunction",
    srcs = [
        "ConjunctionTest.java",
    ],
    test_class = "com.vaticle.typedb.core.pattern.ConjunctionTest",
    native_libraries_deps = [
        "//pattern:pattern",
    ],
    deps = [
        # External dependencies from Vaticle
        "@vaticle_typeql//common/java:common",
        "@vaticle_typeql//java:typeql-lang",
    ],
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*", "*/*", "*/*/*"]),
    license_type = "mpl-header",
)
