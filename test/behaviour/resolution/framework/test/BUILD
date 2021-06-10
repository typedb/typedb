#
# Copyright (C) 2021 Vaticle
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
#

load("@vaticle_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@vaticle_dependencies//builder/java:rules.bzl", "native_java_libraries")
load("@vaticle_dependencies//builder/java:rules.bzl", "host_compatible_java_test")

native_java_libraries(
    name = "load-test",
    srcs = ["LoadTest.java"],
    visibility = ["//visibility:public"],
    native_libraries_deps = [
        "//:typedb",
    ],
)

java_test(
    name = "resolution-query-builder-it",
    size = "medium",
    srcs = [
        "ResolutionQueryBuilderIT.java",
    ],
    classpath_resources = ["//common/test:logback-test"],
    test_class = "grakn.core.test.behaviour.resolution.framework.test.ResolutionQueryBuilderIT",
    deps = [
        "//test/behaviour/resolution/framework/common",
        "//test/behaviour/resolution/framework/test:load-test",
        "//concept:concept",
        "//:typedb",

        # External dependencies from @vaticle
        "@vaticle_typeql_lang_java//:typeql-lang",
    ],
)

java_test(
    name = "test-resolution-query-builder",
    size = "small",
    srcs = [
        "TestResolutionQueryBuilder.java",
    ],
    classpath_resources = ["//common/test:logback-test"],
    test_class = "grakn.core.test.behaviour.resolution.framework.test.TestResolutionQueryBuilder",
    deps = [
        "//test/behaviour/resolution/framework/common",
        "//test/behaviour/resolution/framework/resolve",
        "@maven//:com_google_guava_guava",

        # External dependencies from @vaticle
        "@vaticle_typeql_lang_java//:typeql-lang",
    ],
)

java_test(
    name = "rule-resolution-builder-it",
    size = "medium",
    srcs = [
        "RuleResolutionBuilderIT.java",
    ],
    classpath_resources = ["//common/test:logback-test"],
    test_class = "grakn.core.test.behaviour.resolution.framework.test.RuleResolutionBuilderIT",
    deps = [
        "//test/behaviour/resolution/framework/common",
        "//test/behaviour/resolution/framework/test:load-test",
        "//:typedb",
        "@maven//:com_google_guava_guava",

        # External dependencies from @vaticle
        "@vaticle_typeql_lang_java//:typeql-lang",
    ],
)

host_compatible_java_test(
    name = "test-schema-manager",
    size = "medium",
    srcs = [
        "TestSchemaManager.java",
    ],
    classpath_resources = ["//common/test:logback-test"],
    test_class = "grakn.core.test.behaviour.resolution.framework.test.TestSchemaManager",
    native_libraries_deps = [
        "//:typedb",
        "//concept:concept",
        "//test/behaviour/resolution/framework/complete",
        "//test/behaviour/resolution/framework/test:load-test",
    ],
    deps = [
        # External dependencies from @vaticle
        "@vaticle_typeql_lang_java//:typeql-lang",
    ],
)

java_test(
    name = "test-completer",
    size = "medium",
    srcs = [
        "TestCompleter.java",
    ],
    classpath_resources = ["//common/test:logback-test"],
    test_class = "grakn.core.test.behaviour.resolution.framework.test.TestCompleter",
    deps = [
        "//test/behaviour/resolution/framework/common",
        "//test/behaviour/resolution/framework/complete",
        "//test/behaviour/resolution/framework/test:load-test",
        "//concept:concept",
        "//:typedb",

        # External dependencies from @vaticle
        "@vaticle_typeql_lang_java//:typeql-lang",
    ],
)

java_test(
    name = "test-resolution",
    size = "large",
    srcs = [
        "TestResolution.java",
    ],
    classpath_resources = ["//common/test:logback-test"],
    test_class = "grakn.core.test.behaviour.resolution.framework.test.TestResolution",
    deps = [
        "//test/behaviour/resolution/framework/test:load-test",
        "//test/behaviour/resolution/framework:resolution-lib",
        "//:typedb",

        # External dependencies from @vaticle
        "@vaticle_typeql_lang_java//:typeql-lang",
    ],
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*"]),
    license_type = "agpl",
)

