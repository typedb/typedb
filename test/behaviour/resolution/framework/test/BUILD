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
    deps = [
        "//common",
        "@vaticle_typeql_lang_java//:typeql-lang",
    ]
)


host_compatible_java_test(
    name = "test-reasoner",
    size = "medium",
    srcs = [
        "ReasonerTest.java",
    ],
    classpath_resources = ["//common/test:logback-test"],
    test_class = "com.vaticle.typedb.core.test.behaviour.resolution.framework.test.ReasonerTest",
    native_libraries_deps = [
        "//concept:concept",
        "//:typedb",
        "//logic:logic",
        "//rocks:rocks",
        "//test/behaviour/resolution/framework/test:load-test",
        "//test/behaviour/resolution/framework:resolution",
    ],
    deps = [
        "//test/integration/util:util",
        "//common",
        # External dependencies from @vaticle
        "@vaticle_typeql_lang_java//:typeql-lang",
        "@vaticle_typeql_lang_java//pattern",
        "@vaticle_typeql_lang_java//query",
    ],
)

host_compatible_java_test(
    name = "test-resolution",
    size = "large",
    srcs = [
        "CorrectnessCheckerTest.java",
    ],
    classpath_resources = ["//common/test:logback-test"],
    test_class = "com.vaticle.typedb.core.test.behaviour.resolution.framework.test.CorrectnessCheckerTest",
    native_libraries_deps = [
        "//:typedb",
        "//rocks:rocks",
        "//test/behaviour/resolution/framework/test:load-test",
        "//test/behaviour/resolution/framework:resolution",
    ],
    deps = [
        "//common",
        "//common/test:util",
        "//test/integration/util",
        # External dependencies from @vaticle
        "@vaticle_typeql_lang_java//:typeql-lang",
        "@vaticle_typeql_lang_java//query",
    ],
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*"]),
    license_type = "agpl",
)

