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
load("@vaticle_dependencies//builder/java:rules.bzl", "host_compatible_java_test")

host_compatible_java_test(
    name = "test-basic",
    srcs = ["BasicTest.java"],
    test_class = "com.vaticle.typedb.core.test.integration.BasicTest",
    native_libraries_deps = [
        # Internal dependencies
        "//:typedb",
        "//common:common",
        "//rocks:rocks",
        "//concept:concept",
        "//logic:logic",
    ],
    deps = [
        "//test/integration/util:util",
        # External dependencies from Vaticle
        "@vaticle_typeql_lang_java//pattern:pattern",
        "@vaticle_typeql_lang_java//:typeql-lang",
    ],
    resources = [
        "//common/test:logback-test"
    ],
    resource_strip_prefix = "common/test",
    size = "large",
)

host_compatible_java_test(
    name = "test-query",
    srcs = ["QueryTest.java"],
    test_class = "com.vaticle.typedb.core.test.integration.QueryTest",
    native_libraries_deps = [
        "//:typedb",
        "//rocks:rocks",
        "//common:common",
        "//concept:concept",
    ],
    deps = [
        # Internal dependencies
        "//test/integration/util:util",

        # External dependencies from Vaticle
        "@vaticle_typeql_lang_java//query:query",
        "@vaticle_typeql_lang_java//:typeql-lang",
    ],
    data = [":schema.tql"],
    resources = [
        "//common/test:logback-test"
    ],
    resource_strip_prefix = "common/test",
)

host_compatible_java_test(
    name = "test-consistency",
    srcs = ["ConsistencyTest.java"],
    test_class = "com.vaticle.typedb.core.test.integration.ConsistencyTest",
    native_libraries_deps = [
        # Internal dependencies
        "//:typedb",
        "//rocks:rocks",
        "//common:common",
        "//concept:concept",
        "//logic:logic",
    ],
    deps = [
        "//test/integration/util:util",

        # External dependencies from Vaticle
        "@vaticle_typeql_lang_java//:typeql-lang",
    ],
    resources = [
        "//common/test:logback-test"
    ],
    resource_strip_prefix = "common/test",
    size = "large",
)

host_compatible_java_test(
    name = "test-attribute-string",
    srcs = ["StringAttributeTest.java"],
    test_class = "com.vaticle.typedb.core.test.integration.StringAttributeTest",
    native_libraries_deps = [
        # Internal dependencies
        "//:typedb",
        "//common:common",
        "//rocks:rocks",
        "//concept:concept",
        "//logic:logic",
    ],
    deps = [
        "//test/integration/util:util",

        # External dependencies from Vaticle
        "@vaticle_typedb_common//:common",
        "@vaticle_typeql_lang_java//:typeql-lang",
    ],
    resources = [
        "//common/test:logback-test"
    ],
    resource_strip_prefix = "common/test",
    size = "large",
)

checkstyle_test(
    name = "checkstyle",
    include = glob([
        "*",
    ]),
    license_type = "agpl",
)
