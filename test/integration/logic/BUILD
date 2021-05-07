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
    name = "test-rule",
    srcs = ["RuleTest.java"],
    native_libraries_deps = [
        "//:typedb",
        "//concept:concept",
        "//logic:logic",
        "//pattern:pattern",
        "//rocks:rocks",
        "//traversal:traversal",
    ],
    resource_strip_prefix = "common/test",
    resources = [
        "//common/test:logback-test",
    ],
    test_class = "com.vaticle.typedb.core.logic.RuleTest",
    deps = [
        # Internal dependencies
        "//common",
        "//common/test:util",
        "//graph",
        "//test/integration/util",

        # External dependencies from Vaticle
        "@vaticle_typedb_common//:common",
        "@vaticle_typeql_lang_java//:typeql-lang",
        "@vaticle_typeql_lang_java//pattern:pattern",
    ],
)

host_compatible_java_test(
    name = "test-type-resolver",
    srcs = ["TypeResolverTest.java"],
    test_class = "com.vaticle.typedb.core.logic.TypeResolverTest",
    native_libraries_deps = [
        "//concept:concept",
        "//:typedb",
        "//logic:logic",
        "//pattern:pattern",
        "//rocks:rocks",
    ],
    deps = [
        # Internal dependencies
        "//common",
        "//common/test:util",
        "//test/integration/util",

        # External dependencies from Vaticle
        "@vaticle_typeql_lang_java//query",
        "@vaticle_typeql_lang_java//:typeql-lang",
        "@vaticle_typedb_common//:common",
    ],
    data = [":basic-schema.gql", ":test-type-resolution.gql"],
    resources = [
        "//common/test:logback-test"
    ],
    resource_strip_prefix = "common/test",
)

host_compatible_java_test(
    name = "test-unification-relation-concludable",
    srcs = ["resolvable/UnifyRelationConcludableTest.java", "resolvable/Util.java"],
    native_libraries_deps = [
        "//rocks:rocks",
        "//:typedb",
        "//concept:concept",
        "//logic:logic",
        "//pattern:pattern",
        "//traversal:traversal",
    ],
    test_class = "com.vaticle.typedb.core.logic.resolvable.UnifyRelationConcludableTest",
    deps = [
        # Internal dependencies
        "//test/integration/util",
        "//common",

        # External dependencies from Vaticle
        "@vaticle_typedb_common//:common",
        "@vaticle_typeql_lang_java//:typeql-lang",
    ],
)

host_compatible_java_test(
    name = "test-unification-has-concludable",
    srcs = ["resolvable/UnifyHasConcludableTest.java", "resolvable/Util.java"],
    native_libraries_deps = [
        "//rocks:rocks",
        "//:typedb",
        "//concept:concept",
        "//logic:logic",
        "//pattern:pattern",
        "//traversal:traversal",
    ],
    test_class = "com.vaticle.typedb.core.logic.resolvable.UnifyHasConcludableTest",
    deps = [
        # Internal dependencies
        "//test/integration/util",
        "//common",

        # External dependencies from Vaticle
        "@vaticle_typedb_common//:common",
        "@vaticle_typeql_lang_java//:typeql-lang",
    ],
)

host_compatible_java_test(
    name = "test-unification-isa-concludable",
    srcs = ["resolvable/UnifyIsaConcludableTest.java", "resolvable/Util.java"],
    native_libraries_deps = [
        "//rocks:rocks",
        "//:typedb",
        "//concept:concept",
        "//logic:logic",
        "//pattern:pattern",
        "//traversal:traversal",
    ],
    test_class = "com.vaticle.typedb.core.logic.resolvable.UnifyIsaConcludableTest",
    deps = [
        # Internal dependencies
        "//test/integration/util",
        "//common",

        # External dependencies from Vaticle
        "@vaticle_typedb_common//:common",
        "@vaticle_typeql_lang_java//:typeql-lang",
    ],
    resources = [
        "//common/test:logback-test"
    ],
    resource_strip_prefix = "common/test",
)

host_compatible_java_test(
    name = "test-unification-attribute-concludable",
    srcs = ["resolvable/UnifyAttributeConcludableTest.java", "resolvable/Util.java"],
    native_libraries_deps = [
        "//rocks:rocks",
        "//:typedb",
        "//concept:concept",
        "//logic:logic",
        "//pattern:pattern",
        "//traversal:traversal",
    ],
    test_class = "com.vaticle.typedb.core.logic.resolvable.UnifyAttributeConcludableTest",
    deps = [
        # Internal dependencies
        "//test/integration/util",
        "//common",

        # External dependencies from Vaticle
        "@vaticle_typedb_common//:common",
        "@vaticle_typeql_lang_java//:typeql-lang",
    ],
    resources = [
        "//common/test:logback-test"
    ],
    resource_strip_prefix = "common/test",
)

checkstyle_test(
    name = "checkstyle",
    include = glob([
        "*",
        "reasoner/*",
        "resolvable/*",
    ]),
    license_type = "agpl",
)
