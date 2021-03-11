#
# Copyright (C) 2021 Grakn Labs
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

load("@graknlabs_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@graknlabs_dependencies//builder/java:rules.bzl", "host_compatible_java_test")

host_compatible_java_test(
    name = "test-rule",
    srcs = ["RuleTest.java"],
    native_libraries_deps = [
        "//:grakn",
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
    test_class = "grakn.core.logic.RuleTest",
    deps = [
        # Internal dependencies
        "//common",
        "//common/test:util",
        "//graph",
        "//test/integration/util",

        # External dependencies from Grakn Labs
        "@graknlabs_common//:common",
        "@graknlabs_graql//java:graql",
        "@graknlabs_graql//java/pattern:pattern",
    ],
)

host_compatible_java_test(
    name = "test-type-resolver",
    srcs = ["TypeResolverTest.java"],
    test_class = "grakn.core.logic.TypeResolverTest",
    native_libraries_deps = [
        "//concept:concept",
        "//:grakn",
        "//logic:logic",
        "//pattern:pattern",
        "//rocks:rocks",
    ],
    deps = [
        # Internal dependencies
        "//common",
        "//common/test:util",
        "//test/integration/util",

        # External dependencies from Grakn Labs
        "@graknlabs_graql//java/query",
        "@graknlabs_graql//java:graql",
        "@graknlabs_common//:common",
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
        "//:grakn",
        "//concept:concept",
        "//logic:logic",
        "//pattern:pattern",
        "//traversal:traversal",
    ],
    test_class = "grakn.core.logic.resolvable.UnifyRelationConcludableTest",
    deps = [
        # Internal dependencies
        "//test/integration/util",
        "//common",

        # External dependencies from Grakn Labs
        "@graknlabs_common//:common",
        "@graknlabs_graql//java:graql",
    ],
)

host_compatible_java_test(
    name = "test-unification-has-concludable",
    srcs = ["resolvable/UnifyHasConcludableTest.java", "resolvable/Util.java"],
    native_libraries_deps = [
        "//rocks:rocks",
        "//:grakn",
        "//concept:concept",
        "//logic:logic",
        "//pattern:pattern",
        "//traversal:traversal",
    ],
    test_class = "grakn.core.logic.resolvable.UnifyHasConcludableTest",
    deps = [
        # Internal dependencies
        "//test/integration/util",
        "//common",

        # External dependencies from Grakn Labs
        "@graknlabs_common//:common",
        "@graknlabs_graql//java:graql",
    ],
)

host_compatible_java_test(
    name = "test-unification-isa-concludable",
    srcs = ["resolvable/UnifyIsaConcludableTest.java", "resolvable/Util.java"],
    native_libraries_deps = [
        "//rocks:rocks",
        "//:grakn",
        "//concept:concept",
        "//logic:logic",
        "//pattern:pattern",
        "//traversal:traversal",
    ],
    test_class = "grakn.core.logic.resolvable.UnifyIsaConcludableTest",
    deps = [
        # Internal dependencies
        "//test/integration/util",
        "//common",

        # External dependencies from Grakn Labs
        "@graknlabs_common//:common",
        "@graknlabs_graql//java:graql",
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
        "//:grakn",
        "//concept:concept",
        "//logic:logic",
        "//pattern:pattern",
        "//traversal:traversal",
    ],
    test_class = "grakn.core.logic.resolvable.UnifyAttributeConcludableTest",
    deps = [
        # Internal dependencies
        "//test/integration/util",
        "//common",

        # External dependencies from Grakn Labs
        "@graknlabs_common//:common",
        "@graknlabs_graql//java:graql",
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
