#
# Copyright (C) 2020 Grakn Labs
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
    name = "test-traversal",
    srcs = ["TraversalTest.java"],
    test_class = "grakn.core.traversal.TraversalTest",
    native_libraries_deps = [
        "//rocks:rocks",
        "//:grakn",
        "//concept:concept",
    ],
    deps = [
        # Internal dependencies
        "//test/integration/util:util",
        "//common:common",

        # External dependencies from Grakn Labs
        "@graknlabs_graql//java:graql",
        "@graknlabs_graql//java/query:query",
    ],
    resources = [
        "//common/test:logback"
    ],
    resource_strip_prefix = "common/test",
)

host_compatible_java_test(
    name = "test-traversal-2",
    srcs = ["TraversalTest2.java"],
    test_class = "grakn.core.traversal.TraversalTest2",
    native_libraries_deps = [
        "//rocks:rocks",
        "//:grakn",
        "//concept:concept",
        "//traversal:traversal",
    ],
    deps = [
        # Internal dependencies
        "//test/integration/util:util",
        "//common:common",

        # External dependencies from Grakn Labs
        "@graknlabs_graql//java:graql",
        "@graknlabs_graql//java/query:query",
    ],
    resources = [
        "//common/test:logback"
    ],
    resource_strip_prefix = "common/test",
)

host_compatible_java_test(
    name = "test-traversal-3",
    srcs = ["TraversalTest3.java"],
    test_class = "grakn.core.traversal.TraversalTest3",
    native_libraries_deps = [
        "//rocks:rocks",
        "//:grakn",
        "//concept:concept",
    ],
    deps = [
        # Internal dependencies
        "//test/integration/util:util",
        "//common:common",

        # External dependencies from Grakn Labs
        "@graknlabs_graql//java:graql",
        "@graknlabs_graql//java/query:query",
    ],
    resources = [
        "//common/test:logback"
    ],
    resource_strip_prefix = "common/test",
)

host_compatible_java_test(
    name = "test-traversal-6",
    srcs = ["TraversalTest6.java"],
    test_class = "grakn.core.traversal.TraversalTest6",
    native_libraries_deps = [
        "//rocks:rocks",
        "//:grakn",
        "//concept:concept",
    ],
    deps = [
        # Internal dependencies
        "//test/integration/util:util",
        "//common:common",

        # External dependencies from Grakn Labs
        "@graknlabs_graql//java:graql",
        "@graknlabs_graql//java/query:query",
        "@graknlabs_common//:common",
    ],
    resources = [
        "//common/test:logback"
    ],
    resource_strip_prefix = "common/test",
)

host_compatible_java_test(
    name = "test-traversal-8",
    srcs = ["TraversalTest8.java"],
    test_class = "grakn.core.traversal.TraversalTest8",
    native_libraries_deps = [
        "//rocks:rocks",
        "//:grakn",
        "//concept:concept",
        "//server:server",
    ],
    deps = [
        # Internal dependencies
        "//test/integration/util:util",
        "//common:common",

        # External dependencies from Grakn Labs
        "@graknlabs_graql//java:graql",
        "@graknlabs_graql//java/query:query",
        "@graknlabs_common//:common",
    ],
    resources = [
        "//common/test:logback"
    ],
    resource_strip_prefix = "common/test",
    data = [":TraversalTest8/world2.gql", ":TraversalTest8/world2.data"],
)

checkstyle_test(
    name = "checkstyle",
    include = glob([
        "*",
    ]),
    exclude = ["TraversalTest8/world2.data"],
    license_type = "agpl",
)
