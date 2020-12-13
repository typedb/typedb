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
    name = "test-basic",
    srcs = ["BasicTest.java"],
    test_class = "grakn.core.test.integration.BasicTest",
    native_libraries_deps = [
        "//rocks:rocks",
        "//:grakn",
        "//concept:concept",
        "//logic:logic",
    ],
    deps = [
        # Internal dependencies
        "//test/integration/util:util",
        "//common:common",

        # External dependencies from Grakn Labs
        "@graknlabs_graql//java/pattern:pattern",
        "@graknlabs_graql//java:graql",
    ],
    resources = [
        "//common/test:logback"
    ],
    resource_strip_prefix = "common/test",
    size = "large",
)

host_compatible_java_test(
    name = "test-query",
    srcs = ["QueryTest.java"],
    test_class = "grakn.core.test.integration.QueryTest",
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
        "@graknlabs_graql//java/query:query",
        "@graknlabs_graql//java:graql",
    ],
    data = [":schema.gql"],
)

checkstyle_test(
    name = "checkstyle",
    include = glob([
        "*",
    ]),
    license_type = "agpl",
)
