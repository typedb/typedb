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

java_test(
    name = "test-basic",
    srcs = [
        "BasicTest.java",
        "Util.java",
    ],
    test_class = "grakn.core.test.integration.BasicTest",
    deps = [
        # Internal dependencies
        "//:grakn",
        "//common:common",
        "//concept:concept",
        "//rocks:rocks",

        # External dependencies from Grakn Labs
        "@graknlabs_common//:common",
        "@graknlabs_graql//java/pattern:pattern",
        "@graknlabs_graql//java:graql",
    ],
    size = "large",
)

java_test(
    name = "test-query",
    srcs = [
        "QueryTest.java",
        "Util.java",
    ],
    test_class = "grakn.core.test.integration.QueryTest",
    deps = [
        # Internal dependencies
        "//:grakn",
        "//common:common",
        "//concept:concept",
        "//rocks:rocks",

        # External dependencies from Grakn Labs
        "@graknlabs_graql//java/query:query",
        "@graknlabs_graql//java:graql",
    ],
    data = [":schema.gql"]
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*"]),
    license_type = "agpl",
)
