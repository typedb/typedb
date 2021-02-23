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

package(default_visibility = ["//test/behaviour:__subpackages__"])
load("@graknlabs_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")

java_test(
    name = "test",
    srcs = [
        "CompoundQueriesTest.java",
    ],
    test_class = "grakn.core.test.behaviour.resolution.tests.compound_queries.CompoundQueriesTest",
    deps = [
        # External dependencies from Maven
        "@maven//:io_cucumber_cucumber_junit",
    ],
    runtime_deps = [
        "//test/behaviour/connection:steps",
        "//test/behaviour/connection/session:steps",
        "//test/behaviour/config:parameters",
        "//test/behaviour/resolution:steps",
    ],
    data = [
        "@graknlabs_behaviour//graql/reasoner:compound-queries.feature",
    ],
    resources = [
        "//common/test:logback"
    ],
    resource_strip_prefix = "common/test",
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*"]),
    license_type = "agpl",
)
