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
    name = "test-resolution",
    srcs = ["ResolutionTest.java"],
    native_libraries_deps = [
        "//rocks:rocks",
        "//:grakn",
        "//concept:concept",
        "//pattern:pattern",
        "//logic:logic",
        "//reasoner:reasoner",
    ],
    resource_strip_prefix = "common/test",
    resources = [
        "//common/test:logback",
    ],
    test_class = "grakn.core.reasoner.ResolutionTest",
    deps = [
        # Internal dependencies
        "//test/integration/util",
        "//common",
        "@graknlabs_graql//java:graql",
    ],
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*"]),
    license_type = "agpl",
)
