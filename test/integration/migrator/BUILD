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
    name = "test-migrator",
    srcs = ["MigratorTest.java"],
    test_class = "grakn.core.server.migrator.MigratorTest",
    native_libraries_deps = [
        "//server:server",
        "//rocks:rocks",
        "//migrator:migrator",
        "//:grakn",
    ],
    deps = [
        # Internal dependencies
        "//test/integration/util:util",
        "//common:common",
        "//server:version",

        # External dependencies from Grakn Labs
        "@graknlabs_graql//java:graql",
        "@graknlabs_graql//java/query:query",

        # External dependencies from Maven
        "@maven//:com_google_protobuf_protobuf_java",
    ],
    resources = [
        "//common/test:logback-test"
    ],
    resource_strip_prefix = "common/test",
    data = [":schema.gql", "data.grakn"],
    size = "large",
)

checkstyle_test(
    name = "checkstyle",
    include = glob([
        "*"
    ]),
    exclude = ["data.grakn"],
    license_type = "agpl",
)
