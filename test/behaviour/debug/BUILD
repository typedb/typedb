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


package(default_visibility = ["//visibility:__subpackages__"])
load("@graknlabs_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")

java_test(
    name = "test",
    srcs = [
        "DebugTest.java",
    ],
    test_class = "grakn.core.test.behaviour.debug.DebugTest",
    deps = [
        # Package dependencies
        "//test/behaviour/server:server",

        # TODO: Add your addition debugging dependencies here
        # e.g. "//test/behaviour/connection/steps:connection-keyspace",

        # External dependencies from Maven
        "@maven//:io_cucumber_cucumber_java",
        "@maven//:io_cucumber_cucumber_junit",
    ],
    runtime_deps = [
#        "//test/behaviour/graql:steps",
#        "//test/behaviour/connection:steps",
#        "//test/behaviour/connection/session:steps",
#        "//test/behaviour/connection/transaction:steps",
#        "//test/behaviour/connection/keyspace:steps",
#        "//test/behaviour/config:parameters",
    ],
    classpath_resources = ["//test/resources:logback-test"],
    data = [
        ":debug.feature",
    ],
    size = "medium",
    tags = ["manual"]
)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":test"
    ],
)
