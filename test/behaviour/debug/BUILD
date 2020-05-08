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
load("@graknlabs_build_tools//checkstyle:rules.bzl", "checkstyle_test")

java_test(
    name = "test",
    srcs = [
        "DebugTest.java",
    ],
    test_class = "hypergraph.test.behaviour.debug.DebugTest",
    deps = [
        # Internal Package Dependencies

        # TODO: Add your addition debugging dependencies here
        # e.g. "//test/behaviour/connection/session:steps",

        # External Maven Dependencies
        "//dependencies/maven/artifacts/io/cucumber:cucumber-java",
        "//dependencies/maven/artifacts/io/cucumber:cucumber-junit",
    ],
    runtime_deps = [
        "//test/behaviour/config:parameters",
        "//test/behaviour/concept/type/attributetype:steps",
        "//test/behaviour/concept/type/relationtype:steps",
        "//test/behaviour/concept/type/thingtype:steps",
        "//test/behaviour/connection/keyspace:steps",
        "//test/behaviour/connection/session:steps",
        "//test/behaviour/connection/transaction:steps",
    ],
    data = [
        ":debug.feature",
    ],
    size = "medium",
)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":test"
    ],
)
