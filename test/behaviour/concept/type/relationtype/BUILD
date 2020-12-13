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

package(default_visibility = ["//test/behaviour:__subpackages__"])
load("@graknlabs_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@graknlabs_dependencies//builder/java:rules.bzl", "host_compatible_java_library")

host_compatible_java_library(
    name = "steps",
    srcs = [
        "RelationTypeSteps.java"
    ],
    deps = [
        # Internal dependencies
        "//common",
        "//test/behaviour/config:parameters",
        "//test/behaviour/connection:steps",
        "//test/behaviour/util:util",

        # External Maven Dependencies
        "@maven//:junit_junit",
        "@maven//:io_cucumber_cucumber_java",
    ],
    native_libraries_deps = [
        "//concept:concept",
    ]
)

java_test(
    name = "test",
    srcs = [
        "RelationTypeTest.java"
    ],
    test_class = "grakn.core.test.behaviour.concept.type.relationtype.RelationTypeTest",
    deps = [
        # External Maven Dependencies
        "@maven//:io_cucumber_cucumber_junit",
    ],
    runtime_deps = [
        ":steps",
        "//test/behaviour/config:parameters",
        "//test/behaviour/concept/thing/entity:steps",
        "//test/behaviour/concept/thing/relation:steps",
        "//test/behaviour/concept/type/attributetype:steps",
        "//test/behaviour/concept/type/thingtype:steps",
        "//test/behaviour/connection/database:steps",
        "//test/behaviour/connection/session:steps",
        "//test/behaviour/connection/transaction:steps",
    ],
    data = [
        "@graknlabs_behaviour//concept/type:relationtype.feature",
    ],
    size = "medium",
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*"]),
    license_type = "agpl",
)
