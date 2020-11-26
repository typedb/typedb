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
load("@graknlabs_dependencies//builder/java:rules.bzl", "native_java_libraries")
load("@graknlabs_dependencies//builder/java:rules.bzl", "host_compatible_java_test")

package(default_visibility = [
    "//query:__subpackages__",
    "//reasoner:__subpackages__",
    "//test/integration:__pkg__",
])

native_java_libraries(
    name = "reasoner",
    srcs = glob(
        ["*.java"],
        exclude = ["*Test.java"],
    ),
    native_libraries_deps = [
        "//reasoner/resolution:resolution",
        "//concept:concept",
        "//pattern:pattern",
        "//traversal:traversal",
    ],
    tags = ["maven_coordinates=io.grakn.core:grakn-reasoner:{pom_version}"],
    deps = [
        # Internal dependencies
        "//common",
        "//graph",

        # External dependencies from Grakn Labs
        "@graknlabs_common//:common",
        "@graknlabs_graql//java/common",
        "@graknlabs_graql//java/pattern",

        # External dependencies from Maven
    ],
)

host_compatible_java_test(
    name = "test-type-inference",
    srcs = [
        "TypeInferenceTest.java",
    ],
    native_libraries_deps = [
        "//pattern:pattern",
        "//reasoner:reasoner",
    ],
    test_class = "grakn.core.reasoner.TypeInferenceTest",
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*"]),
    license_type = "agpl",
)
