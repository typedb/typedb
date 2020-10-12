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
package(
    default_visibility = [
        "//pattern:__subpackages__",
        "//query:__subpackages__",
        "//reasoner:__subpackages__",
        "//rocks:__pkg__",
    ]
)

java_library(
    name = "traversal",
    srcs = glob(["*.java", "*/*.java", "*/*/*.java"]),
    deps = [
        # Internal dependencies
        "//common:common",
        "//graph:graph",

        # Grakn Labs Dependencies
        # "@graknlabs_common//:common",

        # External dependencies from Grakn Labs

        # External dependencies from Maven
    ],
    tags = ["maven_coordinates=io.grakn.core:grakn-traversal:{pom_version}"],
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*", "*/*", "*/*/*"]),
    license_type = "agpl",
)
