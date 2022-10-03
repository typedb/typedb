#
# Copyright (C) 2022 Vaticle
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

load("@vaticle_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@vaticle_dependencies//builder/java:rules.bzl", "native_java_libraries")
package(
    default_visibility = [
        "//:__pkg__",
        "//database:__pkg__",
        "//server:__pkg__",
        "//test:__subpackages__",
        "//query:__subpackages__",
        "//reasoner:__subpackages__",
        "//logic:__pkg__"
    ]
)

native_java_libraries(
    name = "concept",
    srcs = glob([
        "**/*.java",
    ]),
    native_libraries_deps = [
        # Internal dependencies
        "//common:common",
        "//concurrent:concurrent",
        "//graph:graph",
        "//encoding:encoding",
        "//pattern:pattern",
        "//traversal:traversal",
    ],
    deps = [
        # Internal Repository Dependencies
        "@vaticle_typedb_common//:common",
        "@vaticle_typeql//java/common:common",
        "@vaticle_typeql//java/pattern:pattern",
        "@vaticle_typeql//java:typeql-lang",

        # External Maven Dependencies
        "@maven//:com_google_code_findbugs_jsr305",
    ],
    tags = ["maven_coordinates=com.vaticle.typedb:typedb-core-concept:{pom_version}"],
    visibility = ["//visibility:public"],
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*", "*/*", "*/*/*"]),
    license_type = "agpl-header",
)
