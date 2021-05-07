#
# Copyright (C) 2021 Vaticle
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
package(
    default_visibility = [
        "//concept:__pkg__",
        "//rocks:__pkg__",
        "//traversal:__pkg__",
        "//reasoner:__pkg__"
    ]
)

java_library(
    name = "graph",
    srcs = glob(["*.java", "*/*.java", "*/*/*.java"]),
    deps = [
        # External dependencies from Vaticle
        "@vaticle_typedb_common//:common",

        # Internal dependencies
        "//common:common",

        # External dependencies from Vaticle
        "@vaticle_typeql_lang_java//:typeql-lang",
        "@vaticle_typeql_lang_java//common:common",
        "@vaticle_typeql_lang_java//pattern:pattern",

        # External Maven Dependencies
        "@maven//:com_google_code_findbugs_jsr305",
    ],
    tags = ["maven_coordinates=com.vaticle.typedb:typedb-core-graph:{pom_version}"],
    visibility = ["//visibility:public"],
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*", "*/*", "*/*/*"]),
    license_type = "agpl",
)
