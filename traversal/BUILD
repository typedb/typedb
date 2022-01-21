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

load("@vaticle_dependencies//library/ortools:deps.bzl", "google_or_tools")
load("@vaticle_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@vaticle_dependencies//builder/java:rules.bzl", "native_java_libraries")

package(
    default_visibility = [
        "//pattern:__subpackages__",
        "//query:__subpackages__",
        "//reasoner:__subpackages__",
        "//database:__pkg__",
    ]
)

native_java_libraries(
    name = "traversal",
    srcs = glob(["*.java", "*/*.java", "*/*/*.java"]),
    native_libraries_deps = [
        # Internal dependencies
        "//common:common",
        "//concurrent:concurrent",
        "//graph:graph",
    ],
    deps = [
        # Vaticle Dependencies
        "@vaticle_typedb_common//:common",
        "@vaticle_typeql_lang_java//common:common",
        "@vaticle_typeql_lang_java//pattern:pattern",

        # External dependencies from Vaticle

        # External dependencies from Maven
        "@maven//:com_github_ben_manes_caffeine_caffeine",
        "@maven//:com_google_code_findbugs_jsr305",
        "@maven//:org_slf4j_slf4j_api",
    ],
    tags = ["maven_coordinates=com.vaticle.typedb:typedb-core-traversal:{pom_version}"],
    visibility = ["//visibility:public"],
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*", "*/*", "*/*/*"]),
    license_type = "agpl",
)
