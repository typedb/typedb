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

load("@vaticle_dependencies//library/ortools:deps.bzl", "google_or_tools")
load("@vaticle_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@vaticle_dependencies//builder/java:rules.bzl", "native_java_libraries")

package(
    default_visibility = [
        "//server:__pkg__",
        "//test:__subpackages__",
    ]
)

native_java_libraries(
    name = "database",
    srcs = glob(["*.java"]),
    native_libraries_deps = [
        # Internal dependencies
        "//:typedb",
        "//common:common",
        "//concept:concept",
        "//concurrent:concurrent",
        "//graph:graph",
        "//encoding:encoding",
        "//logic:logic",
        "//query:query",
        "//reasoner:reasoner",
        "//traversal:traversal",
    ],
    deps = [
        # External dependencies from Vaticle
        "@vaticle_typedb_common//:common",
        "@vaticle_typeql//java:typeql-lang",

        # External dependencies from Maven
        "@maven//:com_google_ortools_ortools_java",
        "@maven//:com_google_code_findbugs_jsr305",
        "@maven//:org_slf4j_slf4j_api",
        "@maven//:io_github_speedb_io_speedbjni",
        "@maven//:io_sentry_sentry",
    ],
    linux_arm64_deps = ["@maven//:com_google_ortools_ortools_linux_aarch64"],
    linux_x86_64_deps = ["@maven//:com_google_ortools_ortools_linux_x86_64"],
    mac_arm64_deps = ["@maven//:com_google_ortools_ortools_darwin_aarch64"],
    mac_x86_64_deps = ["@maven//:com_google_ortools_ortools_darwin_x86_64"],
    windows_x86_64_deps = ["@maven//:com_google_ortools_ortools_win32_x86_64"],
    tags = ["maven_coordinates=com.vaticle.typedb:typedb-core-database:{pom_version}"],
    visibility = [ "//visibility:public" ]
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*"]),
    license_type = "agpl-header",
)
