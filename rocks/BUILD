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
        "//server:__pkg__",
        "//test:__subpackages__",
    ]
)

native_java_libraries(
    name = "rocks",
    srcs = glob(["*.java"]),
    deps = [
        # Internal dependencies
        "//common:common",
        "//concurrent:concurrent",
        "//graph:graph",

        # External dependencies from Vaticle
        "@vaticle_typedb_common//:common",

        # External dependencies from Maven
        "@maven//:com_google_code_findbugs_jsr305",
        "@maven//:org_slf4j_slf4j_api"
    ],
    native_libraries_deps = [
        "//:typedb",
        "//concept:concept",
        "//logic:logic",
        "//query:query",
        "//reasoner:reasoner",
        "//traversal:traversal",
    ],
    mac_deps = [
        "@maven//:com_google_ortools_ortools_darwin",
        "@maven//:com_google_ortools_ortools_darwin_java",
        "@maven//:org_rocksdb_rocksdbjni_dev",
    ],
    linux_deps = [
        "@maven//:com_google_ortools_ortools_linux_x86_64",
        "@maven//:com_google_ortools_ortools_linux_x86_64_java",
        "@maven//:org_rocksdb_rocksdbjni",
    ],
    windows_deps = [
        "@maven//:com_google_ortools_ortools_win32_x86_64",
        "@maven//:com_google_ortools_ortools_win32_x86_64_java",
        "@maven//:org_rocksdb_rocksdbjni",
    ],
    tags = ["maven_coordinates=com.vaticle.typedb:typedb-core-rocks:{pom_version}"],
    visibility = [ "//visibility:public" ]
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*"]),
    license_type = "agpl",
)

