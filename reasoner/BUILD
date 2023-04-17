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
load("@vaticle_dependencies//builder/java:rules.bzl", "host_compatible_java_test")

package(default_visibility = [
    "//query:__subpackages__",
    "//reasoner:__subpackages__",
    "//database:__pkg__",
    "//server:__pkg__",
    "//test/integration/reasoner:__pkg__",
    "//test/behaviour/reasoner:__subpackages__",
    "//test/benchmark/reasoner:__pkg__",
])

native_java_libraries(
    name = "reasoner",
    srcs = glob(
        ["*.java",
        "processor/*.java",
        "processor/reactive/*.java",
        "processor/reactive/common/*.java",
        "controller/*.java",
        "answer/*.java",
        "common/*.java",
        "planner/*.java"
        ], exclude = ["*Test.java", "**/*Test.java"],
    ),
    native_libraries_deps = [
        # Internal dependencies
        "//common:common",
        "//concurrent:concurrent",
        "//concept:concept",
        "//graph:graph",
        "//logic:logic",
        "//pattern:pattern",
        "//traversal:traversal",
    ],
    deps = [
        # External dependencies from Vaticle
        "@vaticle_typedb_common//:common",
        "@vaticle_typeql//java/common",
        "@vaticle_typeql//java/pattern",
        "@vaticle_typeql//java/query",

        # External Maven Dependencies
        "@maven//:com_google_code_findbugs_jsr305",
        "@maven//:org_slf4j_slf4j_api"
    ],
    runtime_deps = [
        "@maven//:ch_qos_logback_logback_classic",
    ],
    tags = ["maven_coordinates=com.vaticle.typedb:typedb-core-reasoner:{pom_version}"],
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*", "*/*", "*/*/*", "*/*/*/*"]),
    license_type = "agpl-header",
)
