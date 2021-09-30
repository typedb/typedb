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
load("@vaticle_dependencies//builder/java:rules.bzl", "native_java_libraries")
load("@vaticle_dependencies//builder/java:rules.bzl", "host_compatible_java_test")

package(
    default_visibility = [
        "//:__pkg__",
        "//reasoner:__pkg__",
        "//rocks:__pkg__",
        "//test:__subpackages__",
    ],
)

native_java_libraries(
    name = "logic",
    srcs = glob(["*.java", "tool/*.java", "**/*.java"], exclude = ["*Test.java", "*/*Test.java"]),
    native_libraries_deps = [
        # Internal dependencies
        "//common:common",
        "//graph:graph",
        "//concept:concept",
        "//pattern:pattern",
        "//traversal:traversal",
    ],
    deps = [
        # Internal Repository Dependencies
        "@vaticle_typedb_common//:common",
        "@vaticle_typeql_lang_java//common",
        "@vaticle_typeql_lang_java//pattern",

        # External Maven Dependencies
        "@maven//:com_google_code_findbugs_jsr305",
        "@maven//:org_slf4j_slf4j_api"
    ],
    runtime_deps = [
        "@maven//:ch_qos_logback_logback_classic",
    ],
    tags = ["maven_coordinates=com.vaticle.typedb:typedb-core-logic:{pom_version}"],
    visibility = ["//visibility:public"],
)

host_compatible_java_test(
    name = "test-concludable",
    srcs = [
        "resolvable/ConcludableTest.java",
    ],
    native_libraries_deps = [
        "//logic:logic",
        "//pattern:pattern",
    ],
    test_class = "com.vaticle.typedb.core.logic.resolvable.ConcludableTest",
    deps = [
        # Internal dependencies

        # External dependencies from Vaticle
        "@vaticle_typedb_common//:common",
        "@vaticle_typeql_lang_java//:typeql-lang",
    ],
)

host_compatible_java_test(
    name = "test-retrievable",
    srcs = [
        "resolvable/RetrievableTest.java",
    ],
    native_libraries_deps = [
        # Internal dependencies
        "//common:common",
        "//logic:logic",
        "//pattern:pattern",
    ],
    test_class = "com.vaticle.typedb.core.logic.resolvable.RetrievableTest",
    deps = [

        # External dependencies from Vaticle
        "@vaticle_typedb_common//:common",
        "@vaticle_typeql_lang_java//:typeql-lang",
    ],
)

checkstyle_test(
    name = "checkstyle",
    include = glob([
        "*",
        "*/*",
    ]),
    license_type = "agpl",
)
